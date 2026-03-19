"""
LLM-assisted property match verification for borderline name-match cases.

Invoked when a name-match score falls in the borderline range (80-94%) —
not high-confidence enough to accept blindly, but not low enough to quarantine.
Also invoked with force=True for structurally suspicious strategies (e.g.
eviction defendant name used as owner proxy).

Uses the Anthropic SDK directly for a structured JSON response.
Model: claude-sonnet-4-5-20250929.

Safety limits:
  - Per-loader cap (default 20 calls/run, overridden per loader class)
  - LLM can only accept property_id values from the provided candidate set
  - JSON parse failure or API error → returns low-confidence rejection (quarantine path)
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional

import anthropic

from config.settings import get_settings

logger = logging.getLogger(__name__)

# Default cap — overridden per-loader via _LLM_MAX_CALLS class variable.
MAX_LLM_CALLS_PER_RUN = 20

# Score thresholds used by _apply_llm_verification() in BaseLoader.
# At/above HIGH_CONFIDENCE: accept without LLM.
# Below LLM_SCORE_FLOOR: quarantine without LLM.
# In range [LLM_SCORE_FLOOR, HIGH_CONFIDENCE): trigger LLM.
HIGH_CONFIDENCE = 95
LLM_SCORE_FLOOR = 80
LLM_SCORE_CEILING = 94   # kept for backwards compat

# Per-record-type context injected into the verification prompt so Claude
# understands the data provenance and which party is typically the property owner.
RECORD_TYPE_CONTEXT = {
    'lien_tcl':    "Tampa Code Lien (TCL). Issued by City of Tampa. ONLY applies to properties within Tampa city limits (city='Tampa').",
    'lien_ccl':    "County Code Lien (CCL). Issued by Hillsborough County. Applies to unincorporated Hillsborough County (outside Tampa).",
    'lien_ml':     "Mechanics/Judgment Lien. Grantor may be the debtor (property owner) OR creditor (filer). Verify carefully.",
    'lien_hoa':    "HOA Lien. Debtor is the property owner who owes dues to the HOA.",
    'lien_tl':     "Federal Tax Lien (IRS). Grantee is the property owner/taxpayer.",
    'deed':        "Deed transfer. Grantor = seller (prior owner), Grantee = buyer (new owner).",
    'lis_pendens': "Lis Pendens (pending lawsuit). Grantee is typically the property owner/defendant.",
    'eviction':    "Eviction filing. Plaintiff = landlord (property owner). Match must be to Plaintiff, NOT the tenant/defendant.",
    'probate':     "Probate case. Decedent was likely the property owner.",
    'bankruptcy':  "Bankruptcy. Debtor MAY own property — verify name and context carefully.",
}


@dataclass
class LLMMatchResult:
    matched: bool
    property_id: Optional[int]
    confidence: str          # "high" | "medium" | "low"
    reason: str
    tokens_used: int = 0


class LLMPropertyMatcher:
    """
    Verifies ambiguous property matches using Claude.

    Instantiated once per loader (i.e. per scraper run). The internal call
    counter enforces the per-loader cap across all records in a run.

    Usage via BaseLoader._apply_llm_verification() (preferred) or directly:
        matcher = LLMPropertyMatcher(max_calls=30)
        result = matcher.verify_match(raw_row, candidates, current_best,
                                      current_score, record_type='deed',
                                      match_field='Grantor')
        if result.matched and result.confidence != 'low':
            # accept the match
        else:
            # quarantine
    """

    def __init__(self, max_calls: int = MAX_LLM_CALLS_PER_RUN):
        settings = get_settings()
        self._client = anthropic.Anthropic(
            api_key=settings.anthropic_api_key.get_secret_value()
        )
        self._calls_this_run: int = 0
        self._max_calls: int = max_calls

    @property
    def budget_exhausted(self) -> bool:
        return self._calls_this_run >= self._max_calls

    def verify_match(
        self,
        raw_row: dict,
        candidates: list,
        current_best,
        current_score: int,
        record_type: str = 'lien_tcl',
        match_field: str = 'Grantee',
    ) -> LLMMatchResult:
        """
        Ask Claude to confirm or reject the best candidate property match.

        Args:
            raw_row:       Full CSV row dict (as stored in UnmatchedRecord.raw_data).
            candidates:    Top-N candidate Property objects from pg_trgm similarity.
            current_best:  The highest-scoring Property from name matching.
            current_score: Rapidfuzz score for current_best.
            record_type:   Key into RECORD_TYPE_CONTEXT (e.g. 'deed', 'eviction').
            match_field:   CSV column used for name matching (e.g. 'Grantor', 'Plaintiff').

        Returns:
            LLMMatchResult. On any error, returns matched=False, confidence='low'.
        """
        if self.budget_exhausted:
            logger.warning(
                "LLM match budget exhausted (%d calls this run). Skipping LLM verification.",
                self._max_calls,
            )
            return LLMMatchResult(
                matched=False, property_id=None, confidence='low',
                reason=f'LLM budget exhausted ({self._max_calls} calls/run limit)',
            )

        # Build the set of valid candidate IDs — LLM may only select from these
        valid_ids: set[int] = set()
        if current_best:
            valid_ids.add(current_best.id)
        for prop in candidates:
            if prop:
                valid_ids.add(prop.id)

        if not valid_ids:
            return LLMMatchResult(
                matched=False, property_id=None, confidence='low',
                reason='No candidate properties to evaluate',
            )

        prompt = self._build_prompt(raw_row, candidates, current_best, current_score,
                                    record_type=record_type, match_field=match_field)

        try:
            response = self._client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=512,
                temperature=0,
                system=(
                    "You are a property record matching expert for Hillsborough County, Florida. "
                    "You evaluate county lien records and property database entries to determine "
                    "whether a lien record refers to a specific property. "
                    "Respond ONLY with a valid JSON object. No explanation outside the JSON."
                ),
                messages=[{"role": "user", "content": prompt}],
            )
            self._calls_this_run += 1
            tokens_used = response.usage.input_tokens + response.usage.output_tokens

            raw_text = response.content[0].text.strip()
            # Strip markdown code fences if present
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]
            parsed = json.loads(raw_text.strip())

            matched = bool(parsed.get('matched', False))
            prop_id = parsed.get('property_id')
            confidence = str(parsed.get('confidence', 'low')).lower()
            reason = str(parsed.get('reason', ''))

            # Safety: reject if LLM selected a property_id outside the candidate set
            if matched and prop_id is not None and int(prop_id) not in valid_ids:
                logger.warning(
                    "LLM returned property_id=%s not in candidate set %s. Rejecting.",
                    prop_id, valid_ids,
                )
                return LLMMatchResult(
                    matched=False, property_id=None, confidence='low',
                    reason='LLM property_id not in verified candidate set',
                    tokens_used=tokens_used,
                )

            return LLMMatchResult(
                matched=matched,
                property_id=int(prop_id) if (matched and prop_id is not None) else None,
                confidence=confidence,
                reason=reason,
                tokens_used=tokens_used,
            )

        except json.JSONDecodeError as e:
            logger.error("LLM returned non-JSON response for code lien match: %s", e)
            return LLMMatchResult(
                matched=False, property_id=None, confidence='low',
                reason=f'LLM response parse error: {e}',
            )
        except Exception as e:
            logger.error("LLM verification call failed: %s", e)
            return LLMMatchResult(
                matched=False, property_id=None, confidence='low',
                reason=f'LLM call failed: {e}',
            )

    def _build_prompt(
        self,
        raw_row: dict,
        candidates: list,
        current_best,
        current_score: int,
        record_type: str = 'lien_tcl',
        match_field: str = 'Grantee',
    ) -> str:
        """Build the verification prompt for Claude."""
        record_summary = {k: v for k, v in raw_row.items()
                          if k in ('Instrument', 'document_type', 'Grantor', 'Grantee',
                                   'RecordDate', 'Filing Amt', 'Legal', 'BookType',
                                   'CaseNumber', 'Lead Name', 'Date Filed',
                                   'PartyAddress', 'FirstName', 'LastName/CompanyName',
                                   'FilingDate', 'Title', 'CaseTypeDescription')}

        # Deduplicate candidates (current_best may already be in candidates list)
        seen_ids: set = set()
        all_candidates = []
        for prop in ([current_best] + list(candidates)):
            if prop and prop.id not in seen_ids:
                seen_ids.add(prop.id)
                all_candidates.append(prop)
        all_candidates = all_candidates[:3]

        candidate_list = []
        for prop in all_candidates:
            owner_name = None
            try:
                owner_name = prop.owner.owner_name if prop.owner else None
            except Exception:
                pass
            candidate_list.append({
                'property_id':       prop.id,
                'parcel_id':         prop.parcel_id,
                'address':           prop.address,
                'city':              prop.city,
                'zip':               prop.zip,
                'owner_name':        owner_name,
                'legal_description': (prop.legal_description or '')[:200],
                'name_match_score':  current_score if (current_best and prop.id == current_best.id) else 'N/A',
            })

        context_note = RECORD_TYPE_CONTEXT.get(record_type, f"Record type: {record_type}.")

        prompt = f"""You are verifying whether a public record matches a property in the Hillsborough County, FL property database.

RECORD TYPE: {record_type.upper()}
CONTEXT: {context_note}

SOURCE RECORD (key fields):
{json.dumps(record_summary, indent=2, default=str)}

MATCH FIELD USED: "{match_field}" (this is the field whose value was compared against owner names)
NAME MATCH SCORE: {current_score}% (borderline — range {LLM_SCORE_FLOOR}-{LLM_SCORE_CEILING}% triggers verification)

CANDIDATE PROPERTIES (top matches by owner name similarity):
{json.dumps(candidate_list, indent=2, default=str)}

YOUR TASK:
Determine if the best-matching property (or another candidate) is the correct match for this record.

Consider:
1. Does the owner name on the property plausibly refer to the same person as the record's {match_field} name?
2. Is the property in the correct jurisdiction for this record type?
3. Are multiple candidates equally plausible? If so, decline to match.
4. Does any corroborating detail (amount, date, address proximity) favor one candidate?

Respond with ONLY this JSON object — no text outside it:
{{
  "matched": true or false,
  "property_id": <integer property_id if matched, null if not>,
  "confidence": "high" or "medium" or "low",
  "reason": "<one sentence explaining the decision>"
}}

Rules:
- "high": clear match — name strongly correlates AND correct jurisdiction
- "medium": plausible match but some ambiguity (e.g. common surname, partial name)
- "low": cannot determine with confidence — set matched=false
- If matched=false, property_id must be null
- If multiple candidates are equally plausible, set matched=false and confidence="low"
"""
        return prompt
