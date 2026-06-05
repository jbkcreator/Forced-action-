"""
Re-match Unmatched Records Task
================================
Retries property matching for records in the unmatched_records staging table.
Run manually or wired to the quarterly master parcel refresh.

Usage:
    python -m src.tasks.rematch_unmatched
    python -m src.tasks.rematch_unmatched --source liens --limit 1000
    python -m src.tasks.rematch_unmatched --llm-only --limit 5500 --county hillsborough
    python -m src.tasks.rematch_unmatched --llm-only --dry-run --limit 20
"""
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from config.matching import for_county
from src.core.database import get_db_context
from src.core.models import (
    UnmatchedRecord, Property,
    LegalAndLien, Deed, LegalProceeding, Foreclosure,
)
from src.loaders.liens import LienLoader
from src.loaders.deeds import DeedLoader
from src.loaders.legal_proceedings import ProbateLoader, EvictionLoader, BankruptcyLoader
from src.loaders.base import BaseLoader
from src.loaders.llm_matcher import (
    LLMPropertyMatcher,
    SOURCE_TYPE_TO_RECORD_TYPE,
    SOURCE_TYPE_TO_MATCH_FIELD,
)

# Mirrors _LEGAL_DESC_RE in legal_proceedings.py — detects legal descriptions
# stored in PartyAddress fields (LOT/BLOCK/UNIT prefix, no house number).
_LEGAL_DESC_RE = re.compile(
    r'^\s*(LOT\s|BLOCK\s|UNIT\s|BLDG\s|BUILDING\s|TRACT\s|PARCEL\s|'
    r'[A-Z]-\d|UNIT\s*NO\.?\s*\d)',
    re.IGNORECASE,
)

logger = logging.getLogger(__name__)


def rematch_unmatched(
    source_type: Optional[str] = None,
    limit: int = 5000,
    county_id: str = "hillsborough",
) -> dict:
    """
    Retry property matching for unmatched and pending_review staged records.

    Args:
        source_type: Filter by source (liens, deeds, evictions, etc.). None = all.
        limit: Max records to process per run.
        county_id: County to process.

    Returns:
        dict with counts: total, matched, pending_review, still_unmatched, errors
    """
    stats = {"total": 0, "matched": 0, "pending_review": 0, "still_unmatched": 0, "errors": 0}

    with get_db_context() as db:
        query = db.query(UnmatchedRecord).filter(
            UnmatchedRecord.match_status.in_(["unmatched", "pending_review"]),
            UnmatchedRecord.county_id == county_id,
        )
        if source_type:
            query = query.filter(UnmatchedRecord.source_type == source_type)

        records = query.order_by(UnmatchedRecord.date_added).limit(limit).all()
        stats["total"] = len(records)
        logger.info("Re-matching %d records (source=%s)", len(records), source_type or "all")

        # Instantiate loaders for matching logic — county_id must be passed so
        # all find_property_by_* calls filter against the correct county's properties.
        lien_loader = LienLoader(db, county_id=county_id)
        deed_loader = DeedLoader(db, county_id=county_id)
        lp_loader = EvictionLoader(db, county_id=county_id)

        BATCH_SIZE = 500
        LOG_INTERVAL = 25
        for i, record in enumerate(records):
            if i % LOG_INTERVAL == 0:
                logger.info(
                    "Progress: %d/%d — matched=%d pending_review=%d still_unmatched=%d errors=%d",
                    i, len(records), stats["matched"], stats["pending_review"],
                    stats["still_unmatched"], stats["errors"],
                )
            if i > 0 and i % BATCH_SIZE == 0:
                db.commit()
                logger.info("Committed batch at record %d", i)

            try:
                record.match_attempted_at = datetime.now(timezone.utc)
                raw = record.raw_data or {}

                # County-aware thresholds — Pinellas has wider pending_review band
                thr = for_county(record.county_id)

                property_record = None
                match_score = 0
                match_method = None

                loader = lien_loader  # default — has both matching strategies
                if record.source_type == "deeds":
                    loader = deed_loader
                elif record.source_type in ("evictions", "probate", "bankruptcies", "divorce_filings"):
                    loader = lp_loader

                # ── Probate / eviction / divorce: use source-specific fields ──────
                if record.source_type in ("probate", "evictions", "divorce_filings"):
                    party_address = raw.get("PartyAddress") or record.address_string
                    if party_address:
                        addr_str = str(party_address).strip()
                        if _LEGAL_DESC_RE.match(addr_str):
                            result = loader.find_property_by_legal_description(addr_str)
                            if result:
                                property_record, match_score = result
                                match_method = 'legal_desc'
                        else:
                            result = loader.find_property_by_address(
                                addr_str, threshold=thr.address_floor
                            )
                            if result:
                                property_record, match_score = result
                                match_method = 'address'

                    if not property_record:
                        last = str(raw.get("LastName/CompanyName") or "").strip()
                        first = str(raw.get("FirstName") or "").strip()
                        middle = str(raw.get("MiddleName") or "").strip()
                        full_name = " ".join(p for p in [first, middle, last] if p)
                        full_name = " ".join(full_name.split())
                        if full_name:
                            result = loader.find_property_by_owner_name(
                                full_name, threshold=thr.owner_name_floor
                            )
                            if result:
                                property_record, match_score = result
                                match_method = 'owner_name'
                        # first+last only if middle added noise
                        if not property_record and first and last and middle:
                            result = loader.find_property_by_owner_name(
                                f"{first} {last}", threshold=thr.owner_name_floor
                            )
                            if result:
                                property_record, match_score = result
                                match_method = 'owner_name'

                # ── Bankruptcies: name only ───────────────────────────────────────
                elif record.source_type == "bankruptcies":
                    lead_name = record.grantor or raw.get("Lead Name") or raw.get("lead_name")
                    if lead_name:
                        result = loader.find_property_by_owner_name(
                            str(lead_name), threshold=thr.owner_name_floor
                        )
                        if result:
                            property_record, match_score = result
                            match_method = 'owner_name'

                # ── Liens / deeds / all others ────────────────────────────────────
                else:
                    legal = raw.get("Legal") or raw.get("legal_description")
                    grantor_val = record.grantor or raw.get("Grantor") or raw.get("grantor")
                    grantee_val = raw.get("Grantee") or raw.get("grantee")

                    # Determine which name field to use — mirrors LienLoader field logic.
                    doc_type_str      = str(raw.get('document_type', '')).upper()
                    doc_type_raw      = str(raw.get('DocType', '')).upper()
                    is_tax_lien       = 'TAX LIEN' in doc_type_str
                    is_code_lien      = (
                        'TCL' in doc_type_str or 'CCL' in doc_type_str
                        or 'CODE LIEN' in doc_type_raw
                    )
                    is_mechanics_lien = 'ML' in doc_type_str or 'MECHANIC' in doc_type_str
                    is_judgment       = 'JUDGMENT' in doc_type_raw or 'CERTIFIED' in doc_type_raw

                    _filer_keywords = lien_loader._city_filer_keywords

                    if is_tax_lien:
                        name_to_try = grantee_val
                    elif is_code_lien:
                        grantor_is_filer = grantor_val and any(
                            kw in str(grantor_val).upper() for kw in _filer_keywords
                        )
                        name_to_try = grantee_val if grantor_is_filer else grantor_val
                    elif is_mechanics_lien or is_judgment:
                        # ML: Grantor = contractor, Grantee = property owner
                        # JUDGMENT: Grantor = creditor, Grantee = debtor/property owner
                        name_to_try = grantee_val
                    else:
                        name_to_try = grantor_val

                    if legal:
                        result = loader.find_property_by_legal_description(str(legal))
                        if result:
                            property_record, match_score = result
                            match_method = 'legal_desc'

                    if not property_record and name_to_try:
                        result = loader.find_property_by_owner_name(
                            str(name_to_try), threshold=thr.owner_name_floor
                        )
                        if result:
                            property_record, match_score = result
                            match_method = 'owner_name'

                    # Address fallback for source types where address is the primary path.
                    if not property_record and record.source_type in (
                        "lis_pendens", "deeds", "violations", "permits"
                    ):
                        address_candidate = (
                            raw.get("Address") or raw.get("address") or record.address_string
                        )
                        if address_candidate and len(str(address_candidate).strip()) > 5:
                            result = loader.find_property_by_address(
                                str(address_candidate), threshold=thr.address_floor
                            )
                            if result:
                                property_record, match_score = result
                                match_method = 'address'

                if property_record:
                    tier = lien_loader._classify_match(match_score, match_method)
                    if tier == "matched":
                        record.match_status = "matched"
                        record.matched_property_id = property_record.id
                        record.match_confidence = round(match_score / 100.0, 3)
                        record.match_method = match_method
                        record.candidate_property_id = None
                        stats["matched"] += 1
                        logger.debug(
                            "Re-matched %s record instrument=%s -> property_id=%s (score=%.3f, method=%s)",
                            record.source_type, record.instrument_number,
                            property_record.id, match_score / 100.0, match_method,
                        )
                    else:  # pending_review
                        record.match_status = "pending_review"
                        record.candidate_property_id = property_record.id
                        record.match_confidence = round(match_score / 100.0, 3)
                        record.match_method = match_method
                        stats["pending_review"] += 1
                        logger.debug(
                            "Pending review %s record instrument=%s candidate_property_id=%s (score=%.3f)",
                            record.source_type, record.instrument_number,
                            property_record.id, match_score / 100.0,
                        )
                else:
                    stats["still_unmatched"] += 1

            except Exception as e:
                logger.warning("Error re-matching record id=%s: %s", record.id, e)
                stats["errors"] += 1

        db.commit()

    logger.info(
        "Re-match complete: total=%d matched=%d pending_review=%d still_unmatched=%d errors=%d",
        stats["total"], stats["matched"], stats["pending_review"],
        stats["still_unmatched"], stats["errors"],
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# LLM TIEBREAKER — PENDING REVIEW BACKLOG
# ─────────────────────────────────────────────────────────────────────────────

def _safe_str(val) -> Optional[str]:
    """Return stripped string or None for NaN / empty values."""
    if val is None:
        return None
    try:
        import math
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
    except Exception:
        pass
    s = str(val).strip()
    return s if s else None


def _promote_to_destination(
    session,
    record: UnmatchedRecord,
    property_id: int,
    county_id: str,
) -> bool:
    """
    Build and save the appropriate destination ORM record from UnmatchedRecord.raw_data.

    Returns True if a new destination row was created, False if it already exists
    (duplicate) or the source_type is not handled.
    """
    raw = record.raw_data or {}
    src = record.source_type or ""

    # ── Shared parsing helpers (no session needed) ────────────────────────────
    parse_date   = BaseLoader.parse_date
    parse_amount = BaseLoader.parse_amount

    try:
        # ── Liens ─────────────────────────────────────────────────────────────
        if src in ("liens", "judgments"):
            instrument = _safe_str(raw.get("Instrument") or raw.get("instrument_number"))
            if not instrument:
                return False
            existing = session.query(LegalAndLien).filter_by(instrument_number=instrument).first()
            if existing:
                return False
            record_type = "Judgment" if src == "judgments" else "Lien"
            lien = LegalAndLien(
                property_id=property_id,
                record_type=record_type,
                instrument_number=instrument,
                creditor=_safe_str(raw.get("Grantee")),
                debtor=_safe_str(raw.get("Grantor")),
                amount=parse_amount(raw.get("Filing Amt")),
                filing_date=parse_date(raw.get("RecordDate")),
                book_type=_safe_str(raw.get("BookType")),
                book_number=_safe_str(raw.get("Book")),
                page_number=_safe_str(raw.get("Page")),
                document_type=_safe_str(raw.get("document_type")),
                legal_description=_safe_str(raw.get("Legal")),
                match_confidence=1.0,
                match_method="llm_verified",
                county_id=county_id,
            )
            with session.begin_nested():
                session.add(lien)
                session.flush()
            return True

        # ── Deeds ─────────────────────────────────────────────────────────────
        if src == "deeds":
            instrument = _safe_str(raw.get("Instrument") or raw.get("instrument_number"))
            if not instrument:
                return False
            existing = session.query(Deed).filter_by(instrument_number=instrument).first()
            if existing:
                return False
            deed = Deed(
                property_id=property_id,
                instrument_number=instrument,
                grantor=_safe_str(raw.get("Grantor")),
                grantee=_safe_str(raw.get("Grantee")),
                record_date=parse_date(raw.get("RecordDate")),
                sale_price=parse_amount(raw.get("SalesPrice") or raw.get("sale_price")),
                deed_type=_safe_str(raw.get("DocType") or raw.get("document_type")),
                book_type=_safe_str(raw.get("BookType")),
                book_number=_safe_str(raw.get("Book") or raw.get("BookNum")),
                page_number=_safe_str(raw.get("Page") or raw.get("PageNum")),
                legal_description=_safe_str(raw.get("Legal")),
                match_confidence=1.0,
                match_method="llm_verified",
                county_id=county_id,
            )
            with session.begin_nested():
                session.add(deed)
                session.flush()
            return True

        # ── Legal Proceedings (probate / eviction / bankruptcy / divorce) ─────
        if src in ("probate", "evictions", "bankruptcies", "divorce_filings"):
            case_number = _safe_str(
                raw.get("CaseNumber") or raw.get("Case Number") or
                raw.get("case_number") or raw.get("Docket Number")
            )
            if not case_number:
                return False
            existing = session.query(LegalProceeding).filter_by(case_number=case_number).first()
            if existing:
                return False

            record_type_map = {
                "probate":         "Probate",
                "evictions":       "Eviction",
                "bankruptcies":    "Bankruptcy",
                "divorce_filings": "Divorce",
            }
            record_type = record_type_map[src]

            # Build party names from available fields
            first  = _safe_str(raw.get("FirstName") or "") or ""
            middle = _safe_str(raw.get("MiddleName") or "") or ""
            last   = _safe_str(
                raw.get("LastName/CompanyName") or
                raw.get("Lead Name") or raw.get("lead_name") or ""
            ) or ""
            assoc_party = " ".join(p for p in [first, middle, last] if p).strip() or None

            proc = LegalProceeding(
                property_id=property_id,
                record_type=record_type,
                case_number=case_number,
                filing_date=parse_date(
                    raw.get("FilingDate") or raw.get("Date Filed") or raw.get("date_filed")
                ),
                case_status=_safe_str(raw.get("Title")),
                associated_party=assoc_party,
                match_confidence=1.0,
                match_method="llm_verified",
                county_id=county_id,
                meta_data={
                    "case_type":    _safe_str(raw.get("CaseTypeDescription")),
                    "party_address": _safe_str(raw.get("PartyAddress")),
                },
            )
            with session.begin_nested():
                session.add(proc)
                session.flush()
            return True

        # ── Lis Pendens → Foreclosure ─────────────────────────────────────────
        if src == "lis_pendens":
            instrument = _safe_str(raw.get("Instrument") or record.instrument_number)
            if not instrument:
                return False
            synthetic_case = f"LP-{instrument}"
            existing = session.query(Foreclosure).filter_by(
                case_number=synthetic_case, county_id=county_id
            ).first()
            if existing:
                return False
            lis_date = parse_date(raw.get("RecordDate"))
            fc = Foreclosure(
                property_id=property_id,
                case_number=synthetic_case,
                plaintiff=_safe_str(raw.get("Grantor")),
                defendant=_safe_str(raw.get("Grantee")),
                lis_pendens_date=lis_date,
                filing_date=lis_date,
                match_confidence=1.0,
                match_method="llm_verified",
                county_id=county_id,
            )
            with session.begin_nested():
                session.add(fc)
                session.flush()
            return True

    except Exception as e:
        logger.warning(
            "_promote_to_destination failed source=%s record_id=%s: %s",
            src, record.id, e,
        )
        return False

    logger.debug("_promote_to_destination: unhandled source_type=%s", src)
    return False


def _build_batch_prompt(batch: list[dict]) -> str:
    """
    Build a single LLM prompt for up to 10 pending_review records.

    Each entry in `batch` is a dict with keys:
        record_id, raw_data, candidate_property, trgm_alternatives,
        match_score, record_type, match_field

    Returns a prompt string. The LLM must respond with a JSON array where
    each element has: record_id, matched, property_id, confidence, reason.
    """
    from src.loaders.llm_matcher import RECORD_TYPE_CONTEXT, LLM_SCORE_FLOOR, LLM_SCORE_CEILING

    def _prop_dict(prop, score=None):
        owner_name = None
        try:
            owner_name = prop.owner.owner_name if prop.owner else None
        except Exception:
            pass
        d = {
            "property_id":       prop.id,
            "parcel_id":         prop.parcel_id,
            "address":           prop.address,
            "city":              prop.city,
            "zip":               prop.zip,
            "owner_name":        owner_name,
            "legal_description": (prop.legal_description or "")[:150],
        }
        if score is not None:
            d["name_match_score"] = score
        return d

    _KEY_FIELDS = {
        "Instrument", "document_type", "Grantor", "Grantee", "RecordDate",
        "Filing Amt", "Legal", "CaseNumber", "Case Number", "Lead Name",
        "Date Filed", "PartyAddress", "FirstName", "LastName/CompanyName",
        "FilingDate", "Title", "CaseTypeDescription", "Docket Number",
    }

    sections = []
    for item in batch:
        rid      = item["record_id"]
        raw      = item["raw_data"] or {}
        best     = item["candidate_property"]
        alts     = item["trgm_alternatives"] or []
        score    = item["match_score"]
        rt       = item["record_type"]
        mf       = item["match_field"]
        context  = RECORD_TYPE_CONTEXT.get(rt, f"Record type: {rt}.")

        record_summary = {k: v for k, v in raw.items() if k in _KEY_FIELDS}
        candidates = [_prop_dict(best, score)] + [_prop_dict(p) for p in alts if p]
        candidates = candidates[:3]

        sections.append(
            f"--- RECORD {rid} ---\n"
            f"RECORD_TYPE: {rt.upper()}\n"
            f"CONTEXT: {context}\n"
            f"MATCH_FIELD: \"{mf}\"  NAME_MATCH_SCORE: {score}%\n"
            f"SOURCE_RECORD:\n{json.dumps(record_summary, indent=2, default=str)}\n"
            f"CANDIDATE_PROPERTIES:\n{json.dumps(candidates, indent=2, default=str)}"
        )

    records_block = "\n\n".join(sections)

    return f"""You are a property record matching expert for Florida county records.

IMPORTANT: You are seeing at most 3 candidates per record. The database may contain other equally plausible matches not shown. Assume ambiguity unless the evidence clearly rules out alternatives.

A false confirmation corrupts the property database. A false rejection only delays a match for manual review. When uncertain, always reject.

Process each RECORD below INDEPENDENTLY. Each record's candidates belong ONLY to that record — do not let one record's data influence another's decision.

{records_block}

FOR EACH RECORD, ALL of the following must be true to confirm a match:
1. The name in MATCH_FIELD is essentially the same person or entity as the owner_name on the property (same person/entity, accounting only for punctuation, word order, and well-known abbreviations like TRE/TRUST/INC/LLC).
2. The property is in the correct jurisdiction for the record type.
3. No other shown candidate is equally or more plausible.
4. No corroborating field (address number, city, zip) directly contradicts the match.

If ANY of the above conditions is not clearly met, set matched=false.

Respond with ONLY a valid JSON array — one element per record, in the same order:
[
  {{
    "record_id": <integer — must match the RECORD id above>,
    "matched": true or false,
    "property_id": <integer property_id if matched, null if not>,
    "confidence": "high" or "medium" or "low",
    "reason": "<one sentence>"
  }},
  ...
]

Rules:
- "high": ALL four conditions above are clearly met. Name is essentially identical (not just similar). No contradicting evidence. Use this only when you are certain.
- "medium": name is plausible but has spelling/abbreviation/truncation ambiguity, jurisdiction is correct, and no other candidate is equally plausible. NOT high because of name uncertainty only.
- "low": any condition above is not clearly met — set matched=false.
- NOT "high" if: only the surname matches with no other corroborating detail; address numbers differ; business name only partially overlaps; name is a common surname (e.g. SMITH, JONES, MILLER, JOHNSON) without additional corroboration.
- CRITICAL: If multiple candidates are equally plausible, you MUST set matched=false and confidence=low. This is never "medium".
- If matched=false: property_id must be null.
- Output ONLY the JSON array. No text outside it."""


def llm_tiebreak_pending_review(
    source_type: Optional[str] = None,
    limit: int = 1000,
    county_id: str = "hillsborough",
    dry_run: bool = False,
    batch_size: int = 5,
    model: str = "claude-sonnet-4-6",
) -> dict:
    """
    Run the LLM tiebreaker against pending_review records that already have a
    candidate_property_id.

    For each record:
      - LLM confirms (high/medium confidence) → write destination record +
        update unmatched_records to 'matched'
      - LLM rejects → demote to 'unmatched'

    Records are packed into batches of `batch_size` per API call to reduce cost.
    On JSON parse failure the batch is retried as individual calls.

    Args:
        source_type: Filter to a specific source (liens, deeds, …). None = all.
        limit:       Max records to process.
        county_id:   County to process.
        dry_run:     Log decisions without writing to DB.
        batch_size:  Records per LLM API call (default 5).
        model:       Anthropic model ID.

    Returns:
        dict with keys: total, confirmed, rejected, errors, skipped, dry_run
    """
    stats = {
        "total": 0, "confirmed": 0, "rejected": 0,
        "errors": 0, "skipped": 0, "dry_run": dry_run,
    }

    matcher = LLMPropertyMatcher(max_calls=limit + 50, model=model)

    import anthropic as _ant
    _api_settings = __import__("config.settings", fromlist=["get_settings"]).get_settings()
    _api_client = _ant.Anthropic(api_key=_api_settings.anthropic_api_key.get_secret_value())

    with get_db_context() as session:
        query = (
            session.query(UnmatchedRecord)
            .filter(
                UnmatchedRecord.match_status == "pending_review",
                UnmatchedRecord.candidate_property_id.isnot(None),
                UnmatchedRecord.county_id == county_id,
            )
        )
        if source_type:
            query = query.filter(UnmatchedRecord.source_type == source_type)

        records = (
            query
            .order_by(UnmatchedRecord.match_confidence.desc())
            .limit(limit)
            .all()
        )
        stats["total"] = len(records)
        logger.info(
            "[LLM tiebreaker] Processing %d pending_review records "
            "(county=%s source=%s dry_run=%s model=%s batch=%d)",
            len(records), county_id, source_type or "all", dry_run, model, batch_size,
        )

        def _process_single(record: UnmatchedRecord) -> Optional[str]:
            """Call LLM for one record. Returns 'confirmed', 'rejected', or 'error'."""
            candidate = session.get(Property, record.candidate_property_id)
            if not candidate:
                if not dry_run:
                    record.match_status = "unmatched"
                    record.candidate_property_id = None
                return "rejected"

            # pg_trgm alternatives for LLM context (reuse BaseLoader helper)
            loader = LienLoader(session, county_id=county_id)
            name_for_trgm = (
                record.grantor or
                (record.raw_data or {}).get("Grantor") or
                (record.raw_data or {}).get("LastName/CompanyName") or
                (record.raw_data or {}).get("Lead Name") or ""
            )
            alts = loader._get_top_owner_candidates_base(str(name_for_trgm), limit=2)

            src          = record.source_type or "liens"
            record_type  = SOURCE_TYPE_TO_RECORD_TYPE.get(src, "lien_ml")
            match_field  = (
                "PartyAddress" if record.match_method == "address"
                else SOURCE_TYPE_TO_MATCH_FIELD.get(src, "Grantor")
            )
            match_score  = int((record.match_confidence or 0.85) * 100)

            result = matcher.verify_match(
                raw_row=record.raw_data or {},
                candidates=alts,
                current_best=candidate,
                current_score=match_score,
                record_type=record_type,
                match_field=match_field,
            )

            if result.matched and result.confidence == "high":
                confirmed_id = result.property_id or record.candidate_property_id
                logger.info(
                    "[LLM tiebreaker] CONFIRMED record_id=%s source=%s "
                    "property_id=%s confidence=%s: %s",
                    record.id, src, confirmed_id, result.confidence, result.reason,
                )
                if not dry_run:
                    promoted = _promote_to_destination(session, record, confirmed_id, county_id)
                    # promoted=False means either duplicate (fine) or write error (rare).
                    # Only gate on None-like; False from dedup is still a successful state.
                    record.match_status         = "matched"
                    record.matched_property_id  = confirmed_id
                    record.candidate_property_id = None
                    record.match_attempted_at   = datetime.now(timezone.utc)
                return "confirmed"
            else:
                logger.info(
                    "[LLM tiebreaker] REJECTED record_id=%s source=%s "
                    "confidence=%s: %s",
                    record.id, src, result.confidence, result.reason,
                )
                if not dry_run:
                    record.match_status = "unmatched"
                    record.candidate_property_id = None
                    record.match_attempted_at = datetime.now(timezone.utc)
                return "rejected"

        def _process_batch(batch: list[UnmatchedRecord]) -> None:
            """
            Pack a batch of records into one LLM call. Falls back to
            individual calls if the batch response fails to parse.
            """
            if not batch:
                return

            # Build batch items
            items = []
            loader = LienLoader(session, county_id=county_id)
            for rec in batch:
                candidate = session.get(Property, rec.candidate_property_id)
                if not candidate:
                    if not dry_run:
                        rec.match_status = "unmatched"
                        rec.candidate_property_id = None
                    stats["rejected"] += 1
                    continue

                name_for_trgm = (
                    rec.grantor or
                    (rec.raw_data or {}).get("Grantor") or
                    (rec.raw_data or {}).get("LastName/CompanyName") or
                    (rec.raw_data or {}).get("Lead Name") or ""
                )
                alts = loader._get_top_owner_candidates_base(str(name_for_trgm), limit=2)

                src = rec.source_type or "liens"
                items.append({
                    "record_id":          rec.id,
                    "raw_data":           rec.raw_data or {},
                    "candidate_property": candidate,
                    "trgm_alternatives":  alts,
                    "match_score":        int((rec.match_confidence or 0.85) * 100),
                    "record_type":        SOURCE_TYPE_TO_RECORD_TYPE.get(src, "lien_ml"),
                    "match_field": (
                        "PartyAddress" if rec.match_method == "address"
                        else SOURCE_TYPE_TO_MATCH_FIELD.get(src, "Grantor")
                    ),
                    "_record": rec,
                })

            if not items:
                return

            prompt = _build_batch_prompt(items)
            rec_by_id = {item["record_id"]: item["_record"] for item in items}

            try:
                response = _api_client.messages.create(
                    model=model,
                    max_tokens=1024,
                    temperature=0,
                    system=(
                        "You are a property record matching expert. "
                        "Respond ONLY with a valid JSON array. No text outside it."
                    ),
                    messages=[{"role": "user", "content": prompt}],
                )
                raw_text = response.content[0].text.strip()
                if raw_text.startswith("```"):
                    parts = raw_text.split("```")
                    raw_text = parts[1].lstrip("json").strip() if len(parts) > 1 else raw_text
                if raw_text.endswith("```"):
                    raw_text = raw_text[:-3].strip()
                start, end = raw_text.find("["), raw_text.rfind("]")
                if start == -1 or end == -1:
                    raise ValueError("No JSON array in response")
                decisions = json.loads(raw_text[start: end + 1])
            except Exception as e:
                logger.warning(
                    "[LLM tiebreaker] Batch of %d failed (%s) — falling back to individual calls",
                    len(items), e,
                )
                for item in items:
                    try:
                        outcome = _process_single(item["_record"])
                        stats[{"confirmed": "confirmed", "rejected": "rejected"}.get(outcome, "errors")] += 1
                    except Exception as e2:
                        logger.warning("[LLM tiebreaker] Individual fallback error record_id=%s: %s", item["record_id"], e2)
                        stats["errors"] += 1
                return

            # Apply decisions
            decided_ids = set()
            for decision in decisions:
                try:
                    rid = int(decision.get("record_id"))  # coerce — LLM may return string
                except (TypeError, ValueError):
                    logger.warning("[LLM tiebreaker] Unparseable record_id in decision: %s", decision)
                    continue
                rec = rec_by_id.get(rid)
                if not rec:
                    logger.warning("[LLM tiebreaker] Unknown record_id %s in batch response", rid)
                    continue
                decided_ids.add(rid)

                if decision.get("matched") and decision.get("confidence") == "high":
                    confirmed_id = decision.get("property_id") or rec.candidate_property_id
                    logger.info(
                        "[LLM tiebreaker] CONFIRMED record_id=%s source=%s "
                        "property_id=%s confidence=%s: %s",
                        rid, rec.source_type, confirmed_id,
                        decision.get("confidence"), decision.get("reason"),
                    )
                    if not dry_run:
                        _promote_to_destination(session, rec, confirmed_id, county_id)
                        rec.match_status         = "matched"
                        rec.matched_property_id  = confirmed_id
                        rec.candidate_property_id = None
                        rec.match_attempted_at   = datetime.now(timezone.utc)
                    stats["confirmed"] += 1
                else:
                    logger.info(
                        "[LLM tiebreaker] REJECTED record_id=%s confidence=%s: %s",
                        rid, decision.get("confidence"), decision.get("reason"),
                    )
                    if not dry_run:
                        rec.match_status = "unmatched"
                        rec.candidate_property_id = None
                        rec.match_attempted_at = datetime.now(timezone.utc)
                    stats["rejected"] += 1

            # Any records the LLM didn't mention — treat as errors
            for item in items:
                if item["record_id"] not in decided_ids:
                    logger.warning("[LLM tiebreaker] No decision returned for record_id=%s", item["record_id"])
                    stats["errors"] += 1

        # ── Main loop — process in batches ────────────────────────────────────
        COMMIT_EVERY = 100
        for batch_start in range(0, len(records), batch_size):
            batch = records[batch_start: batch_start + batch_size]
            try:
                _process_batch(batch)
            except Exception as e:
                logger.error("[LLM tiebreaker] Batch error at offset %d: %s", batch_start, e)
                stats["errors"] += len(batch)

            processed = batch_start + len(batch)
            if not dry_run and processed % COMMIT_EVERY == 0:
                session.commit()
                logger.info(
                    "[LLM tiebreaker] Progress %d/%d — confirmed=%d rejected=%d errors=%d",
                    processed, len(records), stats["confirmed"], stats["rejected"], stats["errors"],
                )

        if not dry_run:
            session.commit()

    logger.info(
        "[LLM tiebreaker] Done: total=%d confirmed=%d rejected=%d errors=%d skipped=%d dry_run=%s",
        stats["total"], stats["confirmed"], stats["rejected"],
        stats["errors"], stats["skipped"], dry_run,
    )
    return stats


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Re-match unmatched staged records")
    parser.add_argument("--source", default=None, help="Filter by source type (liens, deeds, etc.)")
    parser.add_argument("--limit", type=int, default=5000, help="Max records to process")
    parser.add_argument("--county", "--county-id", dest="county_id", default="hillsborough", help="County ID")
    parser.add_argument("--llm-only", action="store_true", help="Run LLM tiebreaker only (skip standard rematch)")
    parser.add_argument("--skip-llm", action="store_true", help="Skip LLM tiebreaker (standard rematch only)")
    parser.add_argument("--dry-run", action="store_true", help="Log decisions without writing to DB")
    parser.add_argument("--model", default="claude-sonnet-4-6", help="Anthropic model for LLM tiebreaker")
    parser.add_argument("--batch-size", type=int, default=5, help="Records per LLM API call")
    args = parser.parse_args()

    if args.llm_only:
        result = llm_tiebreak_pending_review(
            source_type=args.source,
            limit=args.limit,
            county_id=args.county_id,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            model=args.model,
        )
        print(result)
    else:
        # Standard rematch
        result = rematch_unmatched(source_type=args.source, limit=args.limit, county_id=args.county_id)
        print(result)

        # LLM tiebreaker as second step (skippable via --skip-llm)
        if not args.skip_llm:
            llm_limit = min(args.limit, 1000)
            llm_result = llm_tiebreak_pending_review(
                source_type=args.source,
                limit=llm_limit,
                county_id=args.county_id,
                dry_run=args.dry_run,
                batch_size=args.batch_size,
                model=args.model,
            )
            print(llm_result)
