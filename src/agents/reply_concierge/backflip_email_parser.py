"""src/agents/reply_concierge/backflip_email_parser.py

WP-T2-6 -- parser for Backflip notification emails. No DB, no side
effects: text in, a structured event out (or None). Two paths: a fast,
free, deterministic regex path tried first, and an LLM fallback (Claude,
one-shot extraction call -- same pattern as src/loaders/llm_matcher.py,
not a LangGraph agent flow) tried only when the regex path can't make
sense of the email at all.

No real Backflip notification samples exist yet (client clarifications
Q7/Q8/Q13 all still open -- exact mailbox and real email format
unconfirmed), so the stage/document/terms regex patterns are guesses.
The ref pattern deliberately does NOT assume a vendor-specific prefix
like 'BF-####' -- Backflip's real reference format is unknown, and our
own test data already uses a different shape ('b1234'). _REF_PATTERN
instead matches the general shape of a reference token (letters followed
by an optional hyphen and digits) and leaves the actual truth check to
resolve_opportunity_by_backflip_ref() downstream -- a token that doesn't
match a real opportunity is dropped there, so a broad pattern costs
nothing but a log line. The LLM fallback exists to cover the rest of that gap:
an automated system's notification emails are normally one fixed template
(regex is the right primary tool for that -- deterministic, free,
instantly testable), but until the real template is confirmed, a genuine
Backflip email in a shape the regex doesn't expect would otherwise be
silently dropped. The regex path stays primary once the real format is
known; this fallback is what keeps things working in the meantime and
remains a reasonable safety net after, for whatever the regex doesn't
anticipate.

Isolated in its own module so recalibrating either path against real
samples later is a self-contained change -- see the spec's adapter-
isolation requirement ("adapter interface is isolated so a Backflip API or
webhook can replace it without touching the monitoring logic").
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

from config.fa_max_stage_monitoring import BACKFLIP_STAGE_KEYS

logger = logging.getLogger(__name__)

_LLM_MODEL = "claude-sonnet-4-5-20250929"
_EVENT_TYPES = frozenset({"stage_change", "document_request", "terms"})

_REF_PATTERN = re.compile(r"\b[A-Za-z]{1,10}-\d+\b|\b[A-Za-z]{1,10}\d{3,}\b")

_STAGE_SIGNALS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"cleared to close", re.IGNORECASE), "cleared_to_close"),
    (re.compile(r"conditional approval", re.IGNORECASE), "conditional_approval"),
    (re.compile(r"under review", re.IGNORECASE), "under_review"),
    (re.compile(r"documents? requested|document.* need|action needed", re.IGNORECASE), "docs_requested"),
    (re.compile(r"\bdeclined\b", re.IGNORECASE), "declined"),
)

_DOCUMENT_REQUEST_PATTERN = re.compile(
    r"following document[s]?:\s*(?P<doc>.+?)(?:\.\s|\.$|$)", re.IGNORECASE,
)
_TERMS_SIGNAL = re.compile(r"term sheet|loan amount", re.IGNORECASE)
_LOAN_AMOUNT_PATTERN = re.compile(r"loan amount\s*\$?([\d,]+(?:\.\d{2})?)", re.IGNORECASE)
_TERM_MONTHS_PATTERN = re.compile(r"term\s+(\d+)\s+months?", re.IGNORECASE)


@dataclass(frozen=True)
class ParsedBackflipEvent:
    event_type: str  # "stage_change" | "document_request" | "terms"
    backflip_ref: Optional[str]
    stage: Optional[str] = None
    document_name: Optional[str] = None
    loan_amount_cents: Optional[int] = None
    maturity_months: Optional[int] = None


def _extract_ref(subject: str, body_text: str) -> Optional[str]:
    match = _REF_PATTERN.search(subject) or _REF_PATTERN.search(body_text)
    return match.group(0) if match else None


def parse_backflip_notification(subject: str, body_text: str) -> Optional[ParsedBackflipEvent]:
    """Regex first (deterministic, free, instantly testable). Falls back to
    an LLM extraction call only when the regex path finds nothing -- either
    because no reference-shaped token is present, or a ref is present but
    none of the known event patterns match. Never raises; a fallback failure
    (API error, malformed response) degrades to None like a genuine
    non-match, so a poller or sweep calling this never needs its own
    try/except around it.
    """
    event = _parse_via_regex(subject, body_text)
    if event is not None:
        return event
    return _parse_via_llm(subject, body_text)


def _parse_via_regex(subject: str, body_text: str) -> Optional[ParsedBackflipEvent]:
    combined = f"{subject}\n{body_text}"
    backflip_ref = _extract_ref(subject, body_text)
    if backflip_ref is None:
        return None

    if _TERMS_SIGNAL.search(combined):
        amount_match = _LOAN_AMOUNT_PATTERN.search(combined)
        months_match = _TERM_MONTHS_PATTERN.search(combined)
        loan_amount_cents = None
        if amount_match:
            dollars = float(amount_match.group(1).replace(",", ""))
            loan_amount_cents = round(dollars * 100)
        maturity_months = int(months_match.group(1)) if months_match else None
        return ParsedBackflipEvent(
            event_type="terms", backflip_ref=backflip_ref,
            loan_amount_cents=loan_amount_cents, maturity_months=maturity_months,
        )

    doc_match = _DOCUMENT_REQUEST_PATTERN.search(combined)
    if doc_match:
        return ParsedBackflipEvent(
            event_type="document_request", backflip_ref=backflip_ref,
            document_name=doc_match.group("doc").strip(),
        )

    for pattern, stage in _STAGE_SIGNALS:
        if pattern.search(combined):
            assert stage in BACKFLIP_STAGE_KEYS  # config/parser drift guard
            return ParsedBackflipEvent(event_type="stage_change", backflip_ref=backflip_ref, stage=stage)

    return None


def _parse_via_llm(subject: str, body_text: str) -> Optional[ParsedBackflipEvent]:
    """One-shot Claude extraction call -- same client/pattern as
    src/loaders/llm_matcher.py (direct anthropic SDK call for a single
    structured-JSON classification, not a LangGraph agent flow, so this
    doesn't fall under CLAUDE.md's "no raw Anthropic SDK loops for agent
    flows" rule -- there's no loop here, one call in, one parsed result out).

    Fails closed: any error (API failure, non-JSON response, a value
    outside the known-valid sets) returns None, identical to "the regex
    path found nothing" -- a caller never needs to distinguish "this isn't
    a Backflip event" from "the LLM fallback broke."
    """
    import anthropic

    from config.settings import get_settings

    try:
        settings = get_settings()
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key.get_secret_value())
        response = client.messages.create(
            model=_LLM_MODEL,
            max_tokens=512,
            temperature=0,
            system=(
                "You extract structured data from Backflip lending-portal notification "
                "emails for Forced Action, a mortgage broker. Respond ONLY with a valid "
                "JSON object. No explanation outside the JSON."
            ),
            messages=[{"role": "user", "content": _build_llm_prompt(subject, body_text)}],
        )
        raw_text = response.content[0].text.strip()
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
        parsed = json.loads(raw_text.strip())
        return _event_from_llm_response(parsed)
    except Exception as exc:
        logger.error("backflip_email_parser: LLM fallback call failed: %s", exc)
        return None


def _build_llm_prompt(subject: str, body_text: str) -> str:
    stage_keys = ", ".join(sorted(BACKFLIP_STAGE_KEYS))
    return f"""This email may be an automated notification from Backflip's lending
portal about a borrower's loan file. It may also be unrelated (e.g. a
newsletter). Determine which, and if related, what it's telling us.

SUBJECT:
{subject}

BODY:
{body_text}

Respond with ONLY this JSON object -- no text outside it:
{{
  "is_backflip_notification": true or false,
  "event_type": "stage_change" or "document_request" or "terms" or null,
  "backflip_ref": "<Backflip's reference/application ID for this file, or null if none is visible>",
  "stage": "<one of [{stage_keys}], or null>",
  "document_name": "<the specific document being requested, or null>",
  "loan_amount_cents": <integer, the loan amount in cents if terms are mentioned, or null>,
  "maturity_months": <integer, the loan term in months if terms are mentioned, or null>
}}

Rules:
- "is_backflip_notification": false for anything that isn't genuinely about
  a specific borrower's file status, document request, or issued terms --
  newsletters, marketing, unrelated correspondence.
- "event_type" is exactly one of the three values when is_backflip_notification
  is true; null when false.
- "stage" must be exactly one of the listed values, or null -- never invent a
  new stage name.
- If you cannot identify a backflip_ref, set is_backflip_notification to false
  even if the email otherwise looks like a real notification -- without a
  reference we cannot match it to a file in our system.
"""


def _event_from_llm_response(parsed: dict) -> Optional[ParsedBackflipEvent]:
    """Validates the LLM's JSON against the same known-valid sets the regex
    path is constrained to, mirroring src/loaders/llm_matcher.py's own
    safety pattern (never trust an LLM-picked value outside a verified set)."""
    if not isinstance(parsed, dict) or not parsed.get("is_backflip_notification"):
        return None

    backflip_ref = parsed.get("backflip_ref")
    if not backflip_ref or not isinstance(backflip_ref, str):
        logger.warning("backflip_email_parser: LLM fallback found no backflip_ref -- dropping")
        return None

    event_type = parsed.get("event_type")
    if event_type not in _EVENT_TYPES:
        logger.warning(
            "backflip_email_parser: LLM fallback returned invalid event_type=%r -- dropping",
            event_type,
        )
        return None

    if event_type == "stage_change":
        stage = parsed.get("stage")
        if stage not in BACKFLIP_STAGE_KEYS:
            logger.warning(
                "backflip_email_parser: LLM fallback returned stage=%r outside BACKFLIP_STAGE_KEYS -- dropping",
                stage,
            )
            return None
        return ParsedBackflipEvent(event_type="stage_change", backflip_ref=backflip_ref, stage=stage)

    if event_type == "document_request":
        document_name = parsed.get("document_name")
        if not document_name or not isinstance(document_name, str):
            return None
        return ParsedBackflipEvent(
            event_type="document_request", backflip_ref=backflip_ref, document_name=document_name.strip(),
        )

    # terms
    loan_amount_cents = parsed.get("loan_amount_cents")
    maturity_months = parsed.get("maturity_months")
    return ParsedBackflipEvent(
        event_type="terms", backflip_ref=backflip_ref,
        loan_amount_cents=int(loan_amount_cents) if isinstance(loan_amount_cents, (int, float)) else None,
        maturity_months=int(maturity_months) if isinstance(maturity_months, (int, float)) else None,
    )
