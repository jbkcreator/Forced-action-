"""src/agents/reply_concierge/backflip_email_parser.py

WP-T2-6 -- pure parser for Backflip notification emails. No DB, no
network, no side effects: text in, a structured event out (or None).

No real Backflip notification samples exist yet (client clarifications
Q7/Q8/Q13 all still open -- exact mailbox and real email format
unconfirmed). This is built against a reasonable ASSUMED format and is
deliberately isolated in its own module so that recalibrating against real
samples later is a self-contained change -- see the spec's adapter-
isolation requirement ("adapter interface is isolated so a Backflip API or
webhook can replace it without touching the monitoring logic").
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from config.fa_max_stage_monitoring import BACKFLIP_STAGE_KEYS

_REF_PATTERN = re.compile(r"\bBF-\d{4,}\b")

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
