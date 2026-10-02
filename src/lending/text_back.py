"""WP-GL-9 missed-call text-back (GoHighLevel).

Consumes ``lending.missed_call_events`` rows that the single BatchDialer CDR poller queues for an
unanswered outbound lending call. Each event gets exactly one decision: sent, or why not. Texts
go out only through GHL, only to consented numbers, once per Eastern day, within 60 seconds of
the call. Logs carry phone hashes, never phones, names or message bodies.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from config.lending_text_back import (
    CAPS, ENTITY_TOKENS, FALLBACK_CALLER, GENERAL, QUEUE_TEMPLATES, TEMPLATES, TEMPLATE_NEEDS,
)

_NAME = re.compile(r"^[A-Za-z][A-Za-z'\-]+$")


@dataclass(frozen=True)
class PendingText:
    event_id: int
    call_id: str
    phone: str
    ended_at: datetime
    property_address: Optional[str]
    queue: Optional[str]
    caller_name: Optional[str]
    borrower_name: Optional[str]
    county: Optional[str]


def first_name_of(borrower_name: Optional[str]) -> Optional[str]:
    """A person's first name, or None for blanks, initials and entity-looking names."""
    tokens = (borrower_name or "").split()
    if not tokens or any(t.strip(".,").lower() in ENTITY_TOKENS for t in tokens):
        return None
    first = tokens[0]
    if not _NAME.match(first):
        return None
    return first.title() if first.isupper() or first.islower() else first


def choose_template(item: PendingText) -> str:
    key = QUEUE_TEMPLATES.get(item.queue or "", GENERAL)
    for field in TEMPLATE_NEEDS[key]:
        if not (getattr(item, field) or "").strip():
            return GENERAL
    return key


def format_number(e164: str) -> str:
    digits = re.sub(r"\D", "", e164)
    if len(digits) == 11 and digits.startswith("1"):
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return e164


def render_body(item: PendingText, template_key: str, *, number: str) -> str:
    first = first_name_of(item.borrower_name)
    street = (item.property_address or "").split(",")[0].strip()
    return TEMPLATES[template_key].format(
        greeting=f"Hi {first[:CAPS['first_name']]}" if first else "Hi",
        caller=(item.caller_name or "").strip()[:CAPS["caller"]] or FALLBACK_CALLER,
        property=street[:CAPS["property"]],
        county=(item.county or "").strip()[:CAPS["county"]],
        number=format_number(number),
    )
