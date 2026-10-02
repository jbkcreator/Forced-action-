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
_INITIAL = re.compile(r"^[A-Za-z]\.?$")
_INVALID_CHARS = re.compile(r"[&/,\d]")


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
    """A person's first name, or None for blanks, initials and entity-looking names.

    Accepts only 2-3 token names where all tokens are name-like (letters, apostrophes, hyphens),
    no entity words, no special characters, and first token <= 20 chars.
    """
    name = (borrower_name or "").strip()
    if not name or _INVALID_CHARS.search(name):
        return None

    tokens = name.split()
    if len(tokens) not in (2, 3):
        return None

    # Check entity tokens and token lengths
    for token in tokens:
        cleaned = token.strip(".,").lower()
        if cleaned in ENTITY_TOKENS:
            return None

    first = tokens[0]
    if len(first) > CAPS["first_name"]:
        return None

    # For 2-token names: both must be name-like (no initials)
    if len(tokens) == 2:
        if not (_NAME.match(first) and _NAME.match(tokens[1])):
            return None
    # For 3-token names: first and third are name-like, second can be initial
    else:
        if not _NAME.match(first):
            return None
        if not (_INITIAL.match(tokens[1]) and _NAME.match(tokens[2])):
            return None

    return first.title() if first.isupper() or first.islower() else first


def _street(item: PendingText) -> str:
    """Extract street address (part before first comma), whitespace-collapsed."""
    addr = (item.property_address or "").split(",")[0].strip()
    return " ".join(addr.split())


def choose_template(item: PendingText) -> str:
    key = QUEUE_TEMPLATES.get(item.queue or "", GENERAL)
    for field in TEMPLATE_NEEDS[key]:
        if field == "property_address":
            if not _street(item):
                return GENERAL
        else:
            if not (getattr(item, field) or "").strip():
                return GENERAL
    return key


def format_number(e164: str) -> str:
    digits = re.sub(r"\D", "", e164)
    if len(digits) == 11 and digits.startswith("1"):
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return e164


def render_body(item: PendingText, template_key: str, *, number: str) -> str:
    if not number or not number.strip():
        raise ValueError("a texting number is required")

    first = first_name_of(item.borrower_name)
    street = _street(item)

    # Collapse whitespace in all substituted values
    caller = " ".join((item.caller_name or "").split())[:CAPS["caller"]] or FALLBACK_CALLER
    county = " ".join((item.county or "").split())[:CAPS["county"]]
    street = " ".join(street.split())[:CAPS["property"]]

    return TEMPLATES[template_key].format(
        greeting=f"Hi {first[:CAPS['first_name']]}" if first else "Hi",
        caller=caller,
        property=street,
        county=county,
        number=format_number(number),
    )
