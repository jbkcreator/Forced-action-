"""Find a Backflip name, domain or phone number in outbound text (config/lending_lane.py)."""
from __future__ import annotations

import re
from typing import Optional

from config.lending_lane import BACKFLIP_DENY_DOMAINS, BACKFLIP_DENY_PHONES, BACKFLIP_DENY_WORDS
from src.services.phone_utils import normalize

# A US number in any common spelling: +1 656-209-6700, (656) 209 6700, 656.209.6700, 16562096700.
_PHONE = re.compile(r"(?<!\d)(?:\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}(?!\d)")
_DENIED_PHONES = frozenset(BACKFLIP_DENY_PHONES)


def backflip_hit(*values: Optional[str]) -> Optional[str]:
    """``"word"``, ``"domain"`` or ``"phone"`` for the first Backflip match in any value, else None."""
    for value in values:
        if not value:
            continue
        lowered = value.casefold()
        if any(word in lowered for word in BACKFLIP_DENY_WORDS):
            return "word"
        if any(domain in lowered for domain in BACKFLIP_DENY_DOMAINS):
            return "domain"
        if any(normalize(match) in _DENIED_PHONES for match in _PHONE.findall(value)):
            return "phone"
    return None
