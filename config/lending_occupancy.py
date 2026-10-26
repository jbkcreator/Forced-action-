"""Chunk 1 Addendum 1.1.2: business-purpose loans only, so an owner-occupied home is never brokered.

Every intake stores one of OCCUPANCIES. A form answer the server cannot read is ``unknown`` and is not blocked:
the caller confirms occupancy on the call (LendingFlow and after-hours never carry it). The question wording is
pending client review.
"""
from __future__ import annotations

from typing import Optional

OWNER_OCCUPIED = "owner_occupied"
INVESTMENT = "investment"
UNKNOWN = "unknown"
OCCUPANCIES = (OWNER_OCCUPIED, INVESTMENT, UNKNOWN)

WEB_QUESTION = "Will you or a family member live in this property?"
PARTNER_QUESTION = "Will the borrower or a family member live in this property?"

# file_routing.hold_reason for an owner-occupied file: no lender, no caller queue, no alarm.
HOMESTEAD_HOLD_REASON = "consumer_homestead_excluded"

_YES = {"yes", "true", "on", "1", OWNER_OCCUPIED}
_NO = {"no", "false", "off", "0", INVESTMENT}


def occupancy_from_answer(value: Optional[str]) -> str:
    answer = str(value or "").strip().lower()
    return OWNER_OCCUPIED if answer in _YES else INVESTMENT if answer in _NO else UNKNOWN
