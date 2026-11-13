"""P2.4 pure rules: when a deal is stuck, who holds it up, which chase step is due, and a lender's checklist."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Literal, Optional
from zoneinfo import ZoneInfo

from config.lending_closing import CHASE_DAYS, CONDITION_ITEMS, LENDER_CHECKLISTS, STAGE_OWNER, STUCK_AFTER, ChecklistItem
from config.lending_compliance import DEFAULT_TZ

logger = logging.getLogger(__name__)

Owner = Literal["borrower", "partner", "lender", "title"]
_EASTERN = ZoneInfo(DEFAULT_TZ)


def is_stuck(entered_at: datetime, now: datetime) -> bool:
    return now - entered_at > STUCK_AFTER


def blame(stage_key: str, *, missing_items: int, from_partner: bool) -> Owner:
    """Missing documents put the deal on the borrower (or the partner who sent it); otherwise the stage decides."""
    if missing_items > 0:
        return "partner" if from_partner else "borrower"
    return STAGE_OWNER.get(stage_key, "borrower")  # type: ignore[return-value]


def due_chase_day(accepted_at: datetime, now: datetime, sent_days: frozenset[int]) -> Optional[int]:
    """The chase step due now: the latest of CHASE_DAYS reached and not yet taken, so an outage never sends two."""
    elapsed = (now.astimezone(_EASTERN).date() - accepted_at.astimezone(_EASTERN).date()).days
    last_sent = max(sent_days, default=-1)
    due = [day for day in CHASE_DAYS if last_sent < day <= elapsed]
    return due[-1] if due else None


def build_checklist(lender_key: str, condition_codes: Iterable[str]) -> list[ChecklistItem]:
    if lender_key not in LENDER_CHECKLISTS:
        raise ValueError(f"no document checklist for lender {lender_key!r}")
    items = list(LENDER_CHECKLISTS[lender_key])
    for code in condition_codes:
        item = CONDITION_ITEMS.get(code)
        if item is None:
            logger.info("[doc-chase] quote condition %s adds no document", code)
        elif item not in items:
            items.append(item)
    return items
