"""Seat-cap counter for the primary lender.

OPEN Q1: Josh to confirm whether the cap counts 'funded' or 'routed' actions.
Default is 'funded' (``config.lending_routing.SEAT_CAP_COUNTED_ACTION``).

The count is derived live from ``file_routing_events`` for the current
Eastern calendar month, so no reset job is needed and the count survives
restarts.

Concurrency: the check is a plain count with no lock.  With the default ``funded``
counting that is safe, because routing never writes ``funded`` events.  If Q1 is
answered "routed", two files routed at the same moment could both see the seat
free and overshoot the cap by one; add a lock on the primary lender row before
switching the counted action.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import ROUTING_TZ, SEAT_CAP_COUNTED_ACTION
from src.lending.routing.rules import LenderRow

logger = logging.getLogger(__name__)


def _et_month_start() -> datetime:
    """First second of the current calendar month in US/Eastern, as UTC."""
    now_et = datetime.now(ROUTING_TZ)
    start_et = now_et.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start_et.astimezone(timezone.utc)


def seat_count(lender: LenderRow, db: Session) -> int:
    """Count of SEAT_CAP_COUNTED_ACTION events for this lender in the current ET month."""
    month_start = _et_month_start()
    row = db.execute(
        text(
            "SELECT COUNT(*) AS cnt"
            "  FROM lending.file_routing_events e"
            " WHERE lender_id = :lid"
            "   AND action = :action"
            "   AND created_at >= :month_start"
            "   AND NOT EXISTS (SELECT 1 FROM lending.file_routing fr WHERE fr.is_test"
            "                    AND (fr.lead_id = e.lead_id OR fr.loan_file_id = e.loan_file_id))"
        ),
        {"lid": lender.id, "action": SEAT_CAP_COUNTED_ACTION, "month_start": month_start},
    ).one()
    return int(row.cnt)


def seat_at_cap(lender: LenderRow, db: Session) -> bool:
    """True when the primary lender has reached its monthly cap (if one is set)."""
    if lender.monthly_cap is None:
        return False
    count = seat_count(lender, db)
    at_cap = count >= lender.monthly_cap
    if at_cap:
        logger.info(
            "seat_at_cap lender_id=%d name=%r cap=%d count=%d",
            lender.id, lender.name, lender.monthly_cap, count,
        )
    return at_cap
