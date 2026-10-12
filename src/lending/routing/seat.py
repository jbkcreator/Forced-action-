"""Seat-cap counter for the primary lender.

OPEN Q1: Josh to confirm whether the cap counts 'funded' or 'routed' actions.
Default is 'funded' (``config.lending_routing.SEAT_CAP_COUNTED_ACTION``).

The count is derived live from ``file_routing_events`` for the current
Eastern calendar month, so no reset job is needed and the count survives
restarts.  The cap check runs under a SELECT FOR UPDATE SKIP LOCKED on the
primary lender's routing row (via the caller's locking in router.py), so
concurrent routing runs never double-count.
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
            "  FROM lending.file_routing_events"
            " WHERE lender_id = :lid"
            "   AND action = :action"
            "   AND created_at >= :month_start"
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
