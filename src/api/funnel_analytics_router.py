"""Funnel Analytics Router — Part 3 traffic-capture stage counts.

Read-only stage-count reporting surface. JWT-protected via `get_current_admin`.
Mounted at /api/analytics/ by main.py.

    GET /api/analytics/funnel?from=&to=

`from`/`to` are ISO dates (or datetimes); when omitted the window defaults to
the last 30 days.

This is intentionally separate from revenue_metrics_router.py — that surface
reports dollars/margin per account, this one reports how many people reached
each funnel stage (visits, sample views, checkout started, paid, rebilled).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db as _get_db
from src.services.funnel_analytics import compute_funnel_counts

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics", tags=["funnel-analytics"])


def _parse(value: Optional[str], field: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field} date: {value!r}")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@router.get("/funnel")
def funnel(
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None, alias="to"),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict:
    now = datetime.now(timezone.utc)
    to_dt = _parse(to, "to") or now
    frm_dt = _parse(from_, "from") or (to_dt - timedelta(days=30))
    if frm_dt > to_dt:
        raise HTTPException(status_code=400, detail="'from' must be on or before 'to'")
    try:
        return compute_funnel_counts(db, frm_dt, to_dt)
    except Exception:
        logger.error("funnel counts computation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to compute funnel counts")
