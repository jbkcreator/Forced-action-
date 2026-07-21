"""Operator Dashboard Router — Block 8.

Read-only surface over existing metrics services/tables, JWT-protected via
`get_current_admin`. Mounted at /api/admin/operator-dashboard by main.py.

    GET /api/admin/operator-dashboard/summary?from=&to=   — T-B8-01 KPI aggregation
    GET /api/admin/operator-dashboard/action-queue        — T-B8-03 action queue

`from`/`to` are ISO dates (or datetimes); when omitted the window defaults
to the last 30 days, matching /api/revenue/metrics.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db as _get_db
from src.api.deps import parse_iso_date_param as _parse
from src.services.action_queue import build_action_queue
from src.services.operator_dashboard import compute_operator_dashboard

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/operator-dashboard", tags=["admin", "operator-dashboard"])


@router.get("/summary")
def operator_dashboard_summary(
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
        return compute_operator_dashboard(db, frm_dt, to_dt)
    except Exception:
        logger.error("operator dashboard aggregation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to compute operator dashboard")


@router.get("/action-queue")
def get_action_queue(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """Read-time union of pending approvals + source failures across the three
    existing tables. No mutation, no new ledger."""
    try:
        return build_action_queue(db)
    except SQLAlchemyError:
        logger.error("[OperatorDashboard] action-queue read failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to load action queue")
