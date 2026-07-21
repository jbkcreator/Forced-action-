"""Operator Dashboard Router — Block 8 (T-B8-01).

Read-only aggregation surface over existing metrics services/tables. JWT-
protected via `get_current_admin`. Mounted at /api/admin/operator-dashboard
by main.py.

    GET /api/admin/operator-dashboard/summary?from=&to=

`from`/`to` are ISO dates (or datetimes); when omitted the window defaults
to the last 30 days, matching /api/revenue/metrics.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db as _get_db
from src.api.deps import parse_iso_date_param as _parse
from src.services.operator_dashboard import compute_operator_dashboard

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/operator-dashboard", tags=["operator-dashboard"])


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
