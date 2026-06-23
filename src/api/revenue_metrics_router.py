"""Revenue Metrics Router — M11 / B3 (§8B).

Read-only revenue + unit-economics reporting surface. JWT-protected via
`get_current_admin`. Mounted at /api/revenue/ by main.py.

    GET /api/revenue/metrics?from=&to=

`from`/`to` are ISO dates (or datetimes); when omitted the window defaults to the
last 30 days. Snapshot metrics (current MRR, active accounts, past-due, at-risk MRR)
ignore the window; period metrics are bounded by it.

NB: the path is /api/revenue/metrics, not /api/metrics — the latter collides with
the Prometheus scrape endpoint in metrics_router.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db as _get_db
from src.services.revenue_metrics import compute_revenue_metrics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/revenue", tags=["revenue-metrics"])


def _parse(value: Optional[str], field: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field} date: {value!r}")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@router.get("/metrics")
def revenue_metrics(
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
        return compute_revenue_metrics(db, frm_dt, to_dt)
    except Exception:
        logger.error("revenue metrics computation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to compute revenue metrics")
