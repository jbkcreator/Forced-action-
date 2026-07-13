"""Revenue Metrics Router — M11 / B3 (§8B) + Task 6.1 subscriber margin.

Read-only revenue + unit-economics reporting surface. JWT-protected via
`get_current_admin`. Mounted at /api/revenue/ by main.py.

    GET /api/revenue/metrics?from=&to=
    GET /api/revenue/confirmed-delivery-margin?from=&to=
    GET /api/revenue/zip-territory-margin
    GET /api/revenue/zip-territory-margin/{subscriber_id}/leads

`from`/`to` are ISO dates (or datetimes); when omitted the window defaults to the
last 30 days. Snapshot metrics (current MRR, active accounts, past-due, at-risk MRR)
ignore the window; period metrics are bounded by it.

NB: the path is /api/revenue/metrics, not /api/metrics — the latter collides with
the Prometheus scrape endpoint in metrics_router.py.

confirmed-delivery-margin and zip-territory-margin (Task 6.1) are two
deliberately separate views, not one blended "margin" — see
src/services/revenue_telemetry.py for why: one is backed by real delivery
events (SentLead/PremiumPurchase), the other is a current-ownership snapshot
inferred from ZIP-territory locks. Mixing them would misrepresent an estimate
as a verified figure.
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
from src.services.revenue_metrics import compute_revenue_metrics
from src.services.revenue_telemetry import (
    compute_confirmed_delivery_margin,
    compute_zip_territory_margin,
    list_zip_territory_leads,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/revenue", tags=["revenue-metrics"])


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


@router.get("/confirmed-delivery-margin")
def confirmed_delivery_margin(
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None, alias="to"),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> list[dict]:
    now = datetime.now(timezone.utc)
    to_dt = _parse(to, "to") or now
    frm_dt = _parse(from_, "from") or (to_dt - timedelta(days=30))
    if frm_dt > to_dt:
        raise HTTPException(status_code=400, detail="'from' must be on or before 'to'")
    try:
        return compute_confirmed_delivery_margin(db, frm_dt, to_dt)
    except Exception:
        logger.error("confirmed delivery margin computation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to compute confirmed delivery margin")


@router.get("/zip-territory-margin")
def zip_territory_margin(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> list[dict]:
    try:
        return compute_zip_territory_margin(db)
    except Exception:
        logger.error("zip territory margin computation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to compute zip territory margin")


@router.get("/zip-territory-margin/{subscriber_id}/leads")
def zip_territory_margin_leads(
    subscriber_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> list[dict]:
    try:
        return list_zip_territory_leads(db, subscriber_id)
    except Exception:
        logger.error("zip territory lead attribution computation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to compute zip territory lead attribution")
