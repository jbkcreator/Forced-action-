"""Admin Lead-Delivery Visibility Router — B4 (§9 RBAC: admins see all).

JWT-protected (admin). Lists delivery records across every account for the admin
panel; customers see only their own leads through their own surface. Mounted at
/api/admin/ by main.py.

    GET /api/admin/deliveries?account_id=&grade=&status=&property_id=&from=&to=&limit=&offset=

Returns the stable contract documented in src/services/admin_leads.py (the admin
frontend depends on this shape):
    { "total", "limit", "offset", "items": [ {delivery_id, property_id, account_id,
      company_name, grade, vertical, status, rejection_reason, rejected_at,
      billing_period_end, delivered_at, source}, ... ] }
"""

from __future__ import annotations

import logging
import uuid as _uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db as _get_db
from src.services.admin_leads import list_deliveries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin-leads"])

_VALID_STATUS = ("delivered", "rejected")


def _parse_dt(value: Optional[str], field: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field} date: {value!r}")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@router.get("/deliveries")
def admin_list_deliveries(
    account_id: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    property_id: Optional[int] = Query(None),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = Query(None, alias="to"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict:
    if status is not None and status not in _VALID_STATUS:
        raise HTTPException(status_code=400, detail="status must be 'delivered' or 'rejected'")
    if account_id is not None:
        try:
            _uuid.UUID(account_id)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid account_id: {account_id!r}")
    frm_dt = _parse_dt(from_, "from")
    to_dt = _parse_dt(to, "to")
    if frm_dt and to_dt and frm_dt > to_dt:
        raise HTTPException(status_code=400, detail="'from' must be on or before 'to'")

    try:
        return list_deliveries(
            db, account_id=account_id, grade=grade, status=status,
            property_id=property_id, frm=frm_dt, to=to_dt, limit=limit, offset=offset,
        )
    except Exception:
        logger.error("admin deliveries listing failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list deliveries")
