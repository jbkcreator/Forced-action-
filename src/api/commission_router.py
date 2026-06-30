"""Commission Ledger API — broker and admin surfaces.

Routes:
  GET  /api/commissions                    — RBAC: broker sees own, admin sees all
  POST /api/commissions/{entry_id}/dispute — flag entry as disputed
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.api.deps import get_db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["commissions"])

_optional_bearer = HTTPBearer(auto_error=False)

_COMMISSION_SELECT = """
    SELECT
        cl.entry_id,
        cl.lane_id,
        cl.broker_id,
        b.name              AS broker_name,
        cl.gross_amount_cents,
        cl.net_lines,
        cl.status,
        cl.trigger_transition_id,
        cl.posted_at,
        l.property_id,
        pr.address          AS property_address,
        pr.county_id        AS property_county
    FROM commission_ledger cl
    LEFT JOIN brokers b     ON b.broker_id = cl.broker_id
    LEFT JOIN lanes l       ON l.lane_id = cl.lane_id
    LEFT JOIN properties pr ON pr.id = l.property_id
"""


def _serialize_entry(row) -> dict:
    return {
        "entry_id": str(row.entry_id),
        "lane_id": str(row.lane_id) if row.lane_id else None,
        "broker_id": str(row.broker_id) if row.broker_id else None,
        "broker_name": row.broker_name,
        "gross_amount_cents": int(row.gross_amount_cents) if row.gross_amount_cents is not None else None,
        "net_lines": row.net_lines,
        "status": row.status,
        "trigger_transition_id": str(row.trigger_transition_id) if row.trigger_transition_id else None,
        "posted_at": row.posted_at.isoformat() if row.posted_at else None,
        "property": {
            "property_id": str(row.property_id) if row.property_id else None,
            "address": row.property_address,
            "county": row.property_county,
        },
    }


def _resolve_auth(credentials: Optional[HTTPAuthorizationCredentials]) -> tuple[bool, str | None]:
    """Decode the bearer token; return (is_admin, broker_id)."""
    from jose import JWTError, jwt
    from config.settings import get_settings

    if credentials is None:
        raise HTTPException(status_code=401, detail="Authentication required.")

    token = credentials.credentials
    settings = get_settings()
    secret = settings.admin_jwt_secret.get_secret_value() if settings.admin_jwt_secret else ""

    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")

    if payload.get("type") == "broker_access":
        return False, payload.get("sub")
    return True, None


@router.get("/api/commissions")
def list_commissions(
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_optional_bearer),
    db: Session = Depends(get_db),
):
    """RBAC commission list. Broker sees only their own entries; admin sees all."""
    is_admin, broker_id = _resolve_auth(credentials)

    where_parts: list[str] = []
    params: dict = {"limit": limit, "offset": offset}

    if not is_admin:
        where_parts.append("cl.broker_id = CAST(:broker_id AS uuid)")
        params["broker_id"] = broker_id

    if status:
        where_parts.append("cl.status = :status")
        params["status"] = status

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    rows = db.execute(
        sa_text(
            _COMMISSION_SELECT
            + where_sql
            + " ORDER BY cl.posted_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    filter_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
    total = db.execute(
        sa_text("SELECT COUNT(*) FROM commission_ledger cl " + where_sql),
        filter_params,
    ).scalar() or 0

    return {"commissions": [_serialize_entry(r) for r in rows], "total": int(total)}


@router.post("/api/commissions/{entry_id}/dispute")
def dispute_commission_entry(
    entry_id: str,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_optional_bearer),
    db: Session = Depends(get_db),
):
    """Flag a commission entry as disputed. Broker may only dispute their own entries."""
    is_admin, broker_id = _resolve_auth(credentials)
    actor = "admin" if is_admin else f"broker:{broker_id}"

    row = db.execute(
        sa_text(
            "SELECT entry_id, broker_id, status "
            "FROM commission_ledger WHERE entry_id = CAST(:eid AS uuid)"
        ),
        {"eid": entry_id},
    ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Commission entry not found.")

    if not is_admin and str(row.broker_id) != broker_id:
        raise HTTPException(status_code=403, detail="Not your commission entry.")

    if row.status != "posted":
        raise HTTPException(status_code=409, detail=f"Entry is already {row.status}.")

    from src.services.commission_ledger import dispute_entry
    dispute_entry(db, entry_id, actor=actor)

    return {"entry_id": entry_id, "status": "disputed"}
