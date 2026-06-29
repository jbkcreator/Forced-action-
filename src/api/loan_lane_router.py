"""Loan Lane API — shared /api/lanes routes for broker and admin surfaces.

Routes:
  GET  /api/lanes                               — admin: all lanes
  GET  /api/lanes/pool                          — broker: unclaimed pool (contact redacted)
  GET  /api/lanes/{lane_id}                     — broker or admin: single lane detail
  GET  /api/admin/lanes/{lane_id}/transitions   — admin: full transition log
  POST /api/lanes/{lane_id}/advance             — broker: advance lane stage
  PATCH /api/lanes/{lane_id}/lender             — broker: set funding lender
  GET  /api/lane-stage-config                   — broker: stage config for dropdown
  POST /api/admin/lanes/{lane_id}/assign-broker — admin: initial broker assignment (unassigned only)
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.broker_auth import get_current_broker
from src.services.lane_query import fetch_lane, fetch_lanes

logger = logging.getLogger(__name__)

router = APIRouter(tags=["lanes"])

_optional_bearer = HTTPBearer(auto_error=False)


# ---------------------------------------------------------------------------
# Admin: GET /api/lanes
# ---------------------------------------------------------------------------

@router.get("/api/lanes")
def list_all_lanes(
    open_only: bool = True,
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Admin — all lanes."""
    lanes, total = fetch_lanes(db, open_only=open_only, limit=limit, offset=offset)
    return {"lanes": lanes, "total": total}


# ---------------------------------------------------------------------------
# Broker: GET /api/lanes/pool
# Note: /pool must be registered before /{lane_id} so FastAPI doesn't swallow it.
# ---------------------------------------------------------------------------

@router.get("/api/lanes/pool")
def get_pool(
    limit: int = Query(default=50, le=200),
    db: Session = Depends(get_db),
    broker: dict = Depends(get_current_broker),
):
    """Unclaimed prospect pool — contact fields redacted."""
    lanes, total = fetch_lanes(
        db,
        unassigned_only=True,
        open_only=True,
        redact_contact=True,
        limit=limit,
    )
    return {"lanes": lanes, "total": total}


# ---------------------------------------------------------------------------
# Broker + Admin: GET /api/lanes/{lane_id}
# Accepts either a broker JWT (ownership enforced) or admin JWT (no restriction).
# ---------------------------------------------------------------------------

@router.get("/api/lanes/{lane_id}")
def get_lane_detail(
    lane_id: str,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_optional_bearer),
    db: Session = Depends(get_db),
):
    """Single lane detail. Broker callers: ownership enforced. Admin callers: unrestricted."""
    from jose import JWTError, jwt
    from config.settings import get_settings

    if credentials is None:
        raise HTTPException(status_code=401, detail="Authentication required.")

    token = credentials.credentials
    settings = get_settings()
    secret = settings.admin_jwt_secret.get_secret_value() if settings.admin_jwt_secret else ""

    is_admin = False
    broker_id: str | None = None

    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
        if payload.get("type") == "broker_access":
            broker_id = payload.get("sub")
        else:
            is_admin = True
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")

    lane = fetch_lane(db, lane_id)
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found.")

    if not is_admin:
        if lane["assigned_broker_id"] != broker_id:
            raise HTTPException(status_code=403, detail="Not your lane.")

    return lane


# ---------------------------------------------------------------------------
# Admin: GET /api/admin/lanes/{lane_id}/transitions
# ---------------------------------------------------------------------------

@router.get("/api/admin/lanes/{lane_id}/transitions")
def get_lane_transitions_admin(
    lane_id: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Admin — full transition log for any lane."""
    from src.services.broker_state_machine import LaneNotFound, list_transitions
    try:
        items = list_transitions(db, lane_id)
    except LaneNotFound:
        raise HTTPException(status_code=404, detail="Lane not found.")
    return {"lane_id": lane_id, "transitions": items}


# ---------------------------------------------------------------------------
# Broker: POST /api/lanes/{lane_id}/advance
# ---------------------------------------------------------------------------

class _AdvanceRequest(BaseModel):
    to_stage: str


@router.post("/api/lanes/{lane_id}/advance")
def advance_lane_stage(
    lane_id: str,
    body: _AdvanceRequest,
    db: Session = Depends(get_db),
    broker: dict = Depends(get_current_broker),
):
    """Advance the lane's current_stage to the next legal stage."""
    lane = fetch_lane(db, lane_id)
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found.")
    if lane["assigned_broker_id"] != broker["broker_id"]:
        raise HTTPException(status_code=403, detail="Not your lane.")

    from src.services.loan_lane_service import advance_lane
    try:
        advance_lane(db, lane_id, body.to_stage, actor=broker["broker_id"])
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return {"lane_id": lane_id, "current_stage": body.to_stage}


# ---------------------------------------------------------------------------
# Broker: PATCH /api/lanes/{lane_id}/lender
# ---------------------------------------------------------------------------

class _LenderRequest(BaseModel):
    lender_id: str


@router.patch("/api/lanes/{lane_id}/lender")
def set_lender(
    lane_id: str,
    body: _LenderRequest,
    db: Session = Depends(get_db),
    broker: dict = Depends(get_current_broker),
):
    """Set the funding lender on a lane. Fires on dropdown change."""
    lane = fetch_lane(db, lane_id)
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found.")
    if lane["assigned_broker_id"] != broker["broker_id"]:
        raise HTTPException(status_code=403, detail="Not your lane.")

    from src.services.loan_lane_service import set_lane_lender
    try:
        set_lane_lender(db, lane_id, body.lender_id, actor=broker["broker_id"])
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    return {"lane_id": lane_id, "lender_id": body.lender_id}


# ---------------------------------------------------------------------------
# Broker: GET /api/lane-stage-config
# ---------------------------------------------------------------------------

@router.get("/api/lane-stage-config")
def get_lane_stage_config(
    lane_type: str = Query(default="distressed-payoff"),
    db: Session = Depends(get_db),
    broker: dict = Depends(get_current_broker),
):
    """Stage config used to build the advance-control dropdown."""
    rows = db.execute(
        sa_text("""
            SELECT stage_key, display_name, order_index, allowed_next, sms_allowed
            FROM lane_stage_config
            WHERE lane_type = :lt AND is_active = true
            ORDER BY order_index
        """),
        {"lt": lane_type},
    ).fetchall()
    return {
        "stages": [
            {
                "stage_key": r.stage_key,
                "display_name": r.display_name,
                "order_index": r.order_index,
                "allowed_next": r.allowed_next or [],
                "sms_allowed": bool(r.sms_allowed),
            }
            for r in rows
        ]
    }


# ---------------------------------------------------------------------------
# Admin: POST /api/admin/lanes/{lane_id}/assign-broker (initial, unassigned only)
# ---------------------------------------------------------------------------

class _AssignBrokerRequest(BaseModel):
    broker_id: str


@router.post("/api/admin/lanes/{lane_id}/assign-broker")
def assign_broker_initial(
    lane_id: str,
    body: _AssignBrokerRequest,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Initial broker assignment — 409 if lane is already assigned."""
    row = db.execute(
        sa_text("SELECT assigned_broker_id FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
        {"lid": lane_id},
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Lane not found.")
    if row.assigned_broker_id is not None:
        raise HTTPException(status_code=409, detail="Lane is already assigned to a broker.")

    from src.services.loan_lane_service import claim_lane
    claimed = claim_lane(db, lane_id, body.broker_id)
    if not claimed:
        raise HTTPException(status_code=409, detail="Lane could not be assigned.")

    return {"lane_id": lane_id, "assigned_broker_id": body.broker_id}
