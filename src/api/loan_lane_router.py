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
  POST /api/admin/lanes/{lane_id}/fee-config    — admin: flip the RESPA fee gate (fee_config_flag)
  POST /api/admin/lanes/seed-pool               — admin: seed pool from top financing_intent_scores
  GET  /api/admin/financing-intent/export       — admin: lender-pitch CSV export (no lane required)
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
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
    open_only: bool = Query(default=True),
    intent_tier: Optional[str] = Query(default=None),
    work_state: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    lender_id: Optional[str] = Query(default=None),
    broker_id: Optional[str] = Query(default=None),
    assigned: Optional[bool] = Query(default=None),
    stale: Optional[bool] = Query(default=None),
    contact_filter: Optional[str] = Query(default=None),
    entered_from: Optional[str] = Query(default=None),
    entered_to: Optional[str] = Query(default=None),
    activity_from: Optional[str] = Query(default=None),
    activity_to: Optional[str] = Query(default=None),
    sort_by: str = Query(default="entered_at"),
    sort_dir: str = Query(default="desc"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Admin — all lanes with filtering and pagination."""
    lanes, total = fetch_lanes(
        db,
        open_only=open_only,
        broker_id=broker_id,
        assigned_only=(assigned is True),
        unassigned_only=(assigned is False),
        stale_only=(stale is True),
        contact_filter=contact_filter,
        intent_tier=intent_tier,
        work_state=work_state,
        stage=stage,
        county=county,
        lender_id=lender_id,
        entered_from=entered_from,
        entered_to=entered_to,
        activity_from=activity_from,
        activity_to=activity_to,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )
    return {"lanes": lanes, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Broker: GET /api/lanes/pool
# Note: /pool must be registered before /{lane_id} so FastAPI doesn't swallow it.
# ---------------------------------------------------------------------------

@router.get("/api/lanes/pool")
def get_pool(
    intent_tier: Optional[str] = Query(default=None),
    work_state: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    entered_from: Optional[str] = Query(default=None),
    entered_to: Optional[str] = Query(default=None),
    sort_by: str = Query(default="intent_score"),
    sort_dir: str = Query(default="desc"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    broker: dict = Depends(get_current_broker),
):
    """Unclaimed prospect pool — contact fields redacted, sorted by intent score."""
    lanes, total = fetch_lanes(
        db,
        unassigned_only=True,
        open_only=True,
        redact_contact=True,
        has_contact=True,
        exclude_guess_leads=True,
        intent_tier=intent_tier,
        stage=stage,
        county=county,
        entered_from=entered_from,
        entered_to=entered_to,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )
    return {"lanes": lanes, "total": total, "limit": limit, "offset": offset}


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
    admin_secret = settings.admin_jwt_secret.get_secret_value() if settings.admin_jwt_secret else ""
    broker_secret = settings.broker_jwt_secret.get_secret_value() if settings.broker_jwt_secret else ""

    is_admin = False
    broker_id: str | None = None

    try:
        payload = jwt.decode(token, broker_secret, algorithms=["HS256"])
        if payload.get("type") == "broker_access":
            broker_id = payload.get("sub")
        else:
            raise JWTError("not a broker token")
    except JWTError:
        try:
            payload = jwt.decode(token, admin_secret, algorithms=["HS256"])
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
    """Initial broker assignment — routes through the state machine so broker_transitions
    and audit events are written consistently with the broker self-claim flow.
    409 if lane is already assigned or lane/broker is invalid.
    """
    from src.services.broker_state_machine import (
        assign_broker,
        BrokerNotFound,
        BrokerInactive,
    )
    try:
        claimed = assign_broker(db, lane_id, body.broker_id, actor="admin")
    except BrokerNotFound:
        raise HTTPException(status_code=404, detail="Broker not found.")
    except BrokerInactive:
        raise HTTPException(status_code=409, detail="Broker account is inactive.")

    if not claimed:
        raise HTTPException(status_code=409, detail="Lane is already assigned or not available.")

    return {"lane_id": lane_id, "assigned_broker_id": body.broker_id}


# ---------------------------------------------------------------------------
# Admin: POST /api/admin/lanes/{lane_id}/fee-config (RESPA fee gate)
# ---------------------------------------------------------------------------

class _FeeConfigRequest(BaseModel):
    enabled: bool
    acknowledge_respa: bool = False


@router.post("/api/admin/lanes/{lane_id}/fee-config")
def set_fee_config(
    lane_id: str,
    body: _FeeConfigRequest,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    """Flip the lane's RESPA fee gate — surfaces/hides commission dollar amounts.

    Enabling requires `acknowledge_respa: true` (422 otherwise): fees must not
    be surfaced without written counsel sign-off that the deal is RESPA-exempt.
    Per-lane only — there is intentionally no bulk/global enable. Every flip is
    WARNING-logged with the acting admin.
    """
    from src.services.loan_lane_service import RESPA_FEE_GATE_WARNING, set_fee_config_flag

    if body.enabled and not body.acknowledge_respa:
        raise HTTPException(status_code=422, detail=RESPA_FEE_GATE_WARNING)

    actor = f"admin:{admin.get('sub', 'unknown')}"
    try:
        result = set_fee_config_flag(db, lane_id, body.enabled, actor=actor)
    except ValueError:
        raise HTTPException(status_code=404, detail="Lane not found.")
    db.commit()

    return {**result, "respa_warning": RESPA_FEE_GATE_WARNING}


# ---------------------------------------------------------------------------
# Admin: POST /api/admin/lanes/seed-pool
# Seed the broker pool from top financing_intent_scores properties.
# Idempotent — skips properties that already have a lane.
# ---------------------------------------------------------------------------

@router.post("/api/admin/lanes/seed-pool")
def seed_pool_from_financing_intent(
    limit: int = Query(default=500, le=2000),
    lane_type: str = Query(default="distressed-payoff"),
    dry_run: bool = Query(default=False),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Seed the broker pool from top-scored financing intent properties.

    Pulls the top `limit` properties by latest financing_intent_score that
    do not yet have a lane, then calls enter_lane() for each.
    Safe to re-run — already-entered properties are counted as skipped.
    """
    from src.services.loan_lane_service import enter_lane

    candidates = db.execute(
        sa_text("""
            WITH latest AS (
                SELECT DISTINCT ON (property_id)
                    property_id, financing_intent_score, intent_tier
                FROM financing_intent_scores
                ORDER BY property_id, score_date DESC
            )
            SELECT l.property_id, l.financing_intent_score, l.intent_tier
            FROM latest l
            WHERE NOT EXISTS (
                SELECT 1 FROM lanes ln
                WHERE ln.property_id = l.property_id
                  AND ln.lane_type = :lt
            )
            ORDER BY l.financing_intent_score DESC
            LIMIT :lim
        """),
        {"lt": lane_type, "lim": limit},
    ).fetchall()

    if dry_run:
        return {
            "dry_run": True,
            "would_create": len(candidates),
            "lane_type": lane_type,
        }

    created = 0
    errors = 0
    for row in candidates:
        try:
            enter_lane(db, lane_type=lane_type, property_id=row.property_id)
            created += 1
        except Exception:
            errors += 1

    db.commit()

    return {
        "created": created,
        "errors": errors,
        "lane_type": lane_type,
    }


# ---------------------------------------------------------------------------
# Admin: GET /api/admin/financing-intent/export
# Lender-pitch CSV export — the financing_intent_scores population as a flat
# list, not routed through a lane at all (client item 69). Same latest-per-
# property query as seed-pool above, plus the owner/address join lane_query.py
# already uses so no new join pattern is introduced.
# ---------------------------------------------------------------------------

@router.get("/api/admin/financing-intent/export")
def export_financing_intent(
    intent_tier: Optional[str] = Query(default=None),
    min_score: Optional[float] = Query(default=None),
    county: Optional[str] = Query(default=None),
    contact_only: bool = Query(default=False, description="Only rows with a phone or email on file"),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Lender-pitch export — top financing-intent-scored properties with owner contact.

    Does not touch lanes/leads — this is a standalone list for pitching a
    lender partner, independent of whether a property has ever entered the
    loan-lane pipeline.

    Restricted to the two real counties: a handful of pytest fixture rows
    (county_id='test_<hash>') persist in financing_intent_scores from tests
    whose commits escaped a savepoint, and they score high enough to sort
    to the top of an unfiltered export.
    """
    rows = db.execute(
        sa_text("""
            WITH latest AS (
                SELECT DISTINCT ON (property_id)
                    property_id, financing_intent_score, intent_tier, score_date
                FROM financing_intent_scores
                ORDER BY property_id, score_date DESC
            )
            SELECT
                l.property_id,
                pr.address, pr.city, pr.state, pr.zip, pr.county_id AS county,
                o.owner_name, o.phone_1 AS phone, o.email_1 AS email,
                l.financing_intent_score, l.intent_tier, l.score_date
            FROM latest l
            JOIN properties pr ON pr.id = l.property_id
            LEFT JOIN owners o ON o.property_id = l.property_id
            WHERE pr.county_id IN ('hillsborough', 'pinellas')
              AND (:intent_tier IS NULL OR l.intent_tier = :intent_tier)
              AND (:min_score IS NULL OR l.financing_intent_score >= :min_score)
              AND (:county IS NULL OR pr.county_id = :county)
              AND (:contact_only = false OR o.phone_1 IS NOT NULL OR o.email_1 IS NOT NULL)
            ORDER BY l.financing_intent_score DESC
            LIMIT :limit
        """),
        {
            "intent_tier": intent_tier,
            "min_score": min_score,
            "county": county,
            "contact_only": contact_only,
            "limit": limit,
        },
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([f"# Financing Intent Export — {datetime.now().strftime('%Y-%m-%d')} — {len(rows)} row(s)"])
    writer.writerow([
        "Property ID", "Address", "City", "State", "ZIP", "County",
        "Owner Name", "Phone", "Email", "Financing Intent Score", "Intent Tier", "Score Date",
    ])
    for r in rows:
        writer.writerow([
            r.property_id, r.address, r.city, r.state, r.zip, r.county,
            r.owner_name, r.phone, r.email, r.financing_intent_score, r.intent_tier, r.score_date,
        ])

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="financing_intent_export.csv"'},
    )
