"""Broker self-service API — Layer 3C/3E.

Routes:
  POST /api/broker/login                        — broker authentication
  POST /api/broker/refresh                      — rotate access token via refresh_token
  GET  /api/broker/me                           — current broker profile
  POST /api/broker/forgot-password              — trigger password reset email
  POST /api/broker/reset-password               — apply new password from token
  GET  /api/broker/lanes                        — list broker's open lanes (LaneObject + BSM extensions)
  POST /api/broker/lanes/{lane_id}/claim        — self-claim an open lane
  POST /api/broker/lanes/{lane_id}/transition   — advance broker work-state
  GET  /api/broker/lanes/{lane_id}/transitions  — read own transition history
  GET  /api/broker/lenders                      — cleared+active lenders for dropdown
"""
from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.broker_states import (
    InvalidBrokerReasonCode,
    InvalidBrokerState,
    get_allowed_next_states,
)
from src.api.deps import get_db
from src.services.broker_auth import (
    create_broker_token,
    create_refresh_token,
    get_current_broker,
    verify_refresh_token,
)
from src.services.broker_state_machine import (
    BrokerInactive,
    BrokerNotFound,
    ClosedWonPayloadRequired,
    IllegalTransition,
    InvalidGrossAmount,
    LaneNotFound,
    LaneNotOpen,
    LaneOwnershipError,
    assign_broker,
    current_state,
    list_transitions,
    sms_eligible,
    transition,
)
from src.services.lane_query import fetch_lane, fetch_lanes

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/broker", tags=["broker"])


# ── Error mapper ──────────────────────────────────────────────────────────────

def _map_error(exc: Exception) -> None:
    if isinstance(exc, (LaneNotFound, BrokerNotFound)):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, (BrokerInactive, LaneOwnershipError)):
        raise HTTPException(status_code=403, detail=str(exc))
    if isinstance(exc, LaneNotOpen):
        raise HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, (ClosedWonPayloadRequired, InvalidGrossAmount,
                        InvalidBrokerState, InvalidBrokerReasonCode)):
        raise HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, IllegalTransition):
        raise HTTPException(status_code=409, detail=str(exc))
    raise exc


# ── Auth ──────────────────────────────────────────────────────────────────────

class _LoginRequest(BaseModel):
    email: str
    password: str


@router.post("/login")
def broker_login(body: _LoginRequest, db: Session = Depends(get_db)):
    """Verify broker credentials and issue access + refresh tokens."""
    row = db.execute(
        sa_text(
            "SELECT broker_id, email, name, role, password_hash, is_active "
            "FROM brokers WHERE email = :email"
        ),
        {"email": body.email.strip().lower()},
    ).fetchone()

    if row is None or not row.password_hash:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not bcrypt.checkpw(body.password.encode(), row.password_hash.encode()):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not row.is_active:
        raise HTTPException(status_code=403, detail="Broker account is inactive")

    broker_id = str(row.broker_id)
    access_token = create_broker_token(broker_id, row.email)
    refresh_token = create_refresh_token(broker_id, row.email)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "broker": {
            "id": broker_id,
            "brokerId": broker_id,
            "name": row.name,
            "email": row.email,
            "role": row.role or "broker",
        },
    }


class _RefreshRequest(BaseModel):
    refresh_token: str


@router.post("/refresh")
def broker_refresh(body: _RefreshRequest, db: Session = Depends(get_db)):
    """Rotate the access token using a valid refresh token."""
    payload = verify_refresh_token(body.refresh_token)
    broker_id = payload.get("sub")

    row = db.execute(
        sa_text(
            "SELECT broker_id, email, name, role, is_active "
            "FROM brokers WHERE broker_id = CAST(:bid AS uuid)"
        ),
        {"bid": broker_id},
    ).fetchone()

    if row is None or not row.is_active:
        raise HTTPException(status_code=401, detail="Broker account not found or inactive")

    access_token = create_broker_token(str(row.broker_id), row.email)
    new_refresh = create_refresh_token(str(row.broker_id), row.email)

    return {
        "access_token": access_token,
        "refresh_token": new_refresh,
        "broker": {
            "id": str(row.broker_id),
            "brokerId": str(row.broker_id),
            "name": row.name,
            "email": row.email,
            "role": row.role or "broker",
        },
    }


@router.get("/me")
def get_me(broker: dict = Depends(get_current_broker), db: Session = Depends(get_db)):
    """Return full profile for the authenticated broker."""
    row = db.execute(
        sa_text(
            "SELECT broker_id, email, name, role, is_active, created_at "
            "FROM brokers WHERE broker_id = CAST(:bid AS uuid)"
        ),
        {"bid": broker["broker_id"]},
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Broker not found")
    return {
        "broker_id": str(row.broker_id),
        "email": row.email,
        "name": row.name,
        "role": row.role or "broker",
        "is_active": row.is_active,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


class _ForgotPasswordRequest(BaseModel):
    email: str


@router.post("/forgot-password")
def forgot_password(body: _ForgotPasswordRequest, db: Session = Depends(get_db)):
    """Generate a password reset token and send a reset email."""
    row = db.execute(
        sa_text("SELECT broker_id, email, name FROM brokers WHERE email = :email"),
        {"email": body.email.strip().lower()},
    ).fetchone()

    if row is not None:
        reset_token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
        db.execute(
            sa_text(
                "UPDATE brokers SET reset_token = :tok, reset_token_expires_at = :exp "
                "WHERE broker_id = CAST(:bid AS uuid)"
            ),
            {"tok": reset_token, "exp": expires_at, "bid": str(row.broker_id)},
        )
        db.commit()
        try:
            from src.services.email import send_email
            send_email(
                to=row.email,
                subject="Reset your broker portal password",
                body_text=(
                    f"Hi {row.name},\n\n"
                    f"Use this token to reset your password (valid 24 hours):\n\n"
                    f"  {reset_token}\n\n"
                    "If you did not request this, you can safely ignore this email."
                ),
            )
        except Exception:
            logger.warning("[broker] mailchimp reset email failed for %s", row.email)

    return {"detail": "If that email is registered, a reset link has been sent."}


class _ResetPasswordRequest(BaseModel):
    reset_token: str
    new_password: str


@router.post("/reset-password")
def reset_password(body: _ResetPasswordRequest, db: Session = Depends(get_db)):
    """Apply a new password using the emailed reset token."""
    row = db.execute(
        sa_text(
            "SELECT broker_id, reset_token_expires_at "
            "FROM brokers WHERE reset_token = :tok"
        ),
        {"tok": body.reset_token},
    ).fetchone()

    if row is None:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token.")

    if row.reset_token_expires_at is None or row.reset_token_expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Invalid or expired reset token.")

    pw_hash = bcrypt.hashpw(body.new_password.encode(), bcrypt.gensalt()).decode()
    db.execute(
        sa_text(
            "UPDATE brokers SET password_hash = :ph, reset_token = NULL, "
            "reset_token_expires_at = NULL, updated_at = NOW() "
            "WHERE broker_id = CAST(:bid AS uuid)"
        ),
        {"ph": pw_hash, "bid": str(row.broker_id)},
    )
    db.commit()
    return {"detail": "Password updated successfully."}


# ── Lenders ───────────────────────────────────────────────────────────────────

@router.get("/lenders")
def list_lenders(
    broker: dict = Depends(get_current_broker),
    db: Session = Depends(get_db),
):
    """Cleared and active lenders for the funding-lender dropdown."""
    rows = db.execute(
        sa_text(
            "SELECT lender_id, name FROM lenders "
            "WHERE is_cleared = true AND is_active = true ORDER BY name"
        )
    ).fetchall()
    return {"lenders": [{"lender_id": str(r.lender_id), "name": r.name} for r in rows]}


# ── Claim ─────────────────────────────────────────────────────────────────────

@router.post("/lanes/{lane_id}/claim")
def claim_lane(
    lane_id: str,
    broker: dict = Depends(get_current_broker),
    db: Session = Depends(get_db),
):
    """Atomic broker self-claim on an open, unclaimed lane."""
    broker_id = broker["broker_id"]
    try:
        claimed = assign_broker(db, lane_id, broker_id)
    except Exception as exc:
        _map_error(exc)

    if not claimed:
        raise HTTPException(status_code=409, detail="Lane already claimed or not open.")

    lane = fetch_lane(db, lane_id)
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found after claim.")
    return lane


# ── Transition ────────────────────────────────────────────────────────────────

class TransitionRequest(BaseModel):
    to_state: str
    reason_code: str
    gross_amount_cents: Optional[int] = None
    split_config_id: Optional[str] = None


@router.post("/lanes/{lane_id}/transition")
def do_transition(
    lane_id: str,
    body: TransitionRequest,
    broker: dict = Depends(get_current_broker),
    db: Session = Depends(get_db),
):
    """Advance the broker work-state on a lane the caller owns."""
    broker_id = broker["broker_id"]
    try:
        from_state = current_state(db, lane_id)
        tid = transition(
            db,
            lane_id,
            body.to_state,
            broker_id,
            body.reason_code,
            gross_amount_cents=body.gross_amount_cents,
            split_config_id=body.split_config_id,
        )
    except Exception as exc:
        _map_error(exc)

    occurred_row = db.execute(
        sa_text(
            "SELECT occurred_at FROM broker_transitions "
            "WHERE transition_id = CAST(:tid AS uuid)"
        ),
        {"tid": tid},
    ).fetchone()
    _oa = getattr(occurred_row, "occurred_at", None) if occurred_row else None
    occurred_at = _oa.isoformat() if _oa else None

    return {
        "transition_id": tid,
        "lane_id": lane_id,
        "broker_id": broker_id,
        "from_state": from_state,
        "to_state": body.to_state,
        "reason_code": body.reason_code,
        "occurred_at": occurred_at,
        "allowed_next_states": list(get_allowed_next_states(body.to_state)),
    }


# ── Transition history ────────────────────────────────────────────────────────

@router.get("/lanes/{lane_id}/transitions")
def get_transition_history(
    lane_id: str,
    broker: dict = Depends(get_current_broker),
    db: Session = Depends(get_db),
):
    """Return the full transition history for a lane the caller owns."""
    broker_id = broker["broker_id"]

    lane = db.execute(
        sa_text(
            "SELECT assigned_broker_id FROM lanes "
            "WHERE lane_id = CAST(:lid AS uuid)"
        ),
        {"lid": lane_id},
    ).fetchone()

    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found.")

    if str(lane.assigned_broker_id) != broker_id:
        raise HTTPException(status_code=403, detail="Broker does not own this lane.")

    try:
        state = current_state(db, lane_id)
    except LaneNotFound:
        raise HTTPException(status_code=404, detail="Lane not found.")

    return {
        "lane_id": lane_id,
        "current_state": state,
        "transitions": list_transitions(db, lane_id),
    }


# ── Lane list ─────────────────────────────────────────────────────────────────

@router.get("/lanes")
def list_broker_lanes(
    broker: dict = Depends(get_current_broker),
    db: Session = Depends(get_db),
):
    """Open lanes assigned to the calling broker — full LaneObject shape + BSM extensions."""
    broker_id = broker["broker_id"]
    lanes, total = fetch_lanes(db, broker_id=broker_id, assigned_only=True, open_only=True)

    result = []
    for lane in lanes:
        lane_id_str = lane["lane_id"]
        try:
            state = current_state(db, lane_id_str)
            sms_ok = sms_eligible(db, lane_id_str)
        except LaneNotFound:
            state = "unassigned"
            sms_ok = False
        result.append({
            **lane,
            "broker_state": state,
            "allowed_next_states": list(get_allowed_next_states(state)),
            "sms_eligible": sms_ok,
        })

    return {"broker_id": broker_id, "lanes": result, "total": total}
