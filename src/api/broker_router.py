"""Broker self-service API — Layer 3C/3E.

Routes:
  POST /api/broker/login                        — broker authentication
  POST /api/broker/refresh                      — rotate access token via refresh_token
  GET  /api/broker/me                           — current broker profile
  POST /api/broker/forgot-password              — trigger password reset email
  POST /api/broker/reset-password               — apply new password from token
  GET  /api/broker/states                       — work-state machine config (transitions, reason codes)
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
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.broker_states import (
    ALLOWED_TRANSITIONS,
    InvalidBrokerReasonCode,
    InvalidBrokerState,
    REASON_CODES_BY_STATE,
    WORK_STATE_ORDER,
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
            from config.settings import get_settings as _get_settings
            _s = _get_settings()
            reset_url = f"{_s.app_base_url}/broker/reset-password/{reset_token}"
            login_url = f"{_s.app_base_url}/broker/login"
            send_email(
                to=row.email,
                subject="Reset your broker portal password",
                body_text=(
                    f"Hi {row.name},\n\n"
                    f"Click the link below to reset your password (valid 24 hours):\n\n"
                    f"  {reset_url}\n\n"
                    f"If you did not request this, you can safely ignore this email."
                ),
                body_html=f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">
        <tr>
          <td style="padding:28px 40px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
              <span style="margin-left:8px;font-size:13px;font-weight:600;color:#94a3b8;">Loan Lane</span>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:24px;font-weight:800;color:#ffffff;">
              Password reset request
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Hi {row.name}, we received a request to reset your broker portal password.
              Click the button below — this link is valid for <strong style="color:#ffffff;">24 hours</strong>.
            </p>
            <table cellpadding="0" cellspacing="0" style="margin-bottom:20px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{reset_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;font-weight:700;text-decoration:none;">
                    Reset My Password &rarr;
                  </a>
                </td>
              </tr>
            </table>
            <p style="margin:0 0 16px;font-size:13px;color:#64748b;">
              After resetting, log in at
              <a href="{login_url}" style="color:#fbbf24;text-decoration:none;">{login_url}</a>
            </p>
            <p style="margin:0;font-size:13px;color:#64748b;">
              Didn&rsquo;t request this? You can safely ignore this email — your password will not change.
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Loan Lane Broker Portal
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>""",
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


# ── State machine config ──────────────────────────────────────────────────────

@router.get("/states")
def get_broker_states(broker: dict = Depends(get_current_broker)):
    """Return the broker work-state machine config for UI consumption."""
    return {
        "allowed_transitions": {k: list(v) for k, v in ALLOWED_TRANSITIONS.items()},
        "work_states": list(WORK_STATE_ORDER),
        "reason_codes_by_state": {k: list(v) for k, v in REASON_CODES_BY_STATE.items()},
    }


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
    intent_tier: Optional[str] = Query(default=None),
    work_state: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    lender_id: Optional[str] = Query(default=None),
    entered_from: Optional[str] = Query(default=None),
    entered_to: Optional[str] = Query(default=None),
    activity_from: Optional[str] = Query(default=None),
    activity_to: Optional[str] = Query(default=None),
    sort_by: str = Query(default="last_activity"),
    sort_dir: str = Query(default="desc"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """Broker's own lanes with filtering and pagination."""
    broker_id = broker["broker_id"]
    lanes, total = fetch_lanes(
        db,
        broker_id=broker_id,
        assigned_only=True,
        open_only=False,
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

    return {"broker_id": broker_id, "lanes": result, "total": total, "limit": limit, "offset": offset}
