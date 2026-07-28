"""
Subscriber feed auth API (fa061 + magic-link).

Endpoints (prefix /api/subscriber):
  POST /login                 — dual mode: {email,password} OR {feed_uuid,password} → JWT
                                 (dormant fallback — kept for subscribers who already
                                 have a password; no new password is ever issued)
  POST /forgot-password       — {email} → emails a reset link (always 200, no enumeration)
  POST /reset-password        — {token,new_password} → sets a new password
  POST /magic-link/request    — {email} → emails a one-time login link (always
                                 200, no enumeration)
  POST /magic-link/verify     — {token} → JWT (same shape as /login)

The returned JWT gates GET /api/feed/{uuid} (+ /stats) via
subscriber_auth.get_current_subscriber.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select, text

from src.api.deps import get_db
from src.core.models import Subscriber
from src.services import subscriber_auth as auth
from src.services.rate_limit import enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/subscriber", tags=["subscriber"])


# ── request models ──────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: Optional[str] = None
    feed_uuid: Optional[str] = None
    password: str

    @model_validator(mode="after")
    def _exactly_one_identifier(self):
        if bool(self.email) == bool(self.feed_uuid):
            raise ValueError("provide exactly one of email or feed_uuid")
        return self


class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str = Field(min_length=8)


class MagicLinkRequest(BaseModel):
    email: str


class MagicLinkVerifyRequest(BaseModel):
    token: str


# Onboarding preference step — kept in sync with Forced-action-ui's
# OnboardingStep.jsx select options.
PROPERTY_TYPE_OPTIONS = frozenset({"single_family", "multi_family", "commercial", "land", "other"})
BUDGET_BAND_OPTIONS = frozenset({"under_50k", "50k_150k", "150k_500k", "500k_plus"})


class OnboardingRequest(BaseModel):
    preferred_property_type: str
    investment_budget_band: str

    @model_validator(mode="after")
    def _valid_options(self):
        if self.preferred_property_type not in PROPERTY_TYPE_OPTIONS:
            raise ValueError(f"preferred_property_type must be one of {sorted(PROPERTY_TYPE_OPTIONS)}")
        if self.investment_budget_band not in BUDGET_BAND_OPTIONS:
            raise ValueError(f"investment_budget_band must be one of {sorted(BUDGET_BAND_OPTIONS)}")
        return self


# ── endpoints ──────────────────────────────────────────────────────────────

@router.post("/login")
def login(body: LoginRequest, request: Request, db=Depends(get_db)):
    enforce_or_429(request, scope="subscriber_login", limit=10, window_seconds=60)

    if body.email:
        sub = db.execute(
            select(Subscriber).where(Subscriber.email == body.email.strip().lower())
        ).scalar_one_or_none()
    else:
        sub = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == body.feed_uuid)
        ).scalar_one_or_none()

    # Uniform failure — never reveal whether the account/password exists.
    if sub is None or not sub.password_hash or not auth.verify_password(body.password, sub.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = auth.create_access_token(sub.id, sub.event_feed_uuid)
    return {"access_token": token, "token_type": "bearer", "feed_uuid": sub.event_feed_uuid}


@router.post("/forgot-password")
def forgot_password(body: ForgotPasswordRequest, request: Request, db=Depends(get_db)):
    enforce_or_429(request, scope="subscriber_forgot", limit=5, window_seconds=300)

    sub = db.execute(
        select(Subscriber).where(Subscriber.email == body.email.strip().lower())
    ).scalar_one_or_none()

    # Always 200 — no account enumeration.
    if sub is not None and sub.email:
        raw, hashed = auth.generate_reset_token()
        sub.reset_token_hash = hashed
        sub.reset_token_expires_at = datetime.now(timezone.utc) + timedelta(hours=auth.RESET_EXPIRE_HOURS)
        db.flush()
        try:
            auth.send_subscriber_password_reset_email(sub.email, sub.name, raw)
        except Exception:
            logger.warning("[subscriber-auth] reset email send failed for sub=%s", sub.id, exc_info=True)

    return {"ok": True}


@router.post("/reset-password")
def reset_password(body: ResetPasswordRequest, request: Request, db=Depends(get_db)):
    enforce_or_429(request, scope="subscriber_reset", limit=10, window_seconds=300)

    hashed = auth.hash_reset_token(body.token)
    now = datetime.now(timezone.utc)
    sub = db.execute(
        select(Subscriber).where(Subscriber.reset_token_hash == hashed)
    ).scalar_one_or_none()

    if sub is None or sub.reset_token_expires_at is None:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    expires = sub.reset_token_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    if expires < now:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    sub.password_hash = auth.hash_password(body.new_password)
    sub.password_set_at = now
    sub.reset_token_hash = None
    sub.reset_token_expires_at = None
    db.flush()

    logger.info("[subscriber-auth] password reset for sub=%s", sub.id)
    return {"ok": True}


@router.post("/magic-link/request")
def request_magic_link(body: MagicLinkRequest, request: Request, db=Depends(get_db)):
    enforce_or_429(request, scope="subscriber_magic_request", limit=5, window_seconds=300)

    sub = db.execute(
        select(Subscriber).where(Subscriber.email == body.email.strip().lower())
    ).scalar_one_or_none()

    # Always 200 — no account enumeration.
    if sub is not None and sub.email:
        try:
            raw = auth.issue_magic_link(sub, db)
            auth.send_magic_link_email(sub.email, sub.name, raw)
        except Exception:
            logger.warning("[subscriber-auth] magic-link email send failed for sub=%s", sub.id, exc_info=True)

    return {"ok": True}


@router.post("/magic-link/verify")
def verify_magic_link(body: MagicLinkVerifyRequest, request: Request, db=Depends(get_db)):
    enforce_or_429(request, scope="subscriber_magic_verify", limit=10, window_seconds=300)

    hashed = auth.hash_magic_link_token(body.token)
    now = datetime.now(timezone.utc)

    # Atomic conditional UPDATE (not select-then-write) — closes a TOCTOU
    # window where two concurrent requests with the same raw token could
    # both pass a Python-side validity check before either write lands,
    # letting a single-use link redeem twice. The WHERE clause is checked
    # and applied by Postgres in one statement, so only one concurrent
    # caller can ever match the row.
    row = db.execute(
        text(
            """
            UPDATE subscribers
            SET magic_link_used_at = :now, magic_link_hash = NULL
            WHERE magic_link_hash = :hashed
              AND magic_link_used_at IS NULL
              AND magic_link_expires_at > :now
            RETURNING id, event_feed_uuid, vertical
            """
        ),
        {"hashed": hashed, "now": now},
    ).first()
    db.flush()

    if row is None:
        raise HTTPException(status_code=400, detail="Invalid or expired link")

    logger.info("[subscriber-auth] magic-link verified for sub=%s", row.id)
    token = auth.create_access_token(row.id, row.event_feed_uuid)
    return {
        "access_token": token,
        "token_type": "bearer",
        "feed_uuid": row.event_feed_uuid,
        "vertical": row.vertical,
    }


@router.patch("/onboarding/{feed_uuid}")
def submit_onboarding(
    feed_uuid: str,
    body: OnboardingRequest,
    db=Depends(get_db),
    subscriber: Subscriber = Depends(auth.get_current_subscriber),
):
    """One-time onboarding preference capture — gates first login until
    submitted (Subscriber.onboarding_completed, defaults False on new
    email signups). Idempotent: re-submitting just overwrites the answer."""
    subscriber.preferred_property_type = body.preferred_property_type
    subscriber.investment_budget_band = body.investment_budget_band
    subscriber.onboarding_completed = True
    db.flush()

    from src.services.activation_tracking import stamp_onboarding_completed
    stamp_onboarding_completed(subscriber.id, db)

    logger.info("[onboarding] preferences captured for sub=%s", subscriber.id)
    return {"ok": True}
