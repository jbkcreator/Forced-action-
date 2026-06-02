"""
Subscriber feed auth API (fa061).

Endpoints (prefix /api/subscriber):
  POST /login            — dual mode: {email,password} OR {feed_uuid,password} → JWT
  POST /forgot-password  — {email} → emails a reset link (always 200, no enumeration)
  POST /reset-password   — {token,new_password} → sets a new password

The returned JWT gates GET /api/feed/{uuid} (+ /stats) via
subscriber_auth.get_current_subscriber.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select

from src.core.database import get_db_context
from src.core.models import Subscriber
from src.services import subscriber_auth as auth
from src.services.rate_limit import enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/subscriber", tags=["subscriber"])


def get_db():
    with get_db_context() as db:
        yield db


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
