"""
Subscriber feed authentication (fa061 + magic-link).

Password-login layer for the lead feed. Mirrors src/services/white_label_auth.py:
  - bcrypt password hashing / verification
  - HS256 JWT access token (7-day) for feed sessions
  - random password generation (legacy — no longer emailed; password login
    stays available as a dormant fallback for subscribers who already have one)
  - forgot-password reset tokens (sha256-hashed in DB, raw emailed)
  - magic-link (passwordless) tokens — the primary login path for new
    subscribers; same raw-token/sha256-hash-in-DB pattern as reset tokens
  - FastAPI dependency `get_current_subscriber` that gates the feed endpoints

Secret: `subscriber_jwt_secret`, falling back to `admin_jwt_secret` (dev).

Security note: signup used to email the generated password in plaintext.
That path is retired — signup now issues a magic link instead (see
`issue_magic_link` / `send_magic_link_email`). No code path should generate
or email a password for a new subscriber.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt as _bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import select

from config.settings import get_settings
from src.core.database import get_db
from src.core.models import Subscriber
from src.services.email import send_email

logger = logging.getLogger(__name__)

_bearer = HTTPBearer(auto_error=False)

_ACCESS_EXPIRE_DAYS = 7
_RESET_EXPIRE_HOURS = 2
_MAGIC_LINK_EXPIRE_MINUTES = 15
_ALGORITHM = "HS256"

# Human-friendly alphabet — no ambiguous chars (0/O, 1/l/I).
_PW_ALPHABET = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"


# ── JWT helpers ──────────────────────────────────────────────────────────────

def _subscriber_secret() -> str:
    s = get_settings()
    secret = s.subscriber_jwt_secret or s.admin_jwt_secret
    if not secret:
        raise HTTPException(status_code=503, detail="Subscriber auth not configured")
    return secret.get_secret_value()


def create_access_token(subscriber_id: int, feed_uuid: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=_ACCESS_EXPIRE_DAYS)
    return jwt.encode(
        {"sub": str(subscriber_id), "feed_uuid": feed_uuid, "type": "access", "exp": exp},
        _subscriber_secret(),
        algorithm=_ALGORITHM,
    )


def verify_access_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, _subscriber_secret(), algorithms=[_ALGORITHM])
        if payload.get("type") != "access":
            raise ValueError("not an access token")
        return payload
    except (JWTError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        ) from exc


# ── Password helpers ───────────────────────────────────────────────────────────

def hash_password(plain: str) -> str:
    return _bcrypt.hashpw(plain.encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return _bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def generate_random_password() -> str:
    """CSPRNG human-friendly password, grouped as xxx-xxx-xxx (9 chars)."""
    chars = [secrets.choice(_PW_ALPHABET) for _ in range(9)]
    return f"{''.join(chars[0:3])}-{''.join(chars[3:6])}-{''.join(chars[6:9])}"


def generate_reset_token() -> tuple[str, str]:
    """Return (raw_token, sha256_hex). Store the hash; email the raw token."""
    raw = secrets.token_urlsafe(32)
    return raw, hashlib.sha256(raw.encode()).hexdigest()


def hash_reset_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Magic-link (passwordless) helpers ────────────────────────────────────────

def generate_magic_link_token() -> tuple[str, str]:
    """Return (raw_token, sha256_hex). Store the hash; email the raw token."""
    raw = secrets.token_urlsafe(32)
    return raw, hashlib.sha256(raw.encode()).hexdigest()


def hash_magic_link_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def issue_magic_link(subscriber: Subscriber, db) -> str:
    """Generate a fresh single-use magic-link token for `subscriber`, store its
    hash + expiry on the row, and return the raw token to email.

    Overwrites any previously-issued, unused link (only the newest is valid).
    Does not commit/flush the session's outer transaction — caller controls that,
    but this does call db.flush() so the values are visible within the same
    transaction (mirrors the password-setup call sites this replaces).
    """
    raw, hashed = generate_magic_link_token()
    subscriber.magic_link_hash = hashed
    subscriber.magic_link_expires_at = datetime.now(timezone.utc) + timedelta(
        minutes=_MAGIC_LINK_EXPIRE_MINUTES
    )
    subscriber.magic_link_used_at = None
    db.flush()
    return raw


def magic_link_url(raw_token: str) -> str:
    base = get_settings().app_base_url.rstrip("/")
    return f"{base}/auth/verify?token={raw_token}"


def send_magic_link_email(email: str, name: Optional[str], raw_token: str) -> None:
    verify_url = magic_link_url(raw_token)
    greeting = f"Hi {name}," if name else "Hi,"
    send_email(
        to=email,
        subject="Your Forced Action login link",
        body_text=(
            f"{greeting}\n\nClick below to access your feed — no password needed:\n"
            f"{verify_url}\n\n"
            f"This link expires in {_MAGIC_LINK_EXPIRE_MINUTES} minutes and can only "
            f"be used once. If you didn't request this, ignore this email.\n\n— Forced Action"
        ),
        body_html=(
            f"<p>{greeting}</p>"
            f"<p>Click below to access your feed — no password needed:</p>"
            f"<p><a href='{verify_url}' style='display:inline-block;padding:10px 18px;"
            f"background:#1a1a2e;color:#fff;text-decoration:none;border-radius:6px;'>"
            f"Open my feed</a></p>"
            f"<p style='color:#888;font-size:12px;'>This link expires in "
            f"{_MAGIC_LINK_EXPIRE_MINUTES} minutes and can only be used once. "
            f"If you didn't request this, ignore this email.</p>"
        ),
    )


# ── FastAPI dependency: gate the feed ───────────────────────────────────────────

def get_current_subscriber(
    feed_uuid: str,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
    db=Depends(get_db),
) -> Subscriber:
    """Authorize feed access for `/api/feed/{feed_uuid}` (and `/stats`).

    401 if the bearer token is missing/invalid/expired.
    404 if no subscriber has this feed_uuid.
    403 if the token's subscriber is not the owner of this feed_uuid.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentication required")

    payload = verify_access_token(credentials.credentials)
    token_sub_id = int(payload["sub"])

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if subscriber is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    if subscriber.id != token_sub_id:
        raise HTTPException(status_code=403, detail="Token does not match this feed")

    return subscriber


# ── Email ────────────────────────────────────────────────────────────────────

def send_subscriber_password_reset_email(email: str, name: Optional[str], raw_token: str) -> None:
    base = get_settings().app_base_url.rstrip("/")
    reset_url = f"{base}/reset-password/{raw_token}"
    greeting = f"Hi {name}," if name else "Hi,"
    send_email(
        to=email,
        subject="Reset your Forced Action feed password",
        body_text=(
            f"{greeting}\n\nReset your feed password here:\n{reset_url}\n\n"
            f"This link expires in {_RESET_EXPIRE_HOURS} hours. If you didn't request "
            f"this, ignore this email.\n\n— Forced Action"
        ),
        body_html=(
            f"<p>{greeting}</p>"
            f"<p>Reset your Forced Action feed password:</p>"
            f"<p><a href='{reset_url}' style='display:inline-block;padding:10px 18px;"
            f"background:#1a1a2e;color:#fff;text-decoration:none;border-radius:6px;'>"
            f"Reset password</a></p>"
            f"<p style='color:#888;font-size:12px;'>Link expires in {_RESET_EXPIRE_HOURS} hours. "
            f"If you didn't request this, ignore this email.</p>"
        ),
    )


RESET_EXPIRE_HOURS = _RESET_EXPIRE_HOURS
MAGIC_LINK_EXPIRE_MINUTES = _MAGIC_LINK_EXPIRE_MINUTES
