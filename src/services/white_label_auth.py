"""
White-label tier authentication helpers (Stage 12 / fa056).

Provides:
  - bcrypt password hashing / verification
  - HS256 JWT access tokens (15-min) and refresh tokens (7-day)
  - FastAPI dependency for authenticated WL routes
  - Email helpers: verification, password reset, admin approval notification
"""

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt as _bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import text as sa_text

from config.settings import get_settings
from src.core.database import get_db
from src.services.email import send_email

logger = logging.getLogger(__name__)

_bearer = HTTPBearer(auto_error=False)

_ACCESS_EXPIRE_MINUTES = 15
_REFRESH_EXPIRE_DAYS = 7
_RESET_EXPIRE_HOURS = 2
_ALGORITHM = "HS256"


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def _wl_secret() -> str:
    s = get_settings()
    secret = s.wl_jwt_secret or s.admin_jwt_secret
    if not secret:
        raise HTTPException(status_code=503, detail="WL auth not configured")
    return secret.get_secret_value()


def create_access_token(user_id: int, client_id: int) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=_ACCESS_EXPIRE_MINUTES)
    return jwt.encode(
        {"sub": str(user_id), "cid": client_id, "exp": exp, "type": "access"},
        _wl_secret(),
        algorithm=_ALGORITHM,
    )


def create_refresh_token() -> tuple[str, str]:
    """Return (raw_token, sha256_hex_of_raw_token). Store only the hash."""
    raw = secrets.token_urlsafe(40)
    hashed = hashlib.sha256(raw.encode()).hexdigest()
    return raw, hashed


def verify_access_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, _wl_secret(), algorithms=[_ALGORITHM])
        if payload.get("type") != "access":
            raise ValueError("not an access token")
        return payload
    except (JWTError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        ) from exc


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def hash_password(plain: str) -> str:
    return _bcrypt.hashpw(plain.encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return _bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------

def get_current_wl_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
    db=Depends(get_db),
):
    """
    Dependency that returns the authenticated WhiteLabelUser.
    Raises 401 if token is missing, invalid, or user is inactive.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentication required")

    payload = verify_access_token(credentials.credentials)
    user_id = int(payload["sub"])
    client_id = int(payload["cid"])

    row = db.execute(
        sa_text("""
            SELECT u.id, u.client_id, u.email, u.name, u.role, u.is_active,
                   u.email_verified_at, c.status AS client_status
              FROM white_label_users u
              JOIN white_label_clients c ON c.id = u.client_id
             WHERE u.id = :uid AND u.client_id = :cid
        """),
        {"uid": user_id, "cid": client_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=401, detail="User not found")
    if not row.is_active:
        raise HTTPException(status_code=403, detail="Account deactivated")
    if row.client_status == "suspended":
        raise HTTPException(status_code=403, detail="Company account suspended")

    return row


def require_admin_role(user=Depends(get_current_wl_user)):
    """Dependency that additionally enforces admin role."""
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return user


# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------

def send_verification_email(user_email: str, user_name: str, company_name: str, token: str) -> None:
    settings = get_settings()
    verify_url = f"{_frontend_base()}/wl/verify-email/{token}"
    send_email(
        to=user_email,
        subject=f"Verify your {company_name} account — Forced Action",
        body_text=f"Hi {user_name},\n\nVerify your email:\n{verify_url}\n\nLink expires in 24 hours.",
        body_html=f"""
        <div style="font-family:sans-serif;background:#0f172a;padding:32px;border-radius:8px">
          <h2 style="color:#fbbf24">Welcome to Forced Action White-label</h2>
          <p style="color:#e2e8f0">Hi {user_name}, please verify your email to activate your <b>{company_name}</b> account.</p>
          <a href="{verify_url}" style="display:inline-block;margin-top:16px;padding:10px 24px;background:#fbbf24;color:#0f172a;border-radius:6px;font-weight:700;text-decoration:none;">
            Verify Email
          </a>
          <p style="color:#64748b;font-size:12px;margin-top:24px">Link expires in 24 hours.</p>
        </div>
        """,
    )


def send_password_reset_email(user_email: str, user_name: str, token: str) -> None:
    reset_url = f"{_frontend_base()}/wl/reset-password/{token}"
    send_email(
        to=user_email,
        subject="Reset your Forced Action White-label password",
        body_text=f"Hi {user_name},\n\nReset your password:\n{reset_url}\n\nExpires in {_RESET_EXPIRE_HOURS} hours.",
        body_html=f"""
        <div style="font-family:sans-serif;background:#0f172a;padding:32px;border-radius:8px">
          <h2 style="color:#fbbf24">Password Reset</h2>
          <p style="color:#e2e8f0">Hi {user_name}, click below to reset your password. This link expires in {_RESET_EXPIRE_HOURS} hours.</p>
          <a href="{reset_url}" style="display:inline-block;margin-top:16px;padding:10px 24px;background:#fbbf24;color:#0f172a;border-radius:6px;font-weight:700;text-decoration:none;">
            Reset Password
          </a>
        </div>
        """,
    )


def send_admin_approval_notification(client_id: int, company_name: str, admin_email: str) -> None:
    """
    Notify ops that a new white-label client signed up. Purely informational:
    the client self-activates by verifying their email. Ops can suspend a bad
    actor via POST /api/admin/white-label/clients/{id}/suspend (admin JWT).
    """
    settings = get_settings()
    alert_target = settings.alert_email
    if not alert_target:
        logger.info("[wl_auth] ALERT_EMAIL not set — skipping admin signup notification")
        return
    send_email(
        to=alert_target,
        subject=f"New white-label signup: {company_name}",
        body_text=(
            f"A new white-label client just signed up (self-serve trial):\n"
            f"  Client ID: {client_id}\n"
            f"  Company:   {company_name}\n"
            f"  Admin:     {admin_email}\n\n"
            f"They activate by verifying their email. To suspend, call:\n"
            f"  POST /api/admin/white-label/clients/{client_id}/suspend"
        ),
    )


def send_invite_email(invitee_email: str, invitee_name: str, company_name: str, set_password_token: str) -> None:
    url = f"{_frontend_base()}/wl/reset-password/{set_password_token}"
    send_email(
        to=invitee_email,
        subject=f"You've been invited to {company_name} on Forced Action",
        body_text=f"Hi {invitee_name},\n\nSet your password:\n{url}",
        body_html=f"""
        <div style="font-family:sans-serif;background:#0f172a;padding:32px;border-radius:8px">
          <h2 style="color:#fbbf24">You're invited!</h2>
          <p style="color:#e2e8f0">You've been invited to join <b>{company_name}</b> on Forced Action. Click below to set your password.</p>
          <a href="{url}" style="display:inline-block;margin-top:16px;padding:10px 24px;background:#fbbf24;color:#0f172a;border-radius:6px;font-weight:700;text-decoration:none;">
            Set Password & Join
          </a>
        </div>
        """,
    )


def send_activation_email(admin_email: str, company_name: str) -> None:
    login_url = f"{_frontend_base()}/wl/login"
    send_email(
        to=admin_email,
        subject=f"Your {company_name} account is now active!",
        body_text=f"Your Forced Action white-label account has been verified and activated. Log in at: {login_url}",
        body_html=f"""
        <div style="font-family:sans-serif;background:#0f172a;padding:32px;border-radius:8px">
          <h2 style="color:#fbbf24">Your account is live!</h2>
          <p style="color:#e2e8f0">Your <b>{company_name}</b> account has been verified. You can now subscribe and access your leads dashboard.</p>
          <a href="{login_url}" style="display:inline-block;margin-top:16px;padding:10px 24px;background:#fbbf24;color:#0f172a;border-radius:6px;font-weight:700;text-decoration:none;">
            Log in
          </a>
        </div>
        """,
    )


# ---------------------------------------------------------------------------
# Token for email verification (stored hashed in user row)
# ---------------------------------------------------------------------------

def generate_verification_token() -> tuple[str, str]:
    """Return (raw_token, sha256_hex). Store only the hash; send raw in email."""
    raw = secrets.token_urlsafe(32)
    hashed = hashlib.sha256(raw.encode()).hexdigest()
    return raw, hashed


def _frontend_base() -> str:
    return get_settings().wl_frontend_base_url.rstrip("/")
