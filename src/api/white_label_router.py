"""
White-label Tier REST API (Stage 12 / fa056).

Prefix: /api/wl
Authentication:
  - Public endpoints (auth/*) — no token required
  - Dashboard endpoints — Bearer JWT from /auth/login
  - Data endpoints — Bearer JWT OR X-API-Key header

Admin management endpoints (for internal ops) are under /api/admin/white-label
and re-use the existing admin JWT from admin_router.py.
"""

import hashlib
import io
import logging
import re
import secrets
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import stripe
from fastapi import (
    APIRouter, Body, Depends, File, Header, HTTPException,
    Response, UploadFile, status,
)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services import clay_service
from src.services.white_label_api_key import (
    generate_api_key,
    get_api_key_client,
    revoke_api_key,
    validate_api_key,
)
from src.services.white_label_auth import (
    create_access_token,
    create_refresh_token,
    generate_verification_token,
    get_current_wl_user,
    hash_password,
    require_admin_role,
    send_activation_email,
    send_admin_approval_notification,
    send_invite_email,
    send_password_reset_email,
    send_verification_email,
    verify_access_token,
    verify_password,
)

# Optional bearer scheme — does NOT raise when the Authorization header is
# absent, so data endpoints can fall back to X-API-Key auth.
_optional_bearer = HTTPBearer(auto_error=False)
from src.services.white_label_billing import (
    create_billing_portal_session,
    create_wl_checkout,
    handle_wl_webhook_event,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/wl", tags=["white-label"])
admin_wl_router = APIRouter(prefix="/api/admin/white-label", tags=["admin-white-label"])

_RESET_EXPIRE_HOURS = 2
_VERIFY_EXPIRE_HOURS = 24


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s_-]+", "-", name)
    return re.sub(r"^-+|-+$", "", name)


def _unique_slug(base: str, db) -> str:
    slug = base
    attempt = 0
    while True:
        existing = db.execute(
            sa_text("SELECT 1 FROM white_label_clients WHERE company_slug = :slug"),
            {"slug": slug},
        ).fetchone()
        if not existing:
            return slug
        attempt += 1
        slug = f"{base}-{attempt}"


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class SignupRequest(BaseModel):
    company_name: str
    admin_name: str
    admin_email: EmailStr
    password: str
    plan_tier: str = "standard"
    primary_color: Optional[str] = None
    secondary_color: Optional[str] = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


class UpdateAccountRequest(BaseModel):
    display_name: Optional[str] = None
    primary_color: Optional[str] = None
    secondary_color: Optional[str] = None
    counties_enabled: Optional[list] = None
    verticals_enabled: Optional[list] = None


class InviteTeamMemberRequest(BaseModel):
    email: EmailStr
    name: str
    role: str = "member"


class UpdateTeamMemberRequest(BaseModel):
    role: str


class CreateApiKeyRequest(BaseModel):
    label: str = "Default"


class CheckoutRequest(BaseModel):
    plan_tier: str = "standard"
    include_trial: bool = True


class DealRequest(BaseModel):
    county_id: str
    trade_vertical: str
    deal_size_bucket: str
    deal_amount: Optional[float] = None
    pipeline_stage: str = "closed_won"
    property_id: Optional[int] = None


# ---------------------------------------------------------------------------
# Auth endpoints (public)
# ---------------------------------------------------------------------------

@router.post("/auth/signup", status_code=201)
def signup(req: SignupRequest, db: Session = Depends(get_db)):
    """Create a new white-label company account."""
    if len(req.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    if req.plan_tier not in ("standard", "premium"):
        raise HTTPException(400, "plan_tier must be standard or premium")

    # Validate optional brand colors (hex). Ignore silently if malformed so a
    # bad color never blocks signup — they can fix it later in Settings.
    hex_re = re.compile(r"^#[0-9a-fA-F]{6}$")
    primary = req.primary_color if (req.primary_color and hex_re.match(req.primary_color)) else None
    secondary = req.secondary_color if (req.secondary_color and hex_re.match(req.secondary_color)) else None

    # Prevent duplicate email for the admin user
    existing_user = db.execute(
        sa_text("SELECT 1 FROM white_label_users WHERE email = :email"),
        {"email": req.admin_email},
    ).fetchone()
    if existing_user:
        raise HTTPException(409, "An account with this email already exists")

    base_slug = _slugify(req.company_name) or "company"
    slug = _unique_slug(base_slug, db)

    client_row = db.execute(
        sa_text("""
            INSERT INTO white_label_clients
                   (company_name, company_slug, admin_email, admin_name, status,
                    intended_plan_tier, primary_color, secondary_color, created_at, updated_at)
            VALUES (:name, :slug, :email, :admin_name, 'pending_verification',
                    :intended_plan, :primary, :secondary, now(), now())
            RETURNING id
        """),
        {"name": req.company_name, "slug": slug, "email": req.admin_email,
         "admin_name": req.admin_name, "intended_plan": req.plan_tier,
         "primary": primary, "secondary": secondary},
    ).fetchone()
    client_id = client_row.id

    verify_raw, verify_hash = generate_verification_token()

    db.execute(
        sa_text("""
            INSERT INTO white_label_users
                   (client_id, email, name, role, password_hash, is_active,
                    reset_token, reset_token_expires_at, created_at)
            VALUES (:cid, :email, :name, 'admin', :pw_hash, false,
                    :token, :exp, now())
        """),
        {
            "cid": client_id,
            "email": req.admin_email,
            "name": req.admin_name,
            "pw_hash": hash_password(req.password),
            "token": verify_hash,
            "exp": datetime.now(timezone.utc) + timedelta(hours=_VERIFY_EXPIRE_HOURS),
        },
    )
    db.commit()

    send_verification_email(req.admin_email, req.admin_name, req.company_name, verify_raw)
    send_admin_approval_notification(client_id, req.company_name, req.admin_email)

    return {"message": "Account created. Check your email to verify your address.", "client_id": client_id}


@router.get("/auth/verify-email/{token}")
def verify_email(token: str, db: Session = Depends(get_db)):
    """
    Verify email via token sent during signup.

    Idempotent: matching the token for an already-verified user returns success
    rather than an error, so a double-click, refresh, or React StrictMode
    double-invoke doesn't surface a false "verification failed" state.
    """
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    user = db.execute(
        sa_text("""
            SELECT id, client_id, is_active, email_verified_at, reset_token_expires_at
              FROM white_label_users
             WHERE reset_token = :hash
        """),
        {"hash": token_hash},
    ).fetchone()

    if not user:
        raise HTTPException(400, "Invalid or already-used verification link")

    # Already verified (e.g. second click / refresh) → idempotent success.
    if user.is_active and user.email_verified_at:
        return {"message": "Email already verified — your account is active. You can now log in."}

    if user.reset_token_expires_at and user.reset_token_expires_at < datetime.now(timezone.utc):
        raise HTTPException(400, "Verification link has expired")

    # Mark verified. Keep the token until it expires (don't NULL it) so repeat
    # clicks within the window stay idempotent; reset_token_expires_at still bounds it.
    db.execute(
        sa_text("""
            UPDATE white_label_users
               SET is_active = true, email_verified_at = now()
             WHERE id = :uid
        """),
        {"uid": user.id},
    )

    # Self-serve activation: verifying the (first) admin user's email activates
    # the company account so they can log in and start their trial. Ops can
    # later suspend via the admin endpoint if needed.
    client = db.execute(
        sa_text("SELECT id, status, admin_email, company_name FROM white_label_clients WHERE id = :cid"),
        {"cid": user.client_id},
    ).fetchone()
    newly_activated = False
    if client and client.status == "pending_verification":
        db.execute(
            sa_text("""
                UPDATE white_label_clients
                   SET status = 'active', verified_at = now(), activated_at = now(), updated_at = now()
                 WHERE id = :cid
            """),
            {"cid": user.client_id},
        )
        newly_activated = True

    db.commit()

    if newly_activated and client:
        try:
            send_activation_email(client.admin_email, client.company_name)
        except Exception as exc:
            logger.warning("[wl] activation email failed: %s", exc)

    return {"message": "Email verified — your account is active. You can now log in."}


@router.post("/auth/login")
def login(req: LoginRequest, db: Session = Depends(get_db)):
    """Exchange email + password for JWT access and refresh tokens."""
    user = db.execute(
        sa_text("""
            SELECT u.id, u.client_id, u.email, u.name, u.role,
                   u.password_hash, u.is_active, u.email_verified_at,
                   c.status AS client_status, c.company_name, c.plan_tier
              FROM white_label_users u
              JOIN white_label_clients c ON c.id = u.client_id
             WHERE u.email = :email
        """),
        {"email": req.email},
    ).fetchone()

    if not user or not verify_password(req.password, user.password_hash or ""):
        raise HTTPException(401, "Invalid email or password")
    if not user.is_active:
        raise HTTPException(403, "Email not yet verified")
    if user.client_status == "pending_verification":
        raise HTTPException(403, "Account pending admin verification")
    if user.client_status == "suspended":
        raise HTTPException(403, "Account has been suspended")

    db.execute(
        sa_text("UPDATE white_label_users SET last_login_at = now() WHERE id = :uid"),
        {"uid": user.id},
    )
    db.commit()

    access_token = create_access_token(user.id, user.client_id)
    refresh_raw, _ = create_refresh_token()

    return {
        "access_token": access_token,
        "refresh_token": refresh_raw,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "role": user.role,
        },
        "client": {
            "id": user.client_id,
            "company_name": user.company_name,
            "plan_tier": user.plan_tier,
            "status": user.client_status,
        },
    }


@router.post("/auth/forgot-password")
def forgot_password(req: ForgotPasswordRequest, db: Session = Depends(get_db)):
    """Send a password reset email."""
    user = db.execute(
        sa_text("SELECT id, name FROM white_label_users WHERE email = :email AND is_active = true"),
        {"email": req.email},
    ).fetchone()
    if user:
        reset_raw, reset_hash = generate_verification_token()
        db.execute(
            sa_text("""
                UPDATE white_label_users
                   SET reset_token = :hash,
                       reset_token_expires_at = :exp
                 WHERE id = :uid
            """),
            {
                "hash": reset_hash,
                "exp": datetime.now(timezone.utc) + timedelta(hours=_RESET_EXPIRE_HOURS),
                "uid": user.id,
            },
        )
        db.commit()
        send_password_reset_email(req.email, user.name, reset_raw)
    # Always return 200 to avoid email enumeration
    return {"message": "If that email exists, a reset link has been sent."}


@router.post("/auth/reset-password")
def reset_password(req: ResetPasswordRequest, db: Session = Depends(get_db)):
    """Reset password via token received by email."""
    if len(req.new_password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")

    token_hash = hashlib.sha256(req.token.encode()).hexdigest()
    user = db.execute(
        sa_text("""
            SELECT id, reset_token_expires_at FROM white_label_users
             WHERE reset_token = :hash
        """),
        {"hash": token_hash},
    ).fetchone()

    if not user:
        raise HTTPException(400, "Invalid or already-used reset token")
    if user.reset_token_expires_at and user.reset_token_expires_at < datetime.now(timezone.utc):
        raise HTTPException(400, "Reset token has expired")

    db.execute(
        sa_text("""
            UPDATE white_label_users
               SET password_hash = :pw, reset_token = NULL,
                   reset_token_expires_at = NULL, is_active = true
             WHERE id = :uid
        """),
        {"pw": hash_password(req.new_password), "uid": user.id},
    )
    db.commit()
    return {"message": "Password updated. You can now log in."}


# ---------------------------------------------------------------------------
# Account endpoints (JWT required, admin role)
# ---------------------------------------------------------------------------

@router.get("/account")
def get_account(user=Depends(get_current_wl_user), db: Session = Depends(get_db)):
    row = db.execute(
        sa_text("""
            SELECT id, company_name, company_slug, display_name, admin_email, admin_name,
                   status, plan_tier, intended_plan_tier, plan_price_cents, trial_ends_at,
                   logo_url, primary_color, secondary_color,
                   counties_enabled, verticals_enabled, api_enabled,
                   api_requests_per_day, verified_at, activated_at, created_at
              FROM white_label_clients WHERE id = :cid
        """),
        {"cid": user.client_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, "Account not found")
    return dict(row._mapping)


@router.patch("/account")
def update_account(req: UpdateAccountRequest, user=Depends(require_admin_role), db: Session = Depends(get_db)):
    updates = {k: v for k, v in req.dict().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")

    # Validate hex color format
    for color_field in ("primary_color", "secondary_color"):
        if color_field in updates:
            if not re.match(r"^#[0-9a-fA-F]{6}$", updates[color_field]):
                raise HTTPException(400, f"{color_field} must be a valid 6-digit hex color (e.g. #fbbf24)")

    set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
    updates["cid"] = user.client_id
    db.execute(
        sa_text(f"UPDATE white_label_clients SET {set_clauses}, updated_at = now() WHERE id = :cid"),
        updates,
    )
    db.commit()
    return {"message": "Account updated"}


@router.post("/account/logo")
def upload_logo(
    file: UploadFile = File(...),
    user=Depends(require_admin_role),
    db: Session = Depends(get_db),
):
    s = get_settings()
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(400, "Only image files are allowed")

    slug = db.execute(
        sa_text("SELECT company_slug FROM white_label_clients WHERE id = :cid"),
        {"cid": user.client_id},
    ).scalar()

    ext = Path(file.filename or "logo.png").suffix or ".png"
    logo_dir = Path(s.wl_logo_upload_dir)
    logo_dir.mkdir(parents=True, exist_ok=True)
    dest = logo_dir / f"{slug}{ext}"

    contents = file.file.read()
    if len(contents) > 5 * 1024 * 1024:
        raise HTTPException(400, "Logo must be under 5 MB")

    dest.write_bytes(contents)
    logo_url = f"/static/white_label_logos/{slug}{ext}"

    db.execute(
        sa_text("UPDATE white_label_clients SET logo_url = :url, updated_at = now() WHERE id = :cid"),
        {"url": logo_url, "cid": user.client_id},
    )
    db.commit()
    return {"logo_url": logo_url}


# ---------------------------------------------------------------------------
# Team management (admin role)
# ---------------------------------------------------------------------------

@router.get("/team")
def list_team(user=Depends(require_admin_role), db: Session = Depends(get_db)):
    rows = db.execute(
        sa_text("""
            SELECT id, email, name, role, is_active, email_verified_at,
                   last_login_at, created_at
              FROM white_label_users
             WHERE client_id = :cid
             ORDER BY created_at
        """),
        {"cid": user.client_id},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/team/invite", status_code=201)
def invite_team_member(req: InviteTeamMemberRequest, user=Depends(require_admin_role), db: Session = Depends(get_db)):
    if req.role not in ("admin", "member"):
        raise HTTPException(400, "role must be admin or member")

    existing = db.execute(
        sa_text("SELECT 1 FROM white_label_users WHERE email = :email"),
        {"email": req.email},
    ).fetchone()
    if existing:
        raise HTTPException(409, "A user with this email already exists")

    # Set-password token (same mechanism as password reset)
    set_pw_raw, set_pw_hash = generate_verification_token()
    db.execute(
        sa_text("""
            INSERT INTO white_label_users
                   (client_id, email, name, role, is_active,
                    reset_token, reset_token_expires_at, invited_by_id, created_at)
            VALUES (:cid, :email, :name, :role, false,
                    :token, :exp, :invited_by, now())
        """),
        {
            "cid": user.client_id,
            "email": req.email,
            "name": req.name,
            "role": req.role,
            "token": set_pw_hash,
            "exp": datetime.now(timezone.utc) + timedelta(hours=48),
            "invited_by": user.id,
        },
    )
    db.commit()

    company_name = db.execute(
        sa_text("SELECT company_name FROM white_label_clients WHERE id = :cid"),
        {"cid": user.client_id},
    ).scalar()
    send_invite_email(req.email, req.name, company_name, set_pw_raw)
    return {"message": f"Invite sent to {req.email}"}


@router.patch("/team/{member_id}")
def update_team_member(member_id: int, req: UpdateTeamMemberRequest,
                       user=Depends(require_admin_role), db: Session = Depends(get_db)):
    if req.role not in ("admin", "member"):
        raise HTTPException(400, "role must be admin or member")
    result = db.execute(
        sa_text("""
            UPDATE white_label_users SET role = :role
             WHERE id = :mid AND client_id = :cid
        """),
        {"role": req.role, "mid": member_id, "cid": user.client_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(404, "Team member not found")
    return {"message": "Role updated"}


@router.delete("/team/{member_id}", status_code=204)
def remove_team_member(member_id: int, user=Depends(require_admin_role), db: Session = Depends(get_db)):
    if member_id == user.id:
        raise HTTPException(400, "Cannot deactivate your own account")
    result = db.execute(
        sa_text("""
            UPDATE white_label_users SET is_active = false
             WHERE id = :mid AND client_id = :cid
        """),
        {"mid": member_id, "cid": user.client_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(404, "Team member not found")


# ---------------------------------------------------------------------------
# Billing (admin role)
# ---------------------------------------------------------------------------

@router.post("/billing/checkout")
def billing_checkout(req: CheckoutRequest, user=Depends(require_admin_role), db: Session = Depends(get_db)):
    client = db.execute(
        sa_text("SELECT id, admin_email, company_name, company_slug, stripe_customer_id, status FROM white_label_clients WHERE id = :cid"),
        {"cid": user.client_id},
    ).fetchone()
    if not client:
        raise HTTPException(404, "Account not found")

    try:
        result = create_wl_checkout(
            client_id=client.id,
            company_slug=client.company_slug,
            admin_email=client.admin_email,
            plan_tier=req.plan_tier,
            include_trial=req.include_trial,
        )
    except ValueError as exc:
        # Price ID not configured in .env (STRIPE_PRICE_WL_* or STRIPE_TEST_PRICE_WL_*)
        raise HTTPException(
            status_code=503,
            detail=f"Stripe price not configured for plan '{req.plan_tier}': {exc}. "
                   f"Set STRIPE_PRICE_WL_{req.plan_tier.upper()} (or the TEST variant) in .env.",
        )
    return result


@router.get("/billing/status")
def billing_status(user=Depends(get_current_wl_user), db: Session = Depends(get_db)):
    row = db.execute(
        sa_text("""
            SELECT status, plan_tier, intended_plan_tier, plan_price_cents, trial_ends_at,
                   stripe_subscription_id, stripe_customer_id, activated_at
              FROM white_label_clients WHERE id = :cid
        """),
        {"cid": user.client_id},
    ).fetchone()
    if not row:
        raise HTTPException(404)
    data = dict(row._mapping)
    # Expose only a boolean — never leak the raw Stripe customer ID to the client.
    data["has_stripe_customer"] = bool(data.pop("stripe_customer_id", None))
    return data


@router.post("/billing/portal")
def billing_portal(user=Depends(require_admin_role), db: Session = Depends(get_db)):
    stripe_customer_id = db.execute(
        sa_text("SELECT stripe_customer_id FROM white_label_clients WHERE id = :cid"),
        {"cid": user.client_id},
    ).scalar()
    if not stripe_customer_id:
        # Manually-activated / demo clients never went through Stripe checkout,
        # so there is no customer to manage. Return a clear, non-500 message.
        raise HTTPException(
            status_code=409,
            detail="No Stripe billing profile yet — subscribe through checkout first.",
        )
    try:
        url = create_billing_portal_session(stripe_customer_id)
    except stripe.error.InvalidRequestError as exc:
        # Most common cause in test mode: Billing Portal not configured in the
        # Stripe dashboard (Settings → Billing → Customer portal).
        logger.warning("[wl] billing portal failed: %s", exc)
        raise HTTPException(
            status_code=409,
            detail="Billing portal is not available yet. Configure the Stripe Customer Portal in the dashboard.",
        )
    return {"url": url}


# ---------------------------------------------------------------------------
# API Keys (admin role for management; any active user for usage stats)
# ---------------------------------------------------------------------------

@router.get("/api-keys")
def list_api_keys(user=Depends(require_admin_role), db: Session = Depends(get_db)):
    rows = db.execute(
        sa_text("""
            SELECT id, key_prefix, label, is_active,
                   requests_today, total_requests, last_used_at, created_at, revoked_at
              FROM white_label_api_keys
             WHERE client_id = :cid
             ORDER BY created_at DESC
        """),
        {"cid": user.client_id},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/api-keys", status_code=201)
def create_api_key(req: CreateApiKeyRequest, user=Depends(require_admin_role), db: Session = Depends(get_db)):
    raw_key, key_row = generate_api_key(user.client_id, req.label, user.id, db)
    return {
        "key": raw_key,
        "message": "Save this key — it will not be shown again.",
        **key_row,
    }


@router.delete("/api-keys/{key_id}", status_code=204)
def delete_api_key(key_id: int, user=Depends(require_admin_role), db: Session = Depends(get_db)):
    revoke_api_key(key_id, user.client_id, db)


@router.get("/api-keys/usage")
def api_key_usage(user=Depends(get_current_wl_user), db: Session = Depends(get_db)):
    rows = db.execute(
        sa_text("""
            SELECT id, key_prefix, label, requests_today, total_requests, last_used_at
              FROM white_label_api_keys
             WHERE client_id = :cid AND is_active = true
             ORDER BY total_requests DESC
        """),
        {"cid": user.client_id},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Data endpoints — JWT Bearer OR X-API-Key
# ---------------------------------------------------------------------------

def _require_subscription(client: dict) -> None:
    """Raise 402 if the client has no active subscription or live trial.

    Access is allowed when:
      - plan_tier is set (subscription created in Stripe, paid or in trial), OR
      - trial_ends_at is in the future (trial started but not yet lapsed).

    A client whose email is verified but has never subscribed stays locked out
    until they go through Stripe checkout. This prevents free lead scraping.
    """
    from datetime import datetime, timezone
    plan_tier = client.get("plan_tier")
    trial_ends_at = client.get("trial_ends_at")

    has_plan = bool(plan_tier)
    has_live_trial = (
        trial_ends_at is not None
        and datetime.now(timezone.utc) < (
            trial_ends_at if trial_ends_at.tzinfo else trial_ends_at.replace(tzinfo=timezone.utc)
        )
    )

    if not has_plan and not has_live_trial:
        raise HTTPException(
            status_code=402,
            detail="No active subscription. Start your trial from the Billing tab to access leads.",
        )


def _get_data_client(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_optional_bearer),
    db: Session = Depends(get_db),
) -> dict:
    """
    Unified auth for data endpoints: accepts EITHER an X-API-Key header OR a
    Bearer JWT. API key takes precedence. Neither present → 401.

    We resolve the bearer token manually (rather than Depends(get_current_wl_user))
    so that an API-key-only request is not rejected for lacking a JWT.
    """
    if x_api_key:
        client = validate_api_key(x_api_key, db)
        _require_subscription(client)
        return client

    if credentials:
        payload = verify_access_token(credentials.credentials)
        client_id = int(payload["cid"])
        client_row = db.execute(
            sa_text("""
                SELECT id AS client_id, counties_enabled, verticals_enabled,
                       status, plan_tier, trial_ends_at
                  FROM white_label_clients WHERE id = :cid
            """),
            {"cid": client_id},
        ).fetchone()
        if not client_row:
            raise HTTPException(401, "Client not found")
        if client_row.status == "suspended":
            raise HTTPException(403, "Company account suspended")
        client = dict(client_row._mapping)
        _require_subscription(client)
        return client

    raise HTTPException(401, "Authentication required (Bearer JWT or X-API-Key)")


@router.get("/data/leads")
def get_leads(
    county_id: Optional[str] = None,
    vertical: Optional[str] = None,
    min_score: float = 40.0,
    tier: Optional[str] = None,
    page: int = 1,
    limit: int = 25,
    client=Depends(_get_data_client),
    db: Session = Depends(get_db),
):
    """Return paginated Gold+ leads for the WL client's enabled counties/verticals."""
    client_id = client["client_id"]
    counties = client.get("counties_enabled") or []
    verticals = client.get("verticals_enabled") or []

    # Restrict to client's enabled scope
    if county_id and counties and county_id not in counties:
        raise HTTPException(403, f"County {county_id!r} not enabled for this account")
    if vertical and verticals and vertical not in verticals:
        raise HTTPException(403, f"Vertical {vertical!r} not enabled for this account")

    filter_counties = [county_id] if county_id else (counties or None)
    filter_verticals = [vertical] if vertical else (verticals or None)

    params: dict[str, Any] = {
        "min_score": min_score,
        "limit": limit,
        "offset": (page - 1) * limit,
    }
    # NOTE: these clauses are injected into the INNER subquery, where the table
    # is `distress_scores` (unaliased) — do not prefix with `ds.` (that alias
    # only exists on the outer derived table).
    county_clause = ""
    if filter_counties:
        params["counties"] = filter_counties
        county_clause = "AND county_id = ANY(:counties)"

    tier_clause = ""
    if tier:
        params["tier"] = tier
        tier_clause = "AND lead_tier = :tier"

    # Total count for the same filter (drives pagination + header count)
    total = int(db.execute(
        sa_text(f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT ON (property_id) property_id
                  FROM distress_scores
                 WHERE final_cds_score >= :min_score
                   AND lead_tier IN ('Ultra Platinum','Platinum','Gold')
                   {county_clause} {tier_clause}
                 ORDER BY property_id, score_date DESC
            ) counted
        """),
        params,
    ).scalar() or 0)

    rows = db.execute(
        sa_text(f"""
            SELECT p.id, p.address, p.city, p.state, p.zip, p.county_id,
                   ds.final_cds_score AS score, ds.lead_tier AS tier,
                   ds.vertical_scores, ds.score_date,
                   o.owner_name, o.phone_1
              FROM (
                  SELECT DISTINCT ON (property_id) property_id, final_cds_score,
                         lead_tier, vertical_scores, score_date, county_id
                    FROM distress_scores
                   WHERE final_cds_score >= :min_score
                     AND lead_tier IN ('Ultra Platinum','Platinum','Gold')
                     {county_clause} {tier_clause}
                   ORDER BY property_id, score_date DESC
              ) ds
              JOIN properties p ON p.id = ds.property_id
              LEFT JOIN owners o ON o.property_id = p.id
             ORDER BY ds.final_cds_score DESC
             LIMIT :limit OFFSET :offset
        """),
        params,
    ).fetchall()

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "leads": [dict(r._mapping) for r in rows],
        "sandbox_mode": get_settings().stripe_test_mode,
    }


@router.get("/data/leads/{property_id}")
def get_lead_detail(
    property_id: int,
    client=Depends(_get_data_client),
    db: Session = Depends(get_db),
):
    row = db.execute(
        sa_text("""
            SELECT p.*, o.owner_name, o.phone_1, o.email_1, o.absentee_status,
                   f.assessed_value_mkt, f.est_equity, f.equity_pct,
                   ds.final_cds_score, ds.lead_tier, ds.vertical_scores,
                   ds.distress_types, ds.factor_scores
              FROM properties p
              LEFT JOIN owners o ON o.property_id = p.id
              LEFT JOIN financials f ON f.property_id = p.id
              LEFT JOIN LATERAL (
                  SELECT final_cds_score, lead_tier, vertical_scores,
                         distress_types, factor_scores
                    FROM distress_scores
                   WHERE property_id = p.id
                   ORDER BY score_date DESC LIMIT 1
              ) ds ON true
             WHERE p.id = :pid
        """),
        {"pid": property_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, "Lead not found")
    result = dict(row._mapping)
    result["sandbox_mode"] = get_settings().stripe_test_mode
    return result


@router.get("/data/stats")
def get_stats(client=Depends(_get_data_client), db: Session = Depends(get_db)):
    counties = client.get("counties_enabled") or []
    params: dict[str, Any] = {}
    county_clause = ""
    if counties:
        params["counties"] = counties
        county_clause = "WHERE county_id = ANY(:counties)"

    rows = db.execute(
        sa_text(f"""
            SELECT lead_tier, county_id, COUNT(*) AS cnt
              FROM (
                  SELECT DISTINCT ON (property_id) property_id, lead_tier, county_id
                    FROM distress_scores {county_clause}
                   ORDER BY property_id, score_date DESC
              ) latest
             WHERE lead_tier IS NOT NULL
             GROUP BY lead_tier, county_id
             ORDER BY county_id, lead_tier
        """),
        params,
    ).fetchall()
    return {
        "stats": [dict(r._mapping) for r in rows],
        "sandbox_mode": get_settings().stripe_test_mode,
    }


@router.get("/data/contractors")
def get_contractors(
    county_id: str,
    vertical: str,
    force_refresh: bool = False,
    client=Depends(_get_data_client),
    db: Session = Depends(get_db),
):
    """Return Clay-enriched contractor data for a county + vertical."""
    data = clay_service.get_or_refresh_enrichment(
        client_id=client["client_id"],
        county_id=county_id,
        vertical=vertical,
        db=db,
        force_refresh=force_refresh,
    )
    return {
        "county_id": county_id,
        "vertical": vertical,
        "contractors": data,
        "count": len(data),
        "sandbox_mode": get_settings().stripe_test_mode,
    }


@router.post("/data/deals", status_code=201)
def submit_deal(
    req: DealRequest,
    client=Depends(_get_data_client),
    db: Session = Depends(get_db),
):
    """
    Record a deal outcome. Feeds the Stage 10 pricing cohort data gate
    via the existing deal_outcomes table.
    """
    valid_buckets = ("5_10k", "10_25k", "25k_plus", "skip")
    if req.deal_size_bucket not in valid_buckets:
        raise HTTPException(400, f"deal_size_bucket must be one of: {valid_buckets}")

    outcome_id: int = db.execute(
        sa_text("""
            INSERT INTO deal_outcomes
                   (subscriber_id, property_id, deal_size_bucket, deal_amount,
                    pipeline_stage, county_id, trade_vertical, created_at)
            VALUES (NULL, :pid, :bucket, :amount, :stage, :county, :vertical, now())
            RETURNING id
        """),
        {
            "pid": req.property_id,
            "bucket": req.deal_size_bucket,
            "amount": req.deal_amount,
            "stage": req.pipeline_stage,
            "county": req.county_id,
            "vertical": req.trade_vertical,
        },
    ).scalar_one()

    # Phase 3 A1: loss autopsy for closed_lost / declined deals
    if req.pipeline_stage in ("closed_lost", "declined"):
        try:
            from src.services.loss_autopsy import run_loss_autopsy
            reason = "DECLINED" if req.pipeline_stage == "declined" else "CLOSED_LOST"
            run_loss_autopsy(
                property_id=req.property_id,
                trigger_reason=reason,
                db=db,
                deal_outcome_id=outcome_id,
            )
        except Exception as exc:
            logger.warning("[wl_deal] loss autopsy failed property_id=%s: %s", req.property_id, exc)

    db.commit()
    return {"message": "Deal recorded"}


# ---------------------------------------------------------------------------
# Reports (JWT Bearer)
# ---------------------------------------------------------------------------

@router.get("/reports/leads.csv")
def download_leads_csv(
    county_id: Optional[str] = None,
    min_score: float = 40.0,
    user=Depends(get_current_wl_user),
    db: Session = Depends(get_db),
):
    """Download a branded CSV export of the client's leads."""
    import csv

    client = db.execute(
        sa_text("SELECT display_name, company_name, counties_enabled FROM white_label_clients WHERE id = :cid"),
        {"cid": user.client_id},
    ).fetchone()
    company = client.display_name or client.company_name

    counties = client.counties_enabled or []
    params: dict[str, Any] = {"min_score": min_score}
    county_clause = ""
    if county_id:
        params["counties"] = [county_id]
        county_clause = "AND ds.county_id = ANY(:counties)"
    elif counties:
        params["counties"] = counties
        county_clause = "AND ds.county_id = ANY(:counties)"

    rows = db.execute(
        sa_text(f"""
            SELECT p.address, p.city, p.state, p.zip, p.county_id,
                   ds.final_cds_score, ds.lead_tier, ds.score_date,
                   o.owner_name, o.phone_1
              FROM (
                  SELECT DISTINCT ON (property_id)
                         property_id, final_cds_score, lead_tier, score_date, county_id
                    FROM distress_scores
                   WHERE final_cds_score >= :min_score {county_clause}
                   ORDER BY property_id, score_date DESC
              ) ds
              JOIN properties p ON p.id = ds.property_id
              LEFT JOIN owners o ON o.property_id = p.id
             ORDER BY ds.final_cds_score DESC
             LIMIT 5000
        """),
        params,
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([f"# {company} — Lead Export — {datetime.now().strftime('%Y-%m-%d')}"])
    writer.writerow(["Address", "City", "State", "ZIP", "County", "Score", "Tier", "Score Date", "Owner", "Phone"])
    for r in rows:
        writer.writerow([r.address, r.city, r.state, r.zip, r.county_id,
                         r.final_cds_score, r.lead_tier, r.score_date,
                         r.owner_name, r.phone_1])

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{company}_leads.csv"'},
    )


@router.get("/reports/benchmark.pdf")
def download_benchmark_pdf(user=Depends(get_current_wl_user), db: Session = Depends(get_db)):
    """Generate and return a branded benchmark PDF for the client."""
    from src.tasks.daily_dashboard import html_to_pdf, render_html

    client = db.execute(
        sa_text("""
            SELECT company_name, display_name, logo_url,
                   primary_color, secondary_color, counties_enabled
              FROM white_label_clients WHERE id = :cid
        """),
        {"cid": user.client_id},
    ).fetchone()

    wl_branding = {
        "company_name": client.display_name or client.company_name,
        "logo_url": client.logo_url,
        "primary_color": client.primary_color or "#fbbf24",
        "secondary_color": client.secondary_color or "#a855f7",
    }

    # Use the contractor_benchmark template with branding context
    from jinja2 import Environment, FileSystemLoader
    from pathlib import Path
    env = Environment(loader=FileSystemLoader("src/templates"), autoescape=True)
    tmpl = env.get_template("contractor_benchmark.html")
    html = tmpl.render(
        run_date=str(datetime.now().date()),
        wl_branding=wl_branding,
        company_name=wl_branding["company_name"],
        counties=client.counties_enabled or [],
    )

    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        html_to_pdf(html, Path(tmp.name))
        pdf_bytes = Path(tmp.name).read_bytes()

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="benchmark.pdf"'},
    )


# ---------------------------------------------------------------------------
# Admin management endpoints (uses existing admin JWT)
# ---------------------------------------------------------------------------

@admin_wl_router.get("/clients")
def admin_list_clients(
    _admin=Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        sa_text("""
            SELECT id, company_name, company_slug, admin_email, status,
                   plan_tier, plan_price_cents, created_at, verified_at, activated_at
              FROM white_label_clients ORDER BY created_at DESC
        """),
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@admin_wl_router.post("/clients/{client_id}/verify")
def admin_verify_client(
    client_id: int,
    _admin=Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    row = db.execute(
        sa_text("SELECT id, admin_email, company_name, status FROM white_label_clients WHERE id = :cid"),
        {"cid": client_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, "Client not found")
    if row.status == "active":
        return {"message": "Already active"}

    db.execute(
        sa_text("""
            UPDATE white_label_clients
               SET status = 'active', verified_at = now(), activated_at = now(), updated_at = now()
             WHERE id = :cid
        """),
        {"cid": client_id},
    )
    db.commit()
    send_activation_email(row.admin_email, row.company_name)
    return {"message": f"Client {client_id} activated"}


@admin_wl_router.post("/clients/{client_id}/suspend")
def admin_suspend_client(
    client_id: int,
    _admin=Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    result = db.execute(
        sa_text("""
            UPDATE white_label_clients
               SET status = 'suspended', updated_at = now()
             WHERE id = :cid
        """),
        {"cid": client_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(404, "Client not found")
    return {"message": f"Client {client_id} suspended"}
