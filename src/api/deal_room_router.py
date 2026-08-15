"""
Deal-room router.

GET  /api/deal-room/{token}   — public read endpoint (no auth).
POST /api/hold-checkout        — create $97 hold Stripe checkout session (no auth).
POST /api/admin/deal-room     — admin generator (JWT required).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

import secrets

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.api.admin_router import create_access_token, verify_token
from src.api.deps import VALID_VERTICALS, get_db
from src.services.hold_lifecycle_service import create_deal_room
from src.services.lead_pool_service import get_lead_pool
from src.services.subscriber_auth import verify_password
from src.services import pricing_truth
from src.utils.test_account import is_test_subscriber
from src.utils.county_config import is_county_launched

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Tier price constants (Starter / Pro / Founder — Dominator retired).
# MUST match the live founding monthly subscription prices in Stripe
# (stripe_price_{tier}_founding) so the deal-room quote/ROI shows what the
# prospect is actually charged. Founding monthly amounts as of 2026-08-13.
# ---------------------------------------------------------------------------

_TIER_PRICES: Dict[str, int] = {
    "starter": 299,
    "pro": 499,
    "founder": 1100,
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _hold_state(
    converted_at: Optional[datetime],
    expires_at: Optional[datetime],
    held_at: Optional[datetime],
    now: datetime,
) -> str:
    if converted_at is not None:
        return "converted"
    if expires_at is not None and expires_at < now:
        return "expired"
    if held_at is not None:
        return "held"
    return "available"


def _iso_or_none(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _compute_roi(job_value: Optional[float], angi_cost: int) -> List[Dict[str, Any]]:
    rows = []
    for tier, price in _TIER_PRICES.items():
        if job_value and price > 0:
            multiple = round(float(job_value) / price, 1)
        else:
            multiple = None
        rows.append({
            "tier": tier,
            "price": price,
            "payback_multiple": multiple,
            "angi_comparison": f"vs ~${angi_cost} per shared Angi lead",
        })
    return rows


def _humanize_distress(types: Optional[List[str]]) -> str:
    """Render a distress_types array (e.g. ['divorce_filings']) as a readable label."""
    if not types:
        return ""
    return ", ".join(t.replace("_", " ").title() for t in types if t)


def _distress_type_map(db: Session, property_ids: List[int]) -> Dict[int, str]:
    """Latest distress_types per property, as readable labels — one query."""
    if not property_ids:
        return {}
    rows = db.execute(
        text(
            """
            SELECT DISTINCT ON (property_id) property_id, distress_types
            FROM distress_scores
            WHERE property_id = ANY(:ids)
            ORDER BY property_id, score_date DESC
            """
        ),
        {"ids": property_ids},
    ).fetchall()
    return {row.property_id: _humanize_distress(row.distress_types) for row in rows}


def _shape_properties(lead_pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for lead in lead_pool:
        out.append({
            "street": lead.get("address") or lead.get("street") or "",
            "distress_type": lead.get("distress_type") or "",
            "tier": lead.get("lead_tier") or lead.get("tier") or "",
            "date_flagged": lead.get("date_flagged") or lead.get("scored_at") or "",
        })
    return out


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.get("/api/deal-room/{token}", tags=["deal-room"])
def get_deal_room(token: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Public (no auth) deal-room read endpoint.

    Performs a live ZIP property query on every call. The properties_snapshot
    audit column is never included in the response.
    """
    # Advisory pricing-config check — surfaces broken/unsellable price config
    # for monitoring but never blocks the room. The displayed prices come from
    # Stripe itself (GET /api/pricing), and a genuinely bad price reveals itself
    # at charge time in /api/checkout — refusing to load the room here only
    # created false outages over stale constants (see pricing_truth docstring).
    try:
        pt_result = pricing_truth.check()
        if not pt_result.get("ok"):
            logger.warning(
                "[deal_room] pricing config problems (advisory, not blocking): %s",
                pt_result.get("problems"),
            )
    except Exception:
        logger.warning("[deal_room] pricing_truth.check failed (advisory)", exc_info=True)

    row = db.execute(
        text(
            """
            SELECT id, token, prospect_name, zip_code, tier,
                   job_value, held_at, expires_at, converted_at
            FROM deal_rooms
            WHERE token = :token
            LIMIT 1
            """
        ),
        {"token": token},
    ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Deal room not found.")

    now = datetime.now(timezone.utc)

    settings = get_settings()
    angi_cost = settings.angi_shared_lead_cost

    try:
        lead_pool = get_lead_pool(zip_code=row.zip_code, limit=25)
        distress_by_pid = _distress_type_map(
            db, [lead["property_id"] for lead in lead_pool if lead.get("property_id")]
        )
        for lead in lead_pool:
            lead["distress_type"] = distress_by_pid.get(lead.get("property_id"), "")
    except Exception:
        logger.warning(
            "[deal_room] lead_pool query failed for ZIP %s token %s",
            row.zip_code,
            token,
            exc_info=True,
        )
        lead_pool = []

    return {
        "token": row.token,
        "prospect_name": row.prospect_name,
        "zip": row.zip_code,
        "tier": row.tier,
        "held_at": _iso_or_none(row.held_at),
        "expires_at": _iso_or_none(row.expires_at),
        "hold_state": _hold_state(
            converted_at=row.converted_at,
            expires_at=row.expires_at,
            held_at=row.held_at,
            now=now,
        ),
        "properties": _shape_properties(lead_pool),
        "roi": _compute_roi(
            job_value=float(row.job_value) if row.job_value is not None else None,
            angi_cost=angi_cost,
        ),
    }


# ---------------------------------------------------------------------------
# Demo generator — POST /api/demo/deal-room
# Gated by email + password login (demo_users table), issuing a demo-scoped JWT.
# ---------------------------------------------------------------------------

_demo_bearer = HTTPBearer(auto_error=True)


class _DemoLoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=1)


class _DemoLoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


@router.post("/api/demo/login", response_model=_DemoLoginResponse, tags=["deal-room"])
def demo_login(body: _DemoLoginRequest, db: Session = Depends(get_db)) -> _DemoLoginResponse:
    """Exchange demo credentials (email + password) for a demo-scoped JWT.

    Credentials are stored in demo_users (bcrypt hash). 401 on bad credentials.
    """
    row = db.execute(
        text("SELECT password_hash, is_active FROM demo_users WHERE lower(email) = lower(:e) LIMIT 1"),
        {"e": str(body.email)},
    ).fetchone()
    if row is None or not row.is_active or not verify_password(body.password, row.password_hash):
        raise HTTPException(status_code=401, detail="Invalid demo credentials.")
    token = create_access_token({"sub": str(body.email).lower(), "scope": "demo"})
    logger.info("[deal_room] demo login ok for %s", str(body.email).lower())
    return _DemoLoginResponse(access_token=token)


def require_demo_auth(
    credentials: HTTPAuthorizationCredentials = Depends(_demo_bearer),
) -> dict:
    """Authorize the demo generator via either:
    - a demo-scoped JWT (from /api/demo/login), or
    - a subscriber JWT where is_demo=True (closer already logged in via subscriber flow).
    """
    claims = verify_token(credentials.credentials)  # 401 on invalid/expired
    if claims.get("scope") == "demo":
        return claims
    if claims.get("is_demo"):
        return claims
    raise HTTPException(status_code=403, detail="Not a demo token.")


class _CreateDealRoomRequest(BaseModel):
    prospect_name: str = Field(..., min_length=1)
    prospect_email: EmailStr
    zip_code: str = Field(..., pattern=r"^\d{5}$")
    # A hold is on one (zip, vertical, county) territory — vertical is required,
    # county defaults to hillsborough (the only launched county today).
    vertical: str = Field(..., min_length=1)
    county_id: str = Field(default="hillsborough", min_length=1)
    tier: Literal["starter", "pro", "founder"]
    job_value: float = Field(..., gt=0)
    close_rate: float = Field(..., gt=0, le=1)

    @field_validator("vertical")
    @classmethod
    def _validate_vertical(cls, v: str) -> str:
        if v not in VALID_VERTICALS:
            raise ValueError(f"Unknown vertical: {v!r}")
        return v


class _CreateDealRoomResponse(BaseModel):
    deal_room_url: str
    prefilled_checkout_url: str


@router.post("/api/demo/deal-room", response_model=_CreateDealRoomResponse, status_code=201, tags=["deal-room"])
def demo_create_deal_room(
    body: _CreateDealRoomRequest,
    db: Session = Depends(get_db),
    _auth: dict = Depends(require_demo_auth),
) -> _CreateDealRoomResponse:
    """Create a deal-room for a prospect and return both copy-able links.

    Authorized by a demo-scoped bearer token from POST /api/demo/login.
    Raises 409 if the (zip, vertical, county) territory is not 'available'.
    Tier must be starter | pro | founder (Dominator is retired).
    """
    if not is_county_launched(body.county_id, db):
        raise HTTPException(status_code=400, detail="county_not_launched")

    settings = get_settings()

    properties = get_lead_pool(zip_code=body.zip_code, limit=100)
    properties_snapshot = {"properties": properties, "zip_code": body.zip_code}

    deal_room = create_deal_room(
        db,
        prospect_name=body.prospect_name,
        prospect_email=str(body.prospect_email),
        zip_code=body.zip_code,
        vertical=body.vertical,
        county_id=body.county_id,
        tier=body.tier,
        job_value=body.job_value,
        close_rate=body.close_rate,
        properties_snapshot=properties_snapshot,
    )
    db.commit()

    base = settings.app_base_url.rstrip("/")
    token = deal_room.token

    logger.info(
        "[deal_room] demo created deal_room token=%s ZIP=%s vertical=%s county=%s tier=%s",
        token, body.zip_code, body.vertical, body.county_id, body.tier,
    )

    return _CreateDealRoomResponse(
        deal_room_url=f"{base}/deal-room/{token}",
        prefilled_checkout_url=(
            f"{base}/?start_tier={body.tier}&zip={body.zip_code}"
            f"&vertical={body.vertical}&county={body.county_id}&hold={token}"
        ),
    )


# ---------------------------------------------------------------------------
# Hold checkout session — POST /api/hold-checkout
# ---------------------------------------------------------------------------


class _HoldCheckoutRequest(BaseModel):
    deal_room_token: str


class _HoldCheckoutResponse(BaseModel):
    url: str


@router.post("/api/hold-checkout", response_model=_HoldCheckoutResponse, tags=["deal-room"])
def create_hold_checkout(body: _HoldCheckoutRequest, db: Session = Depends(get_db)) -> _HoldCheckoutResponse:
    """Create a $97 one-time Stripe Checkout Session for a hold deposit.

    Public — no auth required (the token is the authorization).
    Sets deal_room_token in Stripe metadata so the webhook can call apply_hold_payment.
    """
    import stripe as _stripe

    settings = get_settings()

    row = db.execute(
        text("SELECT token, zip_code, tier, prospect_email FROM deal_rooms WHERE token = :token LIMIT 1"),
        {"token": body.deal_room_token},
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Deal room not found.")
    if not row.zip_code:
        raise HTTPException(status_code=409, detail="Deal room has no ZIP — cannot create hold checkout.")

    if is_test_subscriber(row.prospect_email):
        logger.warning(
            "[hold_checkout] blocked test-account email=%s for token=%s",
            row.prospect_email, body.deal_room_token,
        )
        raise HTTPException(status_code=403, detail="Hold checkout not available for test accounts.")

    price_id = settings.active_hold_deposit_price_id
    if not price_id:
        raise HTTPException(status_code=503, detail="Hold deposit not configured.")

    active_key = settings.active_stripe_secret_key
    if not active_key:
        raise HTTPException(status_code=503, detail="Stripe not configured.")

    _stripe.api_key = active_key.get_secret_value()
    base = settings.app_base_url.rstrip("/")

    try:
        session = _stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            customer_email=row.prospect_email or None,
            line_items=[{"price": price_id, "quantity": 1}],
            metadata={"deal_room_token": body.deal_room_token},
            success_url=f"{base}/deal-room/{row.token}?held=1",
            cancel_url=f"{base}/deal-room/{row.token}",
        )
    except Exception:
        logger.error(
            "[hold_checkout] Stripe session creation failed for token=%s",
            body.deal_room_token,
            exc_info=True,
        )
        raise HTTPException(status_code=502, detail="Could not create checkout session.")

    return _HoldCheckoutResponse(url=session.url)
