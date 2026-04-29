"""
Admin API router.

Provides JWT-authenticated endpoints for internal operations:
  POST /api/admin/login                   — issue a 24-hour bearer token
  POST /api/admin/upload/tax-delinquency  — upload a tax delinquency CSV

Auth pattern:
  - Single credential pair from env (ADMIN_USERNAME / ADMIN_PASSWORD)
  - HS256 JWT signed with ADMIN_JWT_SECRET, 24-hour expiry
  - Every protected endpoint uses Depends(get_current_admin)
  - Returns 503 if admin env vars are not configured
"""

import io
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import stripe
from fastapi import APIRouter, Depends, Form, HTTPException, Query, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from sqlalchemy import and_, case, distinct, func, or_, select, text
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import DistressScore, Owner, Property, SentLead, Subscriber
from src.loaders.tax import TaxDelinquencyLoader

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])

_ALGORITHM = "HS256"
_TOKEN_EXPIRE_HOURS = 24
_bearer = HTTPBearer()

# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def _jwt_secret() -> str:
    """Return the JWT secret or raise 503 if not configured."""
    if not settings.admin_jwt_secret:
        raise HTTPException(status_code=503, detail="Admin not configured")
    return settings.admin_jwt_secret.get_secret_value()


def create_access_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.now(timezone.utc) + timedelta(hours=_TOKEN_EXPIRE_HOURS)
    return jwt.encode(payload, _jwt_secret(), algorithm=_ALGORITHM)


def verify_token(token: str) -> dict:
    try:
        return jwt.decode(token, _jwt_secret(), algorithms=[_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def get_current_admin(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    return verify_token(credentials.credentials)


# ---------------------------------------------------------------------------
# DB dependency
# ---------------------------------------------------------------------------

def get_db():
    with get_db_context() as db:
        yield db


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/login", response_model=TokenResponse)
def admin_login(body: LoginRequest):
    """
    Exchange admin credentials for a 24-hour JWT bearer token.
    Returns 503 if ADMIN_PASSWORD / ADMIN_JWT_SECRET are not set in env.
    Returns 401 on wrong credentials.
    """
    if not settings.admin_password:
        raise HTTPException(status_code=503, detail="Admin not configured")

    username_ok = secrets.compare_digest(body.username, settings.admin_username)
    password_ok = secrets.compare_digest(
        body.password,
        settings.admin_password.get_secret_value(),
    )
    if not (username_ok and password_ok):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({"sub": body.username})
    logger.info("[Admin] Login successful for user: %s", body.username)
    return TokenResponse(access_token=token)


@router.post("/upload/tax-delinquency")
def upload_tax_delinquency(
    file: UploadFile,
    county_id: str = Form("hillsborough"),
    tax_year: Optional[int] = Form(None),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Upload a tax delinquency CSV and run it through TaxDelinquencyLoader.

    Expected CSV columns: Account Number, Tax Yr, Owner Name
    Optional enrichment columns: years_delinquent_scraped, total_amount_due, Cert Status, Deed Status

    If tax_year is provided and the CSV lacks a 'Tax Yr' column, it is injected automatically.

    Returns matched/unmatched/skipped counts.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv")

    content = file.file.read().decode("utf-8", errors="replace")

    try:
        df = pd.read_csv(io.StringIO(content))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {exc}")

    # Validate required column
    if "Account Number" not in df.columns:
        raise HTTPException(
            status_code=400,
            detail="CSV must contain 'Account Number' column. "
                   f"Found columns: {list(df.columns)}",
        )

    # Inject Tax Yr column if caller provided it and CSV doesn't have one
    if "Tax Yr" not in df.columns:
        if tax_year is not None:
            df["Tax Yr"] = tax_year
        else:
            raise HTTPException(
                status_code=400,
                detail="CSV must contain 'Tax Yr' column, or pass tax_year as a form field.",
            )

    total_rows = len(df)
    logger.info(
        "[Admin] Tax delinquency upload: %d rows, county=%s, tax_year=%s, user=%s",
        total_rows, county_id, tax_year, _admin.get("sub"),
    )

    loader = TaxDelinquencyLoader(db, county_id=county_id)
    matched, updated, unmatched = loader.load_from_dataframe(df)

    logger.info(
        "[Admin] Upload complete: inserted=%d updated=%d unmatched=%d",
        matched, updated, unmatched,
    )
    return {
        "matched": matched,
        "updated": updated,
        "unmatched": unmatched,
        "total_rows": total_rows,
    }


# ---------------------------------------------------------------------------
# GET  /api/admin/refunds/unlocks     — list lead-unlock purchases
# POST /api/admin/refunds/unlock/{id} — issue Stripe refund + log reason
# ---------------------------------------------------------------------------

class RefundRequest(BaseModel):
    reason: str


@router.get("/refunds/unlocks")
def list_unlock_refunds(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return recent lead-unlock purchases with refund status.
    Joins SentLead → Subscriber (email) and Property (address).
    """
    rows = db.execute(
        select(SentLead, Subscriber.email, Property.address)
        .join(Subscriber, Subscriber.id == SentLead.subscriber_id)
        .join(Property, Property.id == SentLead.property_id)
        .where(SentLead.source == "lead_unlock_payment")
        .order_by(SentLead.sent_at.desc())
        .limit(limit)
        .offset(offset)
    ).all()

    return [
        {
            "id": sl.id,
            "subscriber_email": email,
            "property_address": address,
            "sent_at": sl.sent_at.isoformat() if sl.sent_at else None,
            "stripe_payment_intent_id": sl.stripe_payment_intent_id,
            "refunded_at": sl.refunded_at.isoformat() if sl.refunded_at else None,
            "refund_reason": sl.refund_reason,
            "stripe_refund_id": sl.stripe_refund_id,
        }
        for sl, email, address in rows
    ]


@router.post("/refunds/unlock/{sent_lead_id}")
def issue_unlock_refund(
    sent_lead_id: int,
    body: RefundRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Issue a full Stripe refund for a $4 lead-unlock and log the reason.
    Returns 404 if the SentLead doesn't exist or isn't an unlock purchase.
    Returns 409 if already refunded.
    Returns 400 if no payment intent ID was stored (pre-migration unlock).
    """
    sl = db.execute(
        select(SentLead).where(
            SentLead.id == sent_lead_id,
            SentLead.source == "lead_unlock_payment",
        )
    ).scalar_one_or_none()

    if not sl:
        raise HTTPException(status_code=404, detail="Lead unlock purchase not found")
    if sl.refunded_at:
        raise HTTPException(status_code=409, detail="Already refunded")
    if not sl.stripe_payment_intent_id:
        raise HTTPException(
            status_code=400,
            detail="No payment intent ID on record — refund manually in Stripe dashboard",
        )

    if not settings.active_stripe_secret_key:
        raise HTTPException(status_code=503, detail="Stripe not configured")

    stripe.api_key = settings.active_stripe_secret_key.get_secret_value()
    try:
        refund = stripe.Refund.create(payment_intent=sl.stripe_payment_intent_id)
    except stripe.error.StripeError as exc:
        logger.error("[Admin] Stripe refund failed for SentLead %s: %s", sent_lead_id, exc)
        raise HTTPException(status_code=502, detail=f"Stripe error: {exc.user_message or str(exc)}")

    sl.refunded_at = datetime.now(timezone.utc)
    sl.refund_reason = body.reason[:255]
    sl.stripe_refund_id = refund.id
    db.commit()

    logger.info(
        "[Admin] Refund issued: sent_lead=%s pi=%s refund=%s reason=%r admin=%s",
        sent_lead_id, sl.stripe_payment_intent_id, refund.id, body.reason, _admin.get("sub"),
    )
    return {
        "refund_id": refund.id,
        "status": refund.status,
        "sent_lead_id": sent_lead_id,
    }


# ---------------------------------------------------------------------------
# GET /api/admin/stats/contact-coverage — dark-pool visibility
# ---------------------------------------------------------------------------

_GOLD_PLUS_TIERS = ("Gold", "Platinum", "Ultra Platinum")

_HAS_CONTACT = or_(
    Owner.phone_1.isnot(None),
    Owner.phone_2.isnot(None),
    Owner.phone_3.isnot(None),
    Owner.email_1.isnot(None),
    Owner.email_2.isnot(None),
)


@router.get("/stats/contact-coverage")
def contact_coverage_stats(
    county_id: str = Query("hillsborough"),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Returns the size of the 'dark pool' — Gold+ scored properties with no
    owner contact data, broken down by ZIP. Used to quantify the upstream
    revenue leak before deciding on skip-tracing investment.

    Uses the most recent DistressScore per property (DISTINCT ON property_id
    ordered by score_date DESC).
    """
    # Subquery: latest score per property for Gold+ tiers in this county
    latest_sq = (
        select(
            DistressScore.property_id,
            DistressScore.final_cds_score,
            DistressScore.lead_tier,
        )
        .distinct(DistressScore.property_id)
        .where(
            DistressScore.lead_tier.in_(_GOLD_PLUS_TIERS),
            DistressScore.county_id == county_id,
        )
        .order_by(DistressScore.property_id, DistressScore.score_date.desc())
        .subquery()
    )

    has_contact_col = case((_HAS_CONTACT, 1), else_=0).label("has_contact")

    # Per-ZIP aggregation
    zip_rows = db.execute(
        select(
            Property.zip.label("zip"),
            func.count().label("total"),
            func.sum(has_contact_col).label("with_contact"),
            func.sum(case((~_HAS_CONTACT, 1), else_=0)).label("contactless"),
        )
        .join(latest_sq, latest_sq.c.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .group_by(Property.zip)
        .order_by(func.sum(case((~_HAS_CONTACT, 1), else_=0)).desc())
    ).all()

    total_qualified  = sum(r.total for r in zip_rows)
    total_contactless = sum(r.contactless for r in zip_rows)
    total_with_contact = sum(r.with_contact for r in zip_rows)

    return {
        "county_id": county_id,
        "summary": {
            "total_gold_plus": total_qualified,
            "with_contact": total_with_contact,
            "contactless": total_contactless,
            "contactless_pct": round(100 * total_contactless / total_qualified, 1) if total_qualified else 0,
        },
        "by_zip": [
            {
                "zip": r.zip or "unknown",
                "total": r.total,
                "with_contact": r.with_contact,
                "contactless": r.contactless,
                "contactless_pct": round(100 * r.contactless / r.total, 1) if r.total else 0,
            }
            for r in zip_rows
        ],
    }


# ---------------------------------------------------------------------------
# GET /api/admin/synthflow/config — Synthflow dashboard data
# ---------------------------------------------------------------------------

_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


@router.get("/synthflow/config")
def synthflow_config(_admin: dict = Depends(get_current_admin)):
    """
    Return campaign list, agent settings, and prompt scripts read from the
    config/ directory JSON/YAML files. Powers the Synthflow dashboard tab.
    """
    import yaml

    agents = []
    for fname in ["finetuner_roofing_agent.json", "finetuner_remediation_agent.json"]:
        p = _CONFIG_DIR / fname
        if not p.exists():
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        cfg = data.get("configuration", {})
        meta = data.get("source_metadata", {})
        vertical = "roofing" if "roofing" in fname else "remediation"
        agents.append({
            "vertical": vertical,
            "agent_id": meta.get("agent_id"),
            "status": "Draft" if meta.get("is_draft") else "Published",
            "voice_name": cfg.get("voice_name"),
            "voice_provider": cfg.get("voice_provider"),
            "llm": cfg.get("llm"),
            "webhook_url": cfg.get("external_webhook_url"),
            "greeting": cfg.get("greeting_message"),
            "max_duration_seconds": cfg.get("max_duration_seconds"),
            "language": cfg.get("language"),
            "agent_type": cfg.get("agent_type"),
        })

    campaigns = []
    prompts = []
    for fname in ["synthflow_roofing_agent.yaml", "synthflow_remediation_agent.yaml"]:
        p = _CONFIG_DIR / "prompts" / fname
        if not p.exists():
            continue
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        camp = data.get("campaign", {})
        campaigns.append({
            "name": camp.get("name"),
            "vertical": data.get("vertical"),
            "area_codes": data.get("area_codes", []),
            "total_volume": camp.get("total_volume"),
            "daily_cap": camp.get("daily_cap_per_number"),
            "launch_date": str(camp.get("launch_date", "")),
            "prospect_sources": camp.get("prospect_sources", []),
            "webhook_url": data.get("webhook", {}).get("url", ""),
        })
        prompts.append({
            "vertical": data.get("vertical"),
            "agent_name": data.get("agent_name"),
            "system_prompt": data.get("system_prompt", ""),
            "first_message": data.get("first_message", ""),
            "voicemail_script": data.get("voicemail_script", ""),
        })

    return {"agents": agents, "campaigns": campaigns, "prompts": prompts}
