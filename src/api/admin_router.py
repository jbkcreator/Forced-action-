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
from typing import Optional, Literal

import pandas as pd
import stripe
from fastapi import APIRouter, Depends, Form, HTTPException, Query, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel, Field
from sqlalchemy import and_, case, distinct, func, or_, select, text
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import (
    County,
    CountyColumnMapping,
    CountySource,
    DistressScore,
    EnrichmentUsageLog,
    Owner,
    PremiumPurchase,
    Property,
    SentLead,
    Subscriber,
)
from src.loaders.tax import TaxDelinquencyLoader
from src.utils.county_config import invalidate_cache

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


# ---------------------------------------------------------------------------
# GET /api/admin/stats/sku-margin — Premium SKU revenue / cost / margin
# ---------------------------------------------------------------------------

# Retail prices in cents — must mirror config.revenue_ladder.PREMIUM_CREDITS.
_SKU_RETAIL_CENTS = {"report": 700, "brief": 1200, "transfer": 6500, "byol": 500}
_PREMIUM_SKUS = ["report", "brief", "transfer", "byol"]
_WINDOW_DAYS = {"7d": 7, "30d": 30, "90d": 90}


@router.get("/stats/sku-margin")
def sku_margin_stats(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """Per-SKU revenue / cost / margin / refund / dispute summary across
    rolling 7-day, 30-day, and 90-day windows.

    Cost is the sum of EnrichmentUsageLog.cost_cents joined to PremiumPurchase
    via property_id within the window. For SKUs that don't trigger an
    enrichment lookup (report, brief — artifact-only today), cost stays at 0
    and gross margin is 100% minus Stripe processing.
    """
    now = datetime.now(timezone.utc)
    out = {"as_of": now.isoformat(), "windows": {}}

    for window_key, days in _WINDOW_DAYS.items():
        cutoff = now - timedelta(days=days)
        per_sku = {}
        for sku in _PREMIUM_SKUS:
            # Counts by paid_via
            counts = db.execute(
                select(
                    PremiumPurchase.paid_via,
                    PremiumPurchase.status,
                    func.count().label("n"),
                    func.coalesce(func.sum(PremiumPurchase.amount_cents), 0).label("amount_cents"),
                )
                .where(
                    PremiumPurchase.sku == sku,
                    PremiumPurchase.purchased_at >= cutoff,
                )
                .group_by(PremiumPurchase.paid_via, PremiumPurchase.status)
            ).all()

            total = 0
            delivered = 0
            refunded = 0
            disputed = 0
            failed = 0
            gross_revenue_cents = 0
            for r in counts:
                total += r.n
                if r.status == "delivered":
                    delivered += r.n
                    gross_revenue_cents += int(r.amount_cents or 0) if r.paid_via == "card" else 0
                elif r.status == "refunded":
                    refunded += r.n
                elif r.status == "disputed":
                    disputed += r.n
                elif r.status == "failed":
                    failed += r.n

            # Cost — sum of enrichment usage logs joined by property_id, within window
            cost_cents = db.execute(
                select(func.coalesce(func.sum(EnrichmentUsageLog.cost_cents), 0))
                .select_from(EnrichmentUsageLog)
                .join(
                    PremiumPurchase,
                    PremiumPurchase.property_id == EnrichmentUsageLog.property_id,
                )
                .where(
                    PremiumPurchase.sku == sku,
                    PremiumPurchase.purchased_at >= cutoff,
                    EnrichmentUsageLog.created_at >= cutoff,
                )
            ).scalar() or 0

            margin_pct = None
            if gross_revenue_cents > 0:
                margin_pct = round(
                    100.0 * (gross_revenue_cents - cost_cents) / gross_revenue_cents, 1
                )

            refund_rate = round(100.0 * refunded / total, 1) if total else 0.0
            dispute_rate = round(100.0 * disputed / total, 1) if total else 0.0

            per_sku[sku] = {
                "label": sku,
                "retail_cents": _SKU_RETAIL_CENTS.get(sku, 0),
                "total_purchases": total,
                "delivered": delivered,
                "refunded": refunded,
                "disputed": disputed,
                "failed": failed,
                "gross_revenue_cents": gross_revenue_cents,
                "cost_cents": int(cost_cents or 0),
                "margin_cents": gross_revenue_cents - int(cost_cents or 0),
                "margin_pct": margin_pct,
                "refund_rate_pct": refund_rate,
                "dispute_rate_pct": dispute_rate,
            }
        out["windows"][window_key] = per_sku

    return out


# ===========================================================================
# DEV TOOLS GATE
# ===========================================================================

@router.get("/dev/ping")
def dev_ping(_admin: dict = Depends(get_current_admin)):
    """
    Returns 200 if DEV_TOOLS_ENABLED=true, 403 otherwise.
    The /dev frontend route calls this on mount — if it gets a 403 it shows
    a locked screen instead of rendering the dev tools.
    """
    if not settings.dev_tools_enabled:
        raise HTTPException(status_code=403, detail="Dev tools are disabled in this environment")
    return {"enabled": True}


# ===========================================================================
# COUNTY MANAGEMENT
# ===========================================================================
# GET  /api/admin/counties                           — list all counties
# POST /api/admin/counties                           — create county
# GET  /api/admin/counties/{county_id}               — get single county
# PATCH /api/admin/counties/{county_id}              — update county fields
# DELETE /api/admin/counties/{county_id}             — soft-delete (is_active=False)
# GET  /api/admin/counties/{county_id}/sources       — list sources for county
# POST /api/admin/counties/{county_id}/sources       — add source
# PUT  /api/admin/counties/{county_id}/sources/{id}  — update source
# DELETE /api/admin/counties/{county_id}/sources/{id} — soft-delete source
# ===========================================================================


class CountyCreateRequest(BaseModel):
    county_id: str
    display_name: str
    fips: Optional[str] = None
    nws_zone: Optional[str] = None
    parcel_id_format: str = "folio"
    bankruptcy_division: Optional[str] = None
    city_filer_keywords: list[str] = []
    code_lien_type_map: dict = {}


class CountyUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    fips: Optional[str] = None
    nws_zone: Optional[str] = None
    parcel_id_format: Optional[str] = None
    bankruptcy_division: Optional[str] = None
    city_filer_keywords: Optional[list[str]] = None
    code_lien_type_map: Optional[dict] = None
    is_active: Optional[bool] = None


ScrapeMode = Literal["ai_only", "playwright_only", "playwright_then_ai"]


class CountySourceCreateRequest(BaseModel):
    signal_type: str
    source_name: Optional[str] = None
    url: str
    description: Optional[str] = None
    navigation_hint: Optional[str] = None
    output_format: Optional[str] = None
    date_range_available: bool = True
    frequency: str = "daily"
    special_flags: dict = {}
    scrape_mode: ScrapeMode = "ai_only"
    # playwright_code is intentionally NOT settable from this endpoint —
    # callers go through /sources/{id}/playwright-code/{save,generate,validate}
    # so the AST + LLM safety pipeline runs first.


class CountySourceUpdateRequest(BaseModel):
    source_name: Optional[str] = None
    url: Optional[str] = None
    description: Optional[str] = None
    navigation_hint: Optional[str] = None
    output_format: Optional[str] = None
    date_range_available: Optional[bool] = None
    frequency: Optional[str] = None
    is_active: Optional[bool] = None
    special_flags: Optional[dict] = None
    scrape_mode: Optional[ScrapeMode] = None


def _county_to_dict(county: County) -> dict:
    return {
        "county_id":           county.county_id,
        "display_name":        county.display_name,
        "fips":                county.fips,
        "nws_zone":            county.nws_zone,
        "parcel_id_format":    county.parcel_id_format,
        "bankruptcy_division": county.bankruptcy_division,
        "city_filer_keywords": county.city_filer_keywords or [],
        "code_lien_type_map":  county.code_lien_type_map or {},
        "is_active":           county.is_active,
        "created_at":          county.created_at.isoformat() if county.created_at else None,
        "updated_at":          county.updated_at.isoformat() if county.updated_at else None,
    }


def _source_to_dict(src: CountySource) -> dict:
    return {
        "id":                       src.id,
        "county_id":                src.county_id,
        "signal_type":              src.signal_type,
        "source_name":              src.source_name,
        "url":                      src.url,
        "description":              src.description,
        "navigation_hint":          src.navigation_hint,
        "output_format":            src.output_format,
        "date_range_available":     src.date_range_available,
        "frequency":                src.frequency,
        "is_active":                src.is_active,
        "special_flags":            src.special_flags or {},
        "scrape_mode":              src.scrape_mode,
        "playwright_code":          src.playwright_code,
        "playwright_code_version":  src.playwright_code_version,
        "playwright_code_approved": src.playwright_code_approved,
        "created_at":               src.created_at.isoformat() if src.created_at else None,
        "updated_at":               src.updated_at.isoformat() if src.updated_at else None,
    }


@router.get("/counties")
def list_counties(
    include_inactive: bool = Query(False),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    q = db.query(County)
    if not include_inactive:
        q = q.filter(County.is_active == True)
    counties = q.order_by(County.county_id).all()
    return [_county_to_dict(c) for c in counties]


@router.post("/counties", status_code=201)
def create_county(
    body: CountyCreateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if db.query(County).filter_by(county_id=body.county_id).first():
        raise HTTPException(status_code=409, detail=f"County '{body.county_id}' already exists")

    county = County(
        county_id=body.county_id,
        display_name=body.display_name,
        fips=body.fips,
        nws_zone=body.nws_zone,
        parcel_id_format=body.parcel_id_format,
        bankruptcy_division=body.bankruptcy_division,
        city_filer_keywords=body.city_filer_keywords,
        code_lien_type_map=body.code_lien_type_map,
        is_active=True,
    )
    db.add(county)
    db.commit()
    db.refresh(county)
    logger.info("[Admin] County created: %s by %s", body.county_id, _admin.get("sub"))
    invalidate_cache(body.county_id)
    return _county_to_dict(county)


@router.get("/counties/{county_id}")
def get_county(
    county_id: str,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    county = db.query(County).filter_by(county_id=county_id).first()
    if not county:
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")
    return _county_to_dict(county)


@router.patch("/counties/{county_id}")
def update_county(
    county_id: str,
    body: CountyUpdateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    county = db.query(County).filter_by(county_id=county_id).first()
    if not county:
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")

    updates = body.model_dump(exclude_none=True)
    for field, value in updates.items():
        setattr(county, field, value)

    db.commit()
    db.refresh(county)
    logger.info("[Admin] County updated: %s fields=%s by %s", county_id, list(updates), _admin.get("sub"))
    invalidate_cache(county_id)
    return _county_to_dict(county)


@router.delete("/counties/{county_id}", status_code=204)
def deactivate_county(
    county_id: str,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    county = db.query(County).filter_by(county_id=county_id).first()
    if not county:
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")
    county.is_active = False
    db.commit()
    logger.info("[Admin] County deactivated: %s by %s", county_id, _admin.get("sub"))
    invalidate_cache(county_id)


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

@router.get("/counties/{county_id}/sources")
def list_sources(
    county_id: str,
    include_inactive: bool = Query(False),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if not db.query(County).filter_by(county_id=county_id).first():
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")

    q = db.query(CountySource).filter_by(county_id=county_id)
    if not include_inactive:
        q = q.filter(CountySource.is_active == True)
    sources = q.order_by(CountySource.signal_type).all()
    return [_source_to_dict(s) for s in sources]


@router.post("/counties/{county_id}/sources", status_code=201)
def add_source(
    county_id: str,
    body: CountySourceCreateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if not db.query(County).filter_by(county_id=county_id).first():
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")

    existing = db.query(CountySource).filter_by(
        county_id=county_id, signal_type=body.signal_type
    ).first()
    if existing and existing.is_active:
        raise HTTPException(
            status_code=409,
            detail=f"Active source for signal_type '{body.signal_type}' already exists in '{county_id}'",
        )

    src = CountySource(
        county_id=county_id,
        signal_type=body.signal_type,
        source_name=body.source_name,
        url=body.url,
        description=body.description,
        navigation_hint=body.navigation_hint,
        output_format=body.output_format,
        date_range_available=body.date_range_available,
        frequency=body.frequency,
        special_flags=body.special_flags,
        scrape_mode=body.scrape_mode,
        is_active=True,
    )
    db.add(src)
    db.commit()
    db.refresh(src)
    logger.info(
        "[Admin] Source added: county=%s signal=%s id=%s by %s",
        county_id, body.signal_type, src.id, _admin.get("sub"),
    )
    invalidate_cache(county_id)
    return _source_to_dict(src)


@router.put("/counties/{county_id}/sources/{source_id}")
def update_source(
    county_id: str,
    source_id: int,
    body: CountySourceUpdateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    src = db.query(CountySource).filter_by(id=source_id, county_id=county_id).first()
    if not src:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found in '{county_id}'")

    updates = body.model_dump(exclude_none=True)
    for field, value in updates.items():
        setattr(src, field, value)

    db.commit()
    db.refresh(src)
    logger.info(
        "[Admin] Source updated: id=%s county=%s fields=%s by %s",
        source_id, county_id, list(updates), _admin.get("sub"),
    )
    invalidate_cache(county_id)
    return _source_to_dict(src)


@router.delete("/counties/{county_id}/sources/{source_id}", status_code=204)
def deactivate_source(
    county_id: str,
    source_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    src = db.query(CountySource).filter_by(id=source_id, county_id=county_id).first()
    if not src:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found in '{county_id}'")
    src.is_active = False
    db.commit()
    logger.info("[Admin] Source deactivated: id=%s county=%s by %s", source_id, county_id, _admin.get("sub"))
    invalidate_cache(county_id)


# ===========================================================================
# PLAYWRIGHT CODE LIFECYCLE
# ===========================================================================
# Mounted under /api/admin/counties/{cid}/sources/{sid}/playwright-code.
# Every route enforces ownership of the source by the county_id in the path.
# The on-disk module that does all the heavy lifting (AST validation, LLM
# generation, history-table append) is src/utils/action_sequence.py — these
# routes are thin HTTP wrappers around its helpers.
# ===========================================================================


class PlaywrightCodeSaveRequest(BaseModel):
    code: str
    approved: bool = False  # admin-authored code: True; LLM output: False
    prompt_version: Optional[str] = None


class PlaywrightCodeValidateRequest(BaseModel):
    code: str


def _require_source(county_id: str, source_id: int, db: Session) -> CountySource:
    src = db.query(CountySource).filter_by(id=source_id, county_id=county_id).first()
    if not src:
        raise HTTPException(
            status_code=404,
            detail=f"Source {source_id} not found in county '{county_id}'",
        )
    return src


@router.post("/counties/{county_id}/sources/{source_id}/playwright-code/generate")
def generate_playwright_code_route(
    county_id: str,
    source_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Ask the LLM to generate a run_scrape function for this source. Returns
    the code WITHOUT saving it — caller decides whether to validate, edit, or
    persist. Generation is read-only; the source row is not modified.
    """
    src = _require_source(county_id, source_id, db)
    from src.utils.action_sequence import (
        generate_playwright_code,
        PlaywrightCodeError,
    )
    source_dict = _source_to_dict(src) | (src.special_flags or {})
    try:
        code = generate_playwright_code(source_dict, signal_type=src.signal_type)
    except PlaywrightCodeError as exc:
        raise HTTPException(status_code=502, detail=f"Code generation failed: {exc}")
    return {"code": code, "approved": False}


@router.post("/counties/{county_id}/sources/{source_id}/playwright-code/validate")
def validate_playwright_code_route(
    county_id: str,
    source_id: int,
    body: PlaywrightCodeValidateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Run the AST safety check + structural validation against caller-supplied
    code. Returns {valid, errors} without persisting. Use this from the admin
    UI before saving paste-your-own code.
    """
    _require_source(county_id, source_id, db)
    from src.utils.action_sequence import (
        validate_playwright_code,
        PlaywrightCodeError,
    )
    try:
        validate_playwright_code(body.code)
    except PlaywrightCodeError as exc:
        return {"valid": False, "errors": [str(exc)]}
    return {"valid": True, "errors": []}


@router.post("/counties/{county_id}/sources/{source_id}/playwright-code", status_code=201)
def save_playwright_code_route(
    county_id: str,
    source_id: int,
    body: PlaywrightCodeSaveRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Persist code to county_sources.playwright_code + append a history row.

    Re-runs the AST validator server-side so a client that bypasses /validate
    can't sneak unsafe code in. `approved=True` marks it admin-authored and
    skips the unapproved-code-warning at scrape time.
    """
    _require_source(county_id, source_id, db)
    from src.utils.action_sequence import (
        validate_playwright_code,
        persist_playwright_code,
        PlaywrightCodeError,
    )
    try:
        validate_playwright_code(body.code)
    except PlaywrightCodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid code: {exc}")

    persist_playwright_code(
        county_id, source_id, body.code,
        prompt_version=body.prompt_version,
        is_approved=body.approved,
    )
    logger.info(
        "[Admin] playwright_code saved: source_id=%s approved=%s by %s",
        source_id, body.approved, _admin.get("sub"),
    )
    return {"is_approved": body.approved}


@router.post("/counties/{county_id}/sources/{source_id}/playwright-code/approve")
def approve_playwright_code_route(
    county_id: str,
    source_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """Flip playwright_code_approved=True; appends an 'approved_by:<sub>' history row."""
    _require_source(county_id, source_id, db)
    from src.utils.action_sequence import approve_playwright_code
    approve_playwright_code(county_id, source_id, approved_by=_admin.get("sub") or "admin")
    return {"is_approved": True}


@router.delete("/counties/{county_id}/sources/{source_id}/playwright-code", status_code=204)
def clear_playwright_code_route(
    county_id: str,
    source_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """Wipe the cached code (engine will regenerate on next run)."""
    _require_source(county_id, source_id, db)
    from src.utils.action_sequence import clear_playwright_code
    clear_playwright_code(county_id, source_id)
    logger.info("[Admin] playwright_code cleared: source_id=%s by %s",
                source_id, _admin.get("sub"))


# ===========================================================================
# COLUMN MAPPING APPROVAL WORKFLOW
# ===========================================================================
# GET   /api/admin/mappings/pending          — list pending (LLM-proposed, awaiting review)
# GET   /api/admin/mappings/approved         — list all active approved mappings
# GET   /api/admin/mappings/rejected         — list rejected mappings (queued for re-map)
# GET   /api/admin/mappings/{id}             — single mapping detail
# POST  /api/admin/mappings/{id}/approve     — approve a pending mapping
# POST  /api/admin/mappings/{id}/reject      — reject mapping (with feedback for LLM)
# PATCH /api/admin/mappings/{id}             — edit column assignments on any mapping
# POST  /api/admin/mappings/preview-columns  — upload CSV/XLSX → get columns + sample rows
# POST  /api/admin/mappings/manual           — save human-created mapping as approved
# ===========================================================================


def _mapping_to_dict(m: CountyColumnMapping) -> dict:
    return {
        "id":              m.id,
        "source_id":       m.source_id,
        "source_columns":  m.source_columns,
        "mapping":         m.mapping,
        "is_approved":     m.is_approved,
        "mapped_by":       m.mapped_by,
        "approved_by":     m.approved_by,
        "approved_at":     m.approved_at.isoformat() if m.approved_at else None,
        "sample_rows":     m.sample_rows,
        "reject_feedback": m.reject_feedback,
        # Transformation fields — applied in order after the column rename.
        "post_processors": m.post_processors or [],
        "value_maps":      m.value_maps or {},
        "row_routing":     m.row_routing,
        "created_at":      m.created_at.isoformat() if m.created_at else None,
        "updated_at":      m.updated_at.isoformat() if m.updated_at else None,
    }


@router.get("/mappings/pending")
def list_pending_mappings(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return all column mappings awaiting approval, with their source info.
    The LLM column mapper saves mappings as is_approved=False when it encounters
    a new source or changed columns.
    """
    rows = (
        db.query(CountyColumnMapping, CountySource)
        .join(CountySource, CountySource.id == CountyColumnMapping.source_id)
        .filter(
            CountyColumnMapping.is_approved == False,
            CountyColumnMapping.reject_feedback == None,
        )
        .order_by(CountyColumnMapping.created_at.desc())
        .all()
    )
    return [
        {
            **_mapping_to_dict(mapping),
            "source": _source_to_dict(source),
        }
        for mapping, source in rows
    ]


@router.get("/mappings/rejected")
def list_rejected_mappings(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return all rejected column mappings (is_approved=False, reject_feedback set).
    These are queued for LLM re-map on the next scrape run.
    """
    rows = (
        db.query(CountyColumnMapping, CountySource)
        .join(CountySource, CountySource.id == CountyColumnMapping.source_id)
        .filter(
            CountyColumnMapping.is_approved == False,
            CountyColumnMapping.reject_feedback != None,
        )
        .order_by(CountyColumnMapping.created_at.desc())
        .all()
    )
    return [
        {
            **_mapping_to_dict(mapping),
            "source": _source_to_dict(source),
        }
        for mapping, source in rows
    ]


@router.get("/mappings/approved")
def list_approved_mappings(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return all active approved mappings, one per source (most recently approved wins).
    Used by the admin UI to browse and edit existing mappings.
    """
    from sqlalchemy import func as sqlfunc

    # Subquery: latest approved_at per source_id
    latest_subq = (
        db.query(
            CountyColumnMapping.source_id,
            sqlfunc.max(CountyColumnMapping.approved_at).label("latest_approved_at"),
        )
        .filter(CountyColumnMapping.is_approved == True)
        .group_by(CountyColumnMapping.source_id)
        .subquery()
    )

    rows = (
        db.query(CountyColumnMapping, CountySource)
        .join(CountySource, CountySource.id == CountyColumnMapping.source_id)
        .join(
            latest_subq,
            (CountyColumnMapping.source_id == latest_subq.c.source_id)
            & (CountyColumnMapping.approved_at == latest_subq.c.latest_approved_at),
        )
        .order_by(CountySource.county_id, CountySource.signal_type)
        .all()
    )
    return [
        {
            **_mapping_to_dict(mapping),
            "source": _source_to_dict(source),
        }
        for mapping, source in rows
    ]


@router.get("/mappings/{mapping_id}")
def get_mapping(
    mapping_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    mapping = db.query(CountyColumnMapping).filter_by(id=mapping_id).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")
    source = db.query(CountySource).filter_by(id=mapping.source_id).first()
    return {**_mapping_to_dict(mapping), "source": _source_to_dict(source) if source else None}


class MappingUpdateRequest(BaseModel):
    # {source_col: new_canonical} — merged into the existing rename dict
    column_updates: Optional[dict] = None
    # Optional transformation-layer replacements. None = leave field unchanged.
    # Empty list / dict = clear the field.
    post_processors: Optional[list] = None
    value_maps: Optional[dict] = None
    row_routing: Optional[dict] = None


@router.patch("/mappings/{mapping_id}")
def update_mapping(
    mapping_id: int,
    body: MappingUpdateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Edit column assignments and/or transformation rules on an existing mapping.

    Any field omitted from the body is left unchanged. column_updates is merged
    into the existing rename dict; post_processors / value_maps / row_routing
    are full replacements (since they're admin-curated structured payloads).

    Approval state: marks the mapping is_approved=True, clears any prior reject
    feedback, and stamps approved_by/at to reflect the edit.
    """
    from datetime import datetime, timezone

    mapping = db.query(CountyColumnMapping).filter_by(id=mapping_id).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")

    if body.column_updates is not None:
        merged = dict(mapping.mapping)
        merged.update(body.column_updates)
        mapping.mapping = merged
    if body.post_processors is not None:
        mapping.post_processors = body.post_processors
    if body.value_maps is not None:
        mapping.value_maps = body.value_maps
    if body.row_routing is not None:
        mapping.row_routing = body.row_routing

    mapping.is_approved = True
    mapping.reject_feedback = None  # clear any prior rejection if editing back to approved
    mapping.approved_by = _admin.get("sub")
    mapping.approved_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(mapping)

    logger.info(
        "[Admin] Mapping updated: id=%s source_id=%s by %s cols=%s pp=%s vmap=%s routing=%s",
        mapping_id, mapping.source_id, _admin.get("sub"),
        list((body.column_updates or {}).keys()),
        body.post_processors is not None,
        body.value_maps is not None,
        body.row_routing is not None,
    )
    return _mapping_to_dict(mapping)


class MappingApproveRequest(BaseModel):
    mapping_overrides: Optional[dict] = None  # optional admin corrections before approving


class MappingRejectRequest(BaseModel):
    feedback: str  # plain-English reason — returned to LLM on re-map


@router.post("/mappings/{mapping_id}/approve")
def approve_mapping(
    mapping_id: int,
    body: MappingApproveRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Approve a pending column mapping. Optional mapping_overrides let the admin
    correct individual column assignments before approving — the overrides are
    merged into the LLM-proposed mapping before saving.
    """
    from datetime import datetime, timezone

    mapping = db.query(CountyColumnMapping).filter_by(id=mapping_id).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")
    if mapping.is_approved:
        raise HTTPException(status_code=409, detail="Mapping is already approved")

    if body.mapping_overrides:
        merged = dict(mapping.mapping)
        merged.update(body.mapping_overrides)
        mapping.mapping = merged

    mapping.is_approved = True
    mapping.approved_by = _admin.get("sub")
    mapping.approved_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(mapping)

    logger.info(
        "[Admin] Mapping approved: id=%s source_id=%s by %s overrides=%s",
        mapping_id, mapping.source_id, _admin.get("sub"), bool(body.mapping_overrides),
    )
    return _mapping_to_dict(mapping)


@router.post("/mappings/{mapping_id}/reject")
def reject_mapping(
    mapping_id: int,
    body: MappingRejectRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Reject a pending mapping. The feedback string is stored on the mapping row
    so the LLM column mapper can incorporate it on the next re-map attempt.
    The rejected mapping is left in the table (is_approved=False) — the mapper
    will create a new pending mapping on the next scrape run.
    """
    mapping = db.query(CountyColumnMapping).filter_by(id=mapping_id).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")

    mapping.reject_feedback = body.feedback
    db.commit()

    logger.info(
        "[Admin] Mapping rejected: id=%s source_id=%s by %s feedback=%r",
        mapping_id, mapping.source_id, _admin.get("sub"), body.feedback[:100],
    )
    return {"status": "rejected", "mapping_id": mapping_id, "feedback": body.feedback}


# ---------------------------------------------------------------------------
# Column Mapping — admin-created (human) mappings
# ---------------------------------------------------------------------------

@router.post("/mappings/preview-columns")
async def preview_columns(
    file: UploadFile,
    _admin: dict = Depends(get_current_admin),
):
    """
    Accept a CSV or XLSX file upload and return its column names plus the first
    5 rows as sample data.  Used by the admin UI to populate the manual mapping form.
    """
    import io
    import pandas as pd

    contents = await file.read()
    filename = (file.filename or "").lower()

    try:
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(contents), dtype=str, nrows=5)
        else:
            df = pd.read_csv(io.BytesIO(contents), dtype=str, nrows=5)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")

    df.columns = df.columns.str.strip()
    sample_rows = df.fillna("").astype(str).to_dict("records")

    return {
        "columns": list(df.columns),
        "sample_rows": sample_rows,
    }


class ManualMappingRequest(BaseModel):
    source_id: int
    mapping: dict          # {source_col: canonical_col}
    sample_rows: Optional[list] = None  # optional — populated from preview-columns step
    # Optional transformations alongside the rename. All admin-curated.
    post_processors: Optional[list] = None
    value_maps: Optional[dict] = None
    row_routing: Optional[dict] = None


@router.post("/mappings/manual")
def create_manual_mapping(
    body: ManualMappingRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Save a human-created column mapping as immediately approved.
    Supersedes any existing approved mapping for the same source.

    Optional post_processors / value_maps / row_routing fields carry
    transformations beyond the simple rename — used to express the BookPage
    split, DocType normalization, and DocType→bucket routing for sources
    that fan out into multiple downstream signals (e.g. clerk ORI exports).
    """
    from datetime import datetime, timezone

    source = db.query(CountySource).filter_by(id=body.source_id).first()
    if not source:
        raise HTTPException(status_code=404, detail=f"Source {body.source_id} not found")

    now = datetime.now(timezone.utc)
    row = CountyColumnMapping(
        source_id=body.source_id,
        source_columns=sorted(body.mapping.keys()),
        mapping=body.mapping,
        is_approved=True,
        mapped_by="human",
        approved_by=_admin.get("sub"),
        approved_at=now,
        sample_rows=body.sample_rows,
        post_processors=body.post_processors,
        value_maps=body.value_maps,
        row_routing=body.row_routing,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    logger.info(
        "[Admin] Manual mapping saved: id=%s source_id=%s by %s pp=%s vmap=%s routing=%s",
        row.id, row.source_id, _admin.get("sub"),
        body.post_processors is not None,
        body.value_maps is not None,
        body.row_routing is not None,
    )
    return _mapping_to_dict(row)
