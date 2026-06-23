"""
Forced Action — FastAPI application.

Endpoints:
    GET  /health                   — Health check (UptimeRobot / load balancer)
    POST /webhooks/stripe          — Stripe event receiver
    GET  /api/founding-spots       — Founding countdown for landing page
    GET  /api/zip-check            — ZIP availability checker for landing page
    POST /api/checkout             — Create Stripe checkout session
    GET  /api/feed/{uuid}          — Event Feed for subscribers (paginated leads, sort, search, filter)
    GET  /api/feed/{uuid}/stats    — Aggregate stats for the subscriber's feed
    POST /api/resend-confirmation  — Re-send welcome/confirmation email by feed_uuid
    GET  /                         — Landing page
"""

import functools
import json
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests as _requests
import stripe
from fastapi import FastAPI, Header, HTTPException, Request, Depends, Query, Response
from fastapi.exception_handlers import http_exception_handler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from pydantic import BaseModel, Field, field_validator, model_validator, EmailStr
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_, desc, func, cast, text, Date, distinct, update

from src.core.database import get_db_context
from src.core.models import ConsentAcceptance, FoundingSubscriberCount, ZipTerritory, Subscriber, Property, DistressScore, Incident, LeadPackPurchase, ScraperRunStats, EnrichedContact, Owner, SentLead, WaitlistEntry, SmsOptIn, ExpansionCandidate, County, LeadExclusivity
from src.services.stripe_webhooks import handle_webhook
from src.services.stripe_service import get_price_id_for_checkout, _price_ids
from src.services import lead_exclusivity
from config.settings import get_settings
from config.scoring import VERTICAL_WEIGHTS, for_county
from config.constants import TIER_DISPLAY
from src.utils.logger import setup_logging
from src.services.rate_limit import enforce_or_429
from src.services.phone_utils import normalize as normalize_phone
from src.api.deps import (
    get_db,
    VALID_TIERS,
    VALID_VERTICALS,
    ZIP_RE as _ZIP_RE,
    FLORIDA_PREFIXES as _FLORIDA_PREFIXES,
    ConsentAcceptanceRequest,
    resolve_phone_with_quality as _resolve_phone_with_quality,
    estimate_lead_job_value as _estimate_lead_job_value,
    visible_tier_fields as _visible_tier_fields,
)


# Load config/logging.yaml so every logger.info/warning/error across src/* is
# visible in the uvicorn console (instead of just uvicorn's access logs).
setup_logging()

logger = logging.getLogger(__name__)



# VALID_TIERS and VALID_VERTICALS imported from src.api.deps


app = FastAPI(title="Forced Action API", version="1.0.0")
_s = get_settings()
_cors_origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
if _s.wl_frontend_base_url and _s.wl_frontend_base_url not in _cors_origins:
    _cors_origins.append(_s.wl_frontend_base_url)
_cors_origins.extend(o.strip() for o in _s.wl_allowed_origins.split(",") if o.strip())
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(dict.fromkeys(_cors_origins)),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def affiliate_ref_cookie(request, call_next):
    """Capture an inbound ?aff= affiliate token into a persistent cookie.

    Fallback only — the SPA captures ?aff client-side and forwards it in the
    signup body. Uses ?aff (not ?ref, which the peer referral loop owns).
    Last-touch wins. Validation against a real Affiliate happens at signup.
    """
    from src.services.affiliate_engine import (
        AFFILIATE_COOKIE_NAME,
        AFFILIATE_COOKIE_MAX_AGE,
        AFFILIATE_REF_MAX_LEN,
    )
    ref = request.query_params.get("aff")
    response = await call_next(request)
    if ref:
        response.set_cookie(
            key=AFFILIATE_COOKIE_NAME,
            value=ref[:AFFILIATE_REF_MAX_LEN],
            max_age=AFFILIATE_COOKIE_MAX_AGE,
            httponly=True,
            samesite="lax",
            path="/",
        )
    return response
from src.api.admin_router import router as admin_router, get_current_admin  # noqa: E402
from src.api.attribution_router import router as attribution_router  # noqa: E402
from src.api.cora_incidents_router import router as cora_incidents_router  # noqa: E402
from src.api.sms_analytics_router import router as sms_analytics_router  # noqa: E402
from src.api.operator_crm_router import router as operator_crm_router  # noqa: E402
from src.api.closer_router import router as closer_router  # noqa: E402
app.include_router(admin_router)
app.include_router(attribution_router)
app.include_router(cora_incidents_router)
app.include_router(sms_analytics_router)
app.include_router(operator_crm_router)
app.include_router(closer_router)

from src.api.chat_router import router as chat_router  # noqa: E402
app.include_router(chat_router)

from src.api.metrics_router import router as metrics_router  # noqa: E402
from src.api.alert_webhook_router import router as alert_webhook_router  # noqa: E402
app.include_router(metrics_router)
app.include_router(alert_webhook_router)

from src.api.bankruptcy_alert_router import router as bankruptcy_alert_router  # noqa: E402
app.include_router(bankruptcy_alert_router)

from src.api.email_campaign_router import router as email_campaign_router  # noqa: E402
app.include_router(email_campaign_router)

from src.api.subscriber_router import router as subscriber_router  # noqa: E402
from src.services.subscriber_auth import get_current_subscriber  # noqa: E402
app.include_router(subscriber_router)

from src.api.white_label_router import router as wl_router, admin_wl_router  # noqa: E402
app.include_router(wl_router)
app.include_router(admin_wl_router)

from src.api.icp_channel_router import router as icp_channel_router  # noqa: E402
app.include_router(icp_channel_router)

from src.api.supplier_intel_router import router as supplier_intel_router  # noqa: E402
app.include_router(supplier_intel_router)

from src.api.clay_router import router as clay_router  # noqa: E402
app.include_router(clay_router)

from src.api.dfy_lite_router import router as dfy_lite_router  # noqa: E402
app.include_router(dfy_lite_router)

from src.api.quora_auth_router import router as quora_auth_router  # noqa: E402
app.include_router(quora_auth_router)
from src.api.signals_router import router as signals_router  # noqa: E402
app.include_router(signals_router)


# ---------------------------------------------------------------------------
# Global exception handlers

# ---------------------------------------------------------------------------

@app.exception_handler(SQLAlchemyError)
async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError):
    logger.error("Database error on %s %s", request.method, request.url.path, exc_info=exc)
    return JSONResponse(
        status_code=503,
        content={"error": "service_unavailable", "message": "Database temporarily unavailable"},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled error on %s %s", request.method, request.url.path, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "message": "An unexpected error occurred"},
    )


# ---------------------------------------------------------------------------
# GET /health — Fast uptime check (UptimeRobot / load balancer)
# GET /health/detailed — Full ops health check (Stripe, GHL, scrapers, enrichment)
# ---------------------------------------------------------------------------

@app.get("/health", include_in_schema=False)
def health_check(db: Session = Depends(get_db)):
    """Fast check: DB connectivity only. Used by UptimeRobot and load balancers."""
    try:
        db.execute(select(1))
    except Exception:
        raise HTTPException(status_code=503, detail="db_unavailable")
    return {"status": "ok"}


@app.get("/health/detailed", include_in_schema=False)
def health_check_detailed(db: Session = Depends(get_db)):
    """
    Full ops health check. Returns status for every integrated subsystem.

    Response shape:
      {
        "status": "ok" | "degraded" | "critical",
        "checks": {
          "database":      {"status": "ok"|"error", "detail": ...},
          "stripe":        {"status": "ok"|"unconfigured"|"error", "detail": ...},
          "ghl":           {"status": "ok"|"unconfigured"|"error", "detail": ...},
          "smtp":          {"status": "ok"|"unconfigured"},
          "enrichment":    {"status": "ok"|"unconfigured"|"stale", "last_run": ..., "detail": ...},
          "scrapers":      {"status": "ok"|"stale"|"failures", "last_run": ..., "failed": [...]},
          "scoring":       {"status": "ok"|"stale", "last_scored": ..., "scored_properties": ...},
          "config":        {"status": "ok"|"warnings", "missing_optional": [...]},
        },
        "checked_at": "<ISO timestamp>"
      }

    HTTP 200 for ok/degraded (non-critical issues), 503 only for critical (DB down).
    """
    settings = get_settings()
    checks = {}
    overall = "ok"
    checked_at = datetime.now(timezone.utc).isoformat()

    # ── 1. Database ────────────────────────────────────────────────────────
    try:
        db.execute(select(1))
        checks["database"] = {"status": "ok"}
    except Exception:
        checks["database"] = {"status": "error"}
        # DB down is always critical — return 503 immediately
        return JSONResponse(
            status_code=503,
            content={
                "status": "critical",
                "checks": checks,
                "checked_at": checked_at,
            },
        )

    # ── 2. Stripe API ──────────────────────────────────────────────────────
    if not settings.active_stripe_secret_key:
        checks["stripe"] = {"status": "unconfigured"}
        overall = "degraded"
    else:
        try:
            stripe.api_key = settings.active_stripe_secret_key.get_secret_value()
            # Lightweight call — just fetch account balance
            stripe.Balance.retrieve()
            checks["stripe"] = {"status": "ok"}
        except stripe.error.AuthenticationError:
            checks["stripe"] = {"status": "error", "detail": "invalid_api_key"}
            overall = "degraded"
        except stripe.error.StripeError:
            checks["stripe"] = {"status": "error", "detail": "stripe_api_error"}
            overall = "degraded"
        except Exception:
            checks["stripe"] = {"status": "error", "detail": "unreachable"}
            overall = "degraded"

    # ── 3. GoHighLevel API ─────────────────────────────────────────────────
    if not settings.ghl_api_key or not settings.ghl_location_id:
        checks["ghl"] = {"status": "unconfigured"}
        overall = "degraded"
    else:
        try:
            t0 = time.monotonic()
            resp = _requests.get(
                f"https://services.leadconnectorhq.com/locations/{settings.ghl_location_id}",
                headers={
                    "Authorization": f"Bearer {settings.ghl_api_key.get_secret_value()}",
                    "Version": "2021-07-28",
                },
                timeout=8,
            )
            latency_ms = round((time.monotonic() - t0) * 1000)
            if resp.status_code == 200:
                checks["ghl"] = {"status": "ok", "latency_ms": latency_ms}
            elif resp.status_code == 401:
                checks["ghl"] = {"status": "error", "detail": "invalid_api_key"}
                overall = "degraded"
            else:
                checks["ghl"] = {"status": "error", "detail": f"http_{resp.status_code}"}
                overall = "degraded"
        except _requests.exceptions.Timeout:
            checks["ghl"] = {"status": "error", "detail": "timeout"}
            overall = "degraded"
        except Exception:
            checks["ghl"] = {"status": "error", "detail": "unreachable"}
            overall = "degraded"

    # ── 4. SMTP / Email ────────────────────────────────────────────────────
    if settings.smtp_host and settings.smtp_user:
        checks["smtp"] = {
            "status": "ok",
            "host": settings.smtp_host,
            "alert_email": settings.alert_email or "not_set",
        }
    else:
        checks["smtp"] = {"status": "unconfigured"}
        # SMTP unconfigured is a warning, not degraded — alerts won't fire

    # ── 5. Enrichment pipeline ─────────────────────────────────────────────
    if not settings.batch_skip_tracing_api_key:
        checks["enrichment"] = {"status": "unconfigured", "detail": "BATCH_SKIP_TRACING_API_KEY not set"}
        overall = "degraded"
    else:
        try:
            last_enriched = db.execute(
                select(func.max(EnrichedContact.enriched_at))
            ).scalar()

            if last_enriched is None:
                checks["enrichment"] = {"status": "ok", "last_run": None, "detail": "no_runs_yet"}
            else:
                last_enriched_utc = last_enriched.replace(tzinfo=timezone.utc) if last_enriched.tzinfo is None else last_enriched
                hours_ago = (datetime.now(timezone.utc) - last_enriched_utc).total_seconds() / 3600
                # Allow up to 72h on Sat/Sun/Mon — enrichment only runs weekdays
                _off_cycle = date.today().weekday() in (0, 5, 6)
                stale_threshold = 72 if _off_cycle else 26
                status = "ok" if hours_ago < stale_threshold else "stale"
                if status == "stale":
                    overall = "degraded"
                checks["enrichment"] = {
                    "status": status,
                    "last_run": last_enriched_utc.isoformat(),
                    "hours_ago": round(hours_ago, 1),
                    "idi_configured": bool(settings.idi_api_key),
                }
        except Exception:
            checks["enrichment"] = {"status": "error"}
            overall = "degraded"

    # ── 6. Scraper pipeline ────────────────────────────────────────────────
    try:
        cutoff = date.today() - timedelta(days=2)

        last_run_date = db.execute(
            select(func.max(ScraperRunStats.run_date))
            .where(ScraperRunStats.run_success == True)    # noqa: E712
        ).scalar()

        failed_scrapers = db.execute(
            select(ScraperRunStats.source_type, ScraperRunStats.error_message, ScraperRunStats.run_date)
            .where(
                ScraperRunStats.run_success == False,      # noqa: E712
                ScraperRunStats.run_date >= cutoff,
            )
            .order_by(ScraperRunStats.run_date.desc())
        ).all()

        if last_run_date is None:
            scraper_status = "ok"
            scraper_detail = {"last_run": None, "detail": "no_runs_recorded_yet"}
        else:
            days_ago = (date.today() - last_run_date).days
            # Allow 3 days on Sat/Sun/Mon — scrapers only run weekdays
            _off_cycle = date.today().weekday() in (0, 5, 6)
            stale_days = 3 if _off_cycle else 1
            scraper_status = "ok" if days_ago <= stale_days else "stale"
            if scraper_status == "stale":
                overall = "degraded"
            scraper_detail = {
                "last_run": last_run_date.isoformat(),
                "days_ago": days_ago,
            }

        if failed_scrapers:
            scraper_status = "failures"
            overall = "degraded"
            scraper_detail["failed"] = [
                {"source": r.source_type, "date": r.run_date.isoformat()}
                for r in failed_scrapers
            ]

        # Zero-row check — scraper ran and succeeded but returned nothing (silent data gap)
        zero_row_scrapers = db.execute(
            select(ScraperRunStats.source_type, ScraperRunStats.run_date)
            .where(
                ScraperRunStats.run_success == True,       # noqa: E712
                ScraperRunStats.total_scraped == 0,
                ScraperRunStats.run_date >= cutoff,
            )
            .order_by(ScraperRunStats.run_date.desc())
        ).all()

        if zero_row_scrapers:
            scraper_detail["zero_rows"] = [
                {"source": r.source_type, "date": r.run_date.isoformat()}
                for r in zero_row_scrapers
            ]
            if scraper_status == "ok":
                scraper_status = "zero_rows"
            overall = "degraded"

        checks["scrapers"] = {"status": scraper_status, **scraper_detail}

    except Exception:
        checks["scrapers"] = {"status": "error"}
        overall = "degraded"

    # ── 7. Scoring pipeline ────────────────────────────────────────────────
    try:
        last_scored = db.execute(
            select(func.max(DistressScore.score_date))
        ).scalar()

        scored_count = db.execute(
            select(func.count(DistressScore.id.distinct()))
            .where(DistressScore.score_date == last_scored)
        ).scalar() if last_scored else 0

        if last_scored is None:
            checks["scoring"] = {"status": "ok", "last_scored": None, "detail": "no_scores_yet"}
        else:
            # score_date may be datetime or date depending on DB driver — normalise to date
            last_scored_date = last_scored.date() if isinstance(last_scored, datetime) else last_scored
            days_ago = (date.today() - last_scored_date).days
            # Allow 3 days on Sat/Sun/Mon — scoring only runs weekdays
            _off_cycle = date.today().weekday() in (0, 5, 6)
            stale_days = 3 if _off_cycle else 1
            scoring_status = "ok" if days_ago <= stale_days else "stale"
            if scoring_status == "stale":
                overall = "degraded"
            checks["scoring"] = {
                "status": scoring_status,
                "last_scored": last_scored_date.isoformat(),
                "days_ago": days_ago,
                "scored_today": scored_count,
            }
    except Exception:
        checks["scoring"] = {"status": "error"}
        overall = "degraded"

    # ── 8. Config completeness ─────────────────────────────────────────────
    missing_optional = []
    if not settings.ghl_api_key:
        missing_optional.append("GHL_API_KEY")
    if not settings.active_stripe_secret_key:
        missing_optional.append("STRIPE_SECRET_KEY" if not settings.stripe_test_mode else "STRIPE_TEST_SECRET_KEY")
    if not settings.batch_skip_tracing_api_key:
        missing_optional.append("BATCH_SKIP_TRACING_API_KEY")
    if not settings.smtp_host:
        missing_optional.append("SMTP_HOST")
    if not settings.alert_email:
        missing_optional.append("ALERT_EMAIL")
    if not settings.oxylabs_username:
        missing_optional.append("OXYLABS_USERNAME")

    checks["config"] = {
        "status": "warnings" if missing_optional else "ok",
        "missing_optional": missing_optional,
    }

    return JSONResponse(
        status_code=503 if overall == "critical" else 200,
        content={
            "status": overall,
            "checks": checks,
            "checked_at": checked_at,
        },
    )

\
# ---------------------------------------------------------------------------

_PRICING_CACHE_KEY = "pricing:stripe_prices"
_PRICING_TTL = 86_400  # 24 hours


@functools.lru_cache(maxsize=1)
def _fetch_pricing_from_stripe() -> dict:
    """Fetch all tier prices from Stripe. lru_cache = per-process fallback when Redis is down."""
    _s = get_settings()
    stripe.api_key = _s.active_stripe_secret_key.get_secret_value()
    all_prices = _price_ids()

    pricing_info = {}
    for tier in ("starter", "pro", "dominator"):
        founding_id = all_prices.get(tier, {}).get("founding")
        regular_id = all_prices.get(tier, {}).get("regular")

        founding_amount = None
        regular_amount = None
        currency = "usd"

        try:
            if founding_id:
                p = stripe.Price.retrieve(founding_id)
                if p.unit_amount is not None:
                    founding_amount = p.unit_amount // 100
                currency = p.currency
            if regular_id:
                p = stripe.Price.retrieve(regular_id)
                if p.unit_amount is not None:
                    regular_amount = p.unit_amount // 100
                currency = p.currency
        except Exception as e:
            logger.error("Error retrieving Stripe price for tier '%s': %s", tier, e, exc_info=True)

        pricing_info[tier] = {
            "founding_amount": founding_amount,
            "regular_amount": regular_amount,
            "currency": currency,
            **TIER_DISPLAY[tier],
        }

    return pricing_info


def _cached_pricing_info() -> dict:
    """Return pricing, served from Redis (24 h TTL) with per-process lru_cache as fallback."""
    from src.core.redis_client import redis_available, rget, rset

    if redis_available():
        cached = rget(_PRICING_CACHE_KEY)
        if cached:
            return json.loads(cached)

    result = _fetch_pricing_from_stripe()

    if redis_available():
        rset(_PRICING_CACHE_KEY, json.dumps(result), ttl_seconds=_PRICING_TTL)

    return result


@app.get("/api/pricing")
def get_pricing_info():
    """Returns pricing config for the landing page.

    Founding + regular dollar amounts come from Stripe Price objects (via the
    STRIPE_PRICE_{TIER}_{FOUNDING|REGULAR} env vars). Display copy (label,
    zip_limit, features) is owned by TIER_DISPLAY above. The frontend reads
    this once at LandingPage mount and passes it through LandingContext.
    """
    return {"pricing": _cached_pricing_info()}


# ConsentAcceptanceRequest imported from src.api.deps

# ---------------------------------------------------------------------------
# POST /api/checkout — Create Stripe checkout session
# ---------------------------------------------------------------------------

# ── Meta Ads attribution → Stripe metadata (S2) ──────────────────────────────
# Attribution keys forwarded from the frontend localStorage store into Stripe
# metadata so the webhook can stamp the subscriber + fire the Meta CAPI Purchase
# event. The buyer's IP/User-Agent must be captured HERE (the buyer's request),
# never from the Stripe webhook request (that is Stripe's server).
_ATTRIBUTION_META_KEYS = (
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "campaign_id", "attribution_token", "fbclid", "ref",
)
# Stripe caps each metadata value at 500 chars.
_MAX_META_VALUE_LEN = 480


def _attribution_stripe_metadata(request: Request, attribution: Optional[dict]) -> dict:
    """Build a compact, non-empty Stripe-metadata dict from buyer attribution.

    Captures buyer IP + User-Agent from the buyer's own request and merges the
    attribution values the frontend forwarded. Empty values are dropped; the
    user-agent is truncated to stay under Stripe's 500-char metadata limit.
    """
    out: dict = {}
    attribution = attribution or {}
    for key in _ATTRIBUTION_META_KEYS:
        value = attribution.get(key)
        if value:
            out[key] = str(value)[:_MAX_META_VALUE_LEN]

    buyer_ip = _client_ip_for_waitlist(request)
    if buyer_ip and buyer_ip != "unknown":
        out["buyer_ip"] = buyer_ip[:_MAX_META_VALUE_LEN]
    user_agent = request.headers.get("user-agent")
    if user_agent:
        out["buyer_user_agent"] = user_agent[:_MAX_META_VALUE_LEN]
    return out


class CheckoutRequest(BaseModel):
    tier: str        # starter | pro | dominator
    vertical: str    # roofing | remediation | investor
    county_id: str   # hillsborough
    zip_codes: list[str] = []  # ZIP territories to lock on purchase
    email: str       # collected before checkout — used to block duplicate subscriptions
    consent_acceptance: Optional[ConsentAcceptanceRequest] = None
    attribution: Optional[dict] = None  # Meta Ads attribution (utm_*, campaign_id, fbclid, ...)

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.lower().strip()
        if not v or "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError("A valid email address is required")
        return v

    @field_validator("tier")
    @classmethod
    def validate_tier(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in VALID_TIERS:
            raise ValueError(f"Invalid tier '{v}'. Must be one of: {sorted(VALID_TIERS)}")
        return v

    @field_validator("vertical")
    @classmethod
    def validate_vertical(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in VALID_VERTICALS:
            raise ValueError(f"Invalid vertical '{v}'. Must be one of: {sorted(VALID_VERTICALS)}")
        return v

    @field_validator("county_id")
    @classmethod
    def validate_county_id(cls, v: str) -> str:
        v = v.lower().strip()
        if not v:
            raise ValueError("county_id is required")
        return v

    @field_validator("zip_codes")
    @classmethod
    def validate_zip_codes(cls, v: list) -> list:
        if not v:
            raise ValueError("At least one ZIP code must be selected before checkout")
        return v

    @model_validator(mode="after")
    def validate_zip_count(self) -> "CheckoutRequest":
        limits = {"starter": 1, "pro": 3, "dominator": 10}
        limit = limits.get(self.tier)
        if limit and len(self.zip_codes) != limit:
            raise ValueError(f"{self.tier.title()} plan requires exactly {limit} ZIP code{'s' if limit > 1 else ''}.")
        return self


@app.post("/api/checkout")
def create_checkout(payload: CheckoutRequest, request: Request, db: Session = Depends(get_db)):

    _s = get_settings()
    stripe.api_key = _s.active_stripe_secret_key.get_secret_value()

    try:
        price_id, is_founding = get_price_id_for_checkout(db, payload.tier, payload.vertical, payload.county_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": "invalid_configuration", "message": str(e)})
    except OperationalError:
        logger.error("DB error resolving price for checkout", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not price_id:
        raise HTTPException(
            status_code=400,
            detail={"error": "price_not_configured", "message": f"No price configured for tier '{payload.tier}'"},
        )

    # Block duplicate PAID subscriptions — check before creating Stripe session
    # so the user is never charged twice. Free-tier rows (created via
    # /api/free-signup as part of the Phase 2B ladder) are allowed through;
    # the checkout webhook upgrades the existing row in place.
    try:
        existing_sub = db.execute(
            select(Subscriber).where(
                Subscriber.email == payload.email,
                Subscriber.vertical == payload.vertical,
                Subscriber.county_id == payload.county_id,
                Subscriber.status.in_(["active", "grace"]),
                Subscriber.tier != "free",
            )
        ).scalar_one_or_none()
    except OperationalError:
        logger.error("DB error checking existing subscriber at checkout", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if existing_sub:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "already_subscribed",
                "message": (
                    f"{payload.email} already has an active {payload.vertical.replace('_', ' ').title()} "
                    "subscription. Log in to manage your existing subscription."
                ),
            },
        )

    # Validate ZIP availability before taking payment.
    # Without this check a subscriber can pay, the webhook fires, and any ZIPs that
    # were locked between zip-check and payment completion are silently dropped —
    # leaving them with fewer territories than they paid for.
    try:
        taken_zips = db.execute(
            select(ZipTerritory.zip_code).where(
                ZipTerritory.zip_code.in_(payload.zip_codes),
                ZipTerritory.vertical == payload.vertical,
                ZipTerritory.county_id == payload.county_id,
                ZipTerritory.status == "locked",
            )
        ).scalars().all()
    except OperationalError:
        logger.error("DB error checking ZIP availability at checkout", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if taken_zips:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "zips_unavailable",
                "message": (
                    f"ZIP code(s) {', '.join(sorted(taken_zips))} are no longer available. "
                    "Please go back and select different ZIP codes."
                ),
                "unavailable_zips": sorted(taken_zips),
            },
        )

    if payload.consent_acceptance and payload.consent_acceptance.terms_accepted:
        try:
            from datetime import datetime

            def _parse_iso_co(s):
                if not s:
                    return None
                try:
                    return datetime.fromisoformat(s.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    return None

            _co_tcpa = bool(payload.consent_acceptance.tcpa_accepted)
            ca = ConsentAcceptance(
                email=payload.email,
                terms_version=payload.consent_acceptance.terms_version or "2026.06",
                privacy_version=payload.consent_acceptance.privacy_version or "2026.06",
                accepted_at=datetime.now(timezone.utc),
                source_flow="checkout",
                user_agent=payload.consent_acceptance.user_agent,
                modal_opened_at=_parse_iso_co(payload.consent_acceptance.modal_opened_at),
                modal_scrolled_to_end_at=_parse_iso_co(payload.consent_acceptance.modal_scrolled_to_end_at),
                accepted_text_hash=payload.consent_acceptance.accepted_text_hash or "",
                tcpa_consent_text=payload.consent_acceptance.tcpa_consent_text if _co_tcpa else None,
                tcpa_consent_version=payload.consent_acceptance.tcpa_consent_version if _co_tcpa else None,
                tcpa_checked_at=datetime.now(timezone.utc) if _co_tcpa else None,
                consent_scope="marketing" if _co_tcpa else None,
                not_condition_of_purchase_ack=_co_tcpa or None,
                county_id=payload.county_id,
            )
            db.add(ca)
            db.commit()
        except Exception:
            logger.warning("ConsentAcceptance write failed in checkout (non-fatal):", exc_info=True)

    checkout_metadata = {
        "tier": payload.tier,
        "vertical": payload.vertical,
        "county_id": payload.county_id,
        "is_founding": str(is_founding),
        "founding_price_id": price_id if is_founding else "",
        "zip_codes": ",".join(payload.zip_codes),
    }
    # Meta Ads attribution + buyer IP/UA captured from the buyer's request.
    checkout_metadata.update(_attribution_stripe_metadata(request, payload.attribution))

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            ui_mode="embedded",
            customer_email=payload.email,   # pre-fills email in Stripe form
            line_items=[{"price": price_id, "quantity": 1}],
            metadata=checkout_metadata,
            return_url=f"{_s.app_base_url}/success?session_id={{CHECKOUT_SESSION_ID}}",
        )
    except stripe.error.CardError as e:
        logger.warning("Stripe card error: %s", e.user_message)
        raise HTTPException(
            status_code=402,
            detail={"error": "card_error", "message": e.user_message or "Card declined"},
        )
    except stripe.error.InvalidRequestError as e:
        logger.warning("Stripe invalid request: %s", str(e))
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": "Invalid checkout parameters"},
        )
    except stripe.error.AuthenticationError:
        logger.error("Stripe authentication failed — check STRIPE_SECRET_KEY", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail={"error": "payment_unavailable", "message": "Payment service temporarily unavailable"},
        )
    except stripe.error.RateLimitError:
        logger.warning("Stripe rate limit hit")
        raise HTTPException(
            status_code=429,
            detail={"error": "rate_limited", "message": "Too many requests — please retry shortly"},
        )
    except stripe.error.StripeError as e:
        logger.error("Stripe error during checkout: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=502,
            detail={"error": "payment_gateway_error", "message": "Payment gateway error — please try again"},
        )

    return {"client_secret": session.client_secret, "is_founding": is_founding}


# ---------------------------------------------------------------------------
# GET /api/checkout-status — Payment verification for the success page
# ---------------------------------------------------------------------------

@app.get("/api/checkout-status")
def checkout_status(session_id: str, db: Session = Depends(get_db)):
    """
    Called by success.html after embedded checkout redirects to /success.
    Returns the actual payment outcome so the page can show the right state
    instead of always celebrating.

    Returns:
        payment_status: "paid" | "unpaid" | "pending"
        feed_uuid: subscriber's event feed UUID if payment succeeded
        tier: subscription tier
    """
    from src.services.stripe_webhooks import _init_stripe
    if not _init_stripe():
        raise HTTPException(status_code=503, detail="Stripe not configured")

    try:
        session = stripe.checkout.Session.retrieve(session_id)
    except stripe.error.InvalidRequestError:
        raise HTTPException(status_code=404, detail="Session not found")
    except stripe.error.StripeError as exc:
        logger.error("checkout-status: Stripe error retrieving session %s: %s", session_id, exc)
        raise HTTPException(status_code=502, detail="Stripe unavailable")

    payment_status = session.get("payment_status")  # "paid" | "unpaid" | "no_payment_required"
    stripe_customer_id = session.get("customer")

    # For subscriptions that are still processing (incomplete but not yet failed)
    # tell the frontend to keep polling briefly.
    if payment_status not in ("paid", "unpaid", "no_payment_required"):
        return {"payment_status": "pending", "feed_uuid": None, "tier": None}

    feed_uuid = None
    tier = session.get("metadata", {}).get("tier")

    if payment_status == "paid" and stripe_customer_id:
        sub = db.execute(
            select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
        ).scalar_one_or_none()
        if sub:
            feed_uuid = sub.event_feed_uuid

    return {"payment_status": payment_status, "feed_uuid": feed_uuid, "tier": tier}



# ---------------------------------------------------------------------------
# POST /api/portal-session — Create Stripe billing portal session
# ---------------------------------------------------------------------------

class PortalSessionRequest(BaseModel):
    feed_uuid: str


@app.post("/api/portal-session")
def create_portal_session(req: PortalSessionRequest, db: Session = Depends(get_db)):
    """Create a Stripe billing portal session so subscribers can update card / cancel."""
    settings = get_settings()
    if not settings.active_stripe_secret_key:
        raise HTTPException(status_code=503, detail="Billing portal not configured")

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)
    ).scalar_one_or_none()

    if not subscriber or not subscriber.stripe_customer_id:
        raise HTTPException(status_code=404, detail="Subscriber not found")

    stripe.api_key = settings.active_stripe_secret_key.get_secret_value()
    try:
        session = stripe.billing_portal.Session.create(
            customer=subscriber.stripe_customer_id,
            return_url=f"{settings.app_base_url}/dashboard/{req.feed_uuid}",
        )
        return {"url": session.url}
    except stripe.StripeError as exc:
        logger.error("Stripe portal session error for subscriber %s: %s", subscriber.id, exc)
        raise HTTPException(status_code=502, detail="Could not create billing portal session")


# ---------------------------------------------------------------------------
# POST /api/log-event — Client-side event logger (cancel confirm / abort)
# ---------------------------------------------------------------------------

class LogEventRequest(BaseModel):
    event: str      # e.g. "cancel_confirm", "cancel_abort"
    feed_uuid: str


@app.post("/api/log-event")
def log_client_event(req: LogEventRequest):
    """Log a subscriber UI event (cancel modal clicks) for audit trail."""
    logger.info("CLIENT EVENT: event=%s feed_uuid=%s", req.event, req.feed_uuid)
    return {"ok": True}


# ---------------------------------------------------------------------------
# POST /webhooks/stripe
# ---------------------------------------------------------------------------

@app.post("/webhooks/stripe", status_code=200)
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(None, alias="stripe-signature"),
):
    if not stripe_signature:
        raise HTTPException(
            status_code=400,
            detail={"error": "missing_signature", "message": "Missing stripe-signature header"},
        )

    raw_body = await request.body()

    with get_db_context() as db:
        try:
            handle_webhook(raw_body, stripe_signature, db)
        except ValueError as e:
            logger.warning("Webhook signature/payload rejected: %s", str(e))
            raise HTTPException(
                status_code=400,
                detail={"error": "webhook_invalid", "message": str(e)},
            )
        except OperationalError:
            logger.error("DB error processing webhook", exc_info=True)
            raise HTTPException(
                status_code=503,
                detail={"error": "service_unavailable", "message": "Database temporarily unavailable"},
            )
        except Exception as exc:
            logger.error("Unhandled webhook handler error", exc_info=True)
            from src.services.email import send_alert
            send_alert(
                "[FA] Stripe webhook error",
                f"Unhandled exception in /webhooks/stripe:\n\n{exc}",
            )
            raise HTTPException(
                status_code=500,
                detail={"error": "internal_server_error", "message": "Webhook processing failed"},
            )

    return {"status": "ok"}


# ---------------------------------------------------------------------------
# POST /webhooks/aircall — Closer Cockpit call capture (Sprint S1b)
# Events: call.ended, transcription.created, sentiment.created, topics.created.
# HMAC-verified, returns 200 fast; updates closer_calls and (on transcript)
# publishes a Cora event for tagging.
# ---------------------------------------------------------------------------

def _verify_aircall_signature(raw_body: bytes, signature) -> bool:
    """HMAC-SHA256(raw_body, webhook_token) == X-Aircall-Signature.

    NOTE: confirm Aircall's exact signing scheme at first live integration.
    """
    import hmac
    import hashlib

    s = get_settings()
    token = s.aircall_webhook_token.get_secret_value() if s.aircall_webhook_token else None
    if not token:
        logger.error("[aircall] webhook token not configured — rejecting event")
        return False
    if not signature:
        return False
    expected = hmac.new(token.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def _aircall_epoch_to_dt(value):
    from datetime import datetime, timezone
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc) if value else None
    except (TypeError, ValueError, OverflowError):
        return None


def _aircall_match_subscriber_by_phone(db, dialed_e164) -> "Optional[int]":
    """Fallback subscriber match (last 10 digits) when no pending row exists."""
    from sqlalchemy import text as _text

    if not dialed_e164:
        return None
    last10 = "".join(ch for ch in dialed_e164 if ch.isdigit())[-10:]
    if len(last10) < 10:
        return None
    row = db.execute(
        _text(
            "SELECT id FROM subscribers "
            "WHERE right(regexp_replace(coalesce(phone,''), '\\D', '', 'g'), 10) = :last10 "
            "ORDER BY id LIMIT 1"
        ),
        {"last10": last10},
    ).first()
    return row[0] if row else None


def _handle_aircall_event(etype, data: dict, db) -> None:
    from datetime import datetime, timezone

    from src.core.models import CloserCall
    from src.services.phone_utils import normalize_closer as normalize
    from src.services import aircall_client

    call_id = str(data.get("id") or data.get("call_id") or "").strip()
    if not call_id:
        return

    row = db.execute(
        select(CloserCall).where(CloserCall.aircall_call_id == call_id)
    ).scalar_one_or_none()

    if etype == "call.ended":
        dialed = normalize(data.get("raw_digits") or data.get("to"))
        if row is None:
            sub_id = _aircall_match_subscriber_by_phone(db, dialed)
            if sub_id is None:
                logger.warning("[aircall] call.ended unmatched call_id=%s", call_id)
                return
            row = CloserCall(aircall_call_id=call_id, subscriber_id=sub_id)
            db.add(row)
        user = data.get("user") or {}
        row.direction = data.get("direction") or row.direction
        row.dialed_e164 = dialed or row.dialed_e164
        row.duration_sec = data.get("duration")
        row.started_at = _aircall_epoch_to_dt(data.get("started_at"))
        row.ended_at = _aircall_epoch_to_dt(data.get("ended_at"))
        if user.get("id"):
            row.closer_aircall_user_id = str(user.get("id"))
        if user.get("name"):
            row.closer_name = user.get("name")
        return

    if row is None:
        logger.info("[aircall] %s before row exists call_id=%s — skipping", etype, call_id)
        return

    if etype == "transcription.created":
        transcript = aircall_client.get_transcription(call_id)
        if transcript:
            row.transcript_text = transcript
            row.transcript_fetched_at = datetime.now(timezone.utc)
            db.flush()
            from src.agents.events.ingestion import publish_cora_event
            publish_cora_event({
                "event_type": "call_transcribed",
                "subscriber_id": row.subscriber_id,
                "payload": {"aircall_call_id": call_id},
            })
    elif etype == "sentiment.created":
        sentiment = aircall_client.get_sentiment(call_id)
        if sentiment:
            row.sentiment = sentiment
    elif etype == "topics.created":
        topics = aircall_client.get_topics(call_id)
        if topics is not None:
            row.topics = topics
    # other events ignored


@app.post("/webhooks/aircall", status_code=200)
async def aircall_webhook(
    request: Request,
    x_aircall_signature: str = Header(None, alias="X-Aircall-Signature"),
):
    raw_body = await request.body()
    if not _verify_aircall_signature(raw_body, x_aircall_signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    import json as _json
    try:
        event = _json.loads(raw_body or b"{}")
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid payload")

    etype = event.get("event")
    data = event.get("data") or {}

    try:
        with get_db_context() as db:
            _handle_aircall_event(etype, data, db)
    except OperationalError:
        logger.error("[aircall] DB error processing %s", etype, exc_info=True)
        raise HTTPException(status_code=503, detail="database temporarily unavailable")
    except Exception:
        # Signature already verified; log and return 200 so Aircall does not
        # disable the webhook on a transient handler error (reconciled later).
        logger.error("[aircall] handler error event=%s", etype, exc_info=True)

    return {"ok": True}


# ---------------------------------------------------------------------------
# POST /webhooks/stripe/white-label — WL-specific Stripe events (fa056)
# Uses a separate webhook secret so WL and core webhooks are independently
# rotatable. Falls back to the primary secret if WL secret is not configured.
# ---------------------------------------------------------------------------

@app.post("/webhooks/stripe/white-label", status_code=200)
async def stripe_wl_webhook(
    request: Request,
    stripe_signature: str = Header(None, alias="stripe-signature"),
):
    if not stripe_signature:
        raise HTTPException(400, detail="Missing stripe-signature header")

    raw_body = await request.body()
    s = get_settings()
    secret = s.wl_stripe_webhook_secret or s.stripe_webhook_secret
    if not secret:
        raise HTTPException(503, detail="WL Stripe webhook not configured")
    webhook_secret = secret.get_secret_value() if hasattr(secret, "get_secret_value") else secret

    try:
        event = stripe.Webhook.construct_event(raw_body, stripe_signature, webhook_secret)
    except stripe.error.SignatureVerificationError as exc:
        logger.warning("[wl_webhook] signature rejected: %s", exc)
        raise HTTPException(400, detail="Invalid webhook signature")

    from src.services.white_label_billing import handle_wl_webhook_event
    with get_db_context() as db:
        try:
            handle_wl_webhook_event(event, db)
        except Exception as exc:
            logger.error("[wl_webhook] unhandled error for %s: %s", event.type, exc, exc_info=True)
            raise HTTPException(500, detail="Webhook processing failed")

    return {"status": "ok"}


# ---------------------------------------------------------------------------
# GET /api/founding-summary — Total spots taken across all tiers for vertical
# ---------------------------------------------------------------------------

@app.get("/api/founding-summary")
def founding_summary(
    vertical: str = "roofing",
    county_id: str = "hillsborough",
    db: Session = Depends(get_db),
):
    """
    Returns total founding spots taken and remaining across all tiers
    for a given vertical/county. Used by the hero banner counter.
    """
    if vertical not in VALID_VERTICALS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"vertical must be one of: {sorted(VALID_VERTICALS)}"},
        )

    settings = get_settings()
    FOUNDING_CAP = settings.founding_spot_limit
    TIERS = ["starter", "pro", "dominator"]
    TOTAL_CAP = FOUNDING_CAP * len(TIERS)

    try:
        rows = db.execute(
            select(FoundingSubscriberCount).where(
                FoundingSubscriberCount.vertical == vertical,
                FoundingSubscriberCount.county_id == county_id,
            )
        ).scalars().all()
    except OperationalError:
        logger.error("DB error in founding-summary", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    total_taken = sum(r.count for r in rows)
    total_remaining = max(0, TOTAL_CAP - total_taken)

    return {
        "vertical": vertical,
        "county_id": county_id,
        "total_cap": TOTAL_CAP,
        "total_taken": total_taken,
        "total_remaining": total_remaining,
        "founding_available": total_remaining > 0,
    }


# ---------------------------------------------------------------------------
# GET /api/founding-spots
# ---------------------------------------------------------------------------

@app.get("/api/founding-spots")
def founding_spots(
    tier: str = "starter",
    vertical: str = "roofing",
    county_id: str = "hillsborough",
    db: Session = Depends(get_db),
):
    """
    Returns how many founding spots remain for a given tier/vertical/county.
    Landing page polls this every 60s to drive the countdown widget.
    """
    if tier not in VALID_TIERS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_tier", "message": f"tier must be one of: {sorted(VALID_TIERS)}"},
        )
    if vertical not in VALID_VERTICALS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"vertical must be one of: {sorted(VALID_VERTICALS)}"},
        )

    settings = get_settings()
    FOUNDING_CAP = settings.founding_spot_limit

    try:
        row = db.execute(
            select(FoundingSubscriberCount).where(
                FoundingSubscriberCount.tier == tier,
                FoundingSubscriberCount.vertical == vertical,
                FoundingSubscriberCount.county_id == county_id,
            )
        ).scalar_one_or_none()
    except OperationalError:
        logger.error("DB error in founding-spots", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    count = row.count if row else 0
    remaining = max(0, FOUNDING_CAP - count)

    return {
        "tier": tier,
        "vertical": vertical,
        "county_id": county_id,
        "founding_cap": FOUNDING_CAP,
        "founding_taken": count,
        "founding_remaining": remaining,
        "founding_available": remaining > 0,
    }


# ---------------------------------------------------------------------------
# GET /api/zip-check
# ---------------------------------------------------------------------------

# _ZIP_RE and _FLORIDA_PREFIXES imported from src.api.deps


@app.get("/api/zip-check")
def zip_check(
    zip_code: str,
    vertical: str = "roofing",
    county_id: str = "hillsborough",
    db: Session = Depends(get_db),
):
    """
    Returns availability status of a ZIP for a given vertical/county.
    Used by the landing page ZIP checker widget.
    """
    if vertical not in VALID_VERTICALS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"vertical must be one of: {sorted(VALID_VERTICALS)}"},
        )

    # Validate ZIP format and Florida prefix
    if not _ZIP_RE.match(zip_code) or not zip_code.startswith(_FLORIDA_PREFIXES):
        return {
            "zip_code": zip_code,
            "vertical": vertical,
            "status": "invalid",
            "message": "ZIP not found in Hillsborough County service area",
        }

    try:
        zip_exists = db.execute(
            select(func.count()).select_from(Property).where(
                Property.zip == zip_code,
                Property.county_id == county_id,
            )
        ).scalar()
    except OperationalError:
        logger.error("DB error in zip-check", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not zip_exists:
        return {
            "zip_code": zip_code,
            "vertical": vertical,
            "status": "invalid",
            "message": "ZIP not found in Hillsborough County service area",
        }

    try:
        territory = db.execute(
            select(ZipTerritory).where(
                ZipTerritory.zip_code == zip_code,
                ZipTerritory.vertical == vertical,
                ZipTerritory.county_id == county_id,
            )
        ).scalar_one_or_none()
    except OperationalError:
        logger.error("DB error fetching territory in zip-check", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if territory is None or territory.status == "available":
        return {"zip_code": zip_code, "vertical": vertical, "status": "available"}

    if territory.status == "grace":
        return {
            "zip_code": zip_code,
            "vertical": vertical,
            "status": "grace",
            "message": "Opening soon — join waitlist",
        }

    return {
        "zip_code": zip_code,
        "vertical": vertical,
        "status": "taken",
        "message": "This ZIP is locked by another subscriber",
    }


# ---------------------------------------------------------------------------
# GET /api/zip-availability — bulk ZIP availability for a vertical/county
# ---------------------------------------------------------------------------

@app.get("/api/zip-availability")
def zip_availability(
    vertical: str = "roofing",
    county_id: str = "hillsborough",
    db: Session = Depends(get_db),
):
    """
    Returns all ZIPs in a county with their availability status for a given vertical.
    Used by the ZIP selector UI to show a pickable grid instead of manual entry.

    Response: { zips: [ { zip_code, status, property_count, lead_count } ] }
    """
    if vertical not in VALID_VERTICALS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"vertical must be one of: {sorted(VALID_VERTICALS)}"},
        )

    from src.core.redis_client import redis_available, rget, rset
    _cache_key = f"zip_availability:{county_id}:{vertical}"
    _CACHE_TTL = 1800  # 30 minutes — data only changes when CDS runs at 07:00

    if redis_available():
        cached = rget(_cache_key)
        if cached:
            return json.loads(cached)


    try:
        # All distinct ZIPs with property counts
        zip_rows = db.execute(
            text("""
                SELECT p.zip, COUNT(*) AS prop_count
                FROM properties p
                WHERE p.county_id = :county_id
                  AND p.zip IS NOT NULL
                  AND LENGTH(p.zip) = 5
                GROUP BY p.zip
                ORDER BY p.zip
            """),
            {"county_id": county_id},
        ).fetchall()
    except OperationalError:
        logger.error("DB error in zip-availability (properties)", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable"})

    if not zip_rows:
        return {"vertical": vertical, "county_id": county_id, "zips": []}

    all_zips = [r[0] for r in zip_rows]
    prop_counts = {r[0]: r[1] for r in zip_rows}

    # Locked/grace territories for this vertical
    try:
        territory_rows = db.execute(
            select(ZipTerritory.zip_code, ZipTerritory.status, ZipTerritory.waitlist_emails).where(
                ZipTerritory.zip_code.in_(all_zips),
                ZipTerritory.vertical == vertical,
                ZipTerritory.county_id == county_id,
                ZipTerritory.status.in_(["locked", "grace"]),
            )
        ).fetchall()
    except OperationalError:
        logger.error("DB error in zip-availability (territories)", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable"})

    taken_map = {r[0]: r[1] for r in territory_rows}  # zip -> "locked" | "grace"
    waitlist_map = {r[0]: len(r[2]) if r[2] else 0 for r in territory_rows}

    # Gold+ lead counts per ZIP (deduped to latest score per property).
    # Without the latest-score subquery every historical scoring run is counted,
    # inflating counts by ~3-4x (one row per scoring run per property).
    try:
        lead_rows = db.execute(
            text("""
                SELECT p.zip, COUNT(*) AS lead_count
                FROM properties p
                JOIN distress_scores ds ON ds.property_id = p.id
                JOIN (
                    SELECT property_id, MAX(score_date) AS max_date
                    FROM distress_scores
                    GROUP BY property_id
                ) latest ON latest.property_id = ds.property_id
                        AND latest.max_date = ds.score_date
                WHERE p.county_id = :county_id
                  AND p.zip IS NOT NULL
                  AND LENGTH(p.zip) = 5
                  AND ds.lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold')
                  AND ds.qualified = true
                GROUP BY p.zip
            """),
            {"county_id": county_id},
        ).fetchall()
    except OperationalError:
        lead_rows = []

    lead_counts = {r[0]: r[1] for r in lead_rows}

    result = []
    for zip_code in all_zips:
        status = taken_map.get(zip_code)
        if status == "locked":
            availability = "taken"
        elif status == "grace":
            availability = "grace"
        else:
            availability = "available"

        result.append({
            "zip_code": zip_code,
            "status": availability,
            "property_count": prop_counts.get(zip_code, 0),
            "lead_count": lead_counts.get(zip_code, 0),
            "waitlist_count": waitlist_map.get(zip_code, 0),
        })

    payload = {
        "vertical": vertical,
        "county_id": county_id,
        "zips": result,
    }

    if redis_available():
        rset(_cache_key, json.dumps(payload), ttl_seconds=_CACHE_TTL)

    return payload


# ---------------------------------------------------------------------------
# GET /api/feed/{uuid}
# ---------------------------------------------------------------------------

_VALID_SORTS = {"score_desc", "newest", "value_desc"}


def _compute_save_offer_active(subscriber, db) -> bool:
    """Return True if this subscriber is eligible for the Data-Only save offer."""
    from src.tasks.proactive_save import compute_save_offer_active
    return compute_save_offer_active(subscriber, db)


def _payment_recovery_fields(subscriber) -> dict:
    """Surface Stripe failed-payment recovery state to the frontend so the
    dashboard can render a PaymentFailedBanner. Stage is derived client-side
    from `recovery_day3_sent` (soft until day-3 email fires, urgency after)."""
    failed_at = getattr(subscriber, "payment_failed_at", None)
    return {
        "payment_failed_at": failed_at.isoformat() if failed_at else None,
        "recovery_day1_sent": bool(getattr(subscriber, "recovery_day1_sent", False)),
        "recovery_day3_sent": bool(getattr(subscriber, "recovery_day3_sent", False)),
    }


def _what_you_missed_fields(subscriber, db, *, save_offer_active: bool, locked_zips=None) -> dict:
    """Emit a `what_you_missed` block ONLY for recovery-targeted subscribers
    (matches the audience the retention graph already messages). Reuses the
    same lead-pool/zip-activity tools as `_node_assemble_opportunity_gap` so
    copy parity with email/SMS is automatic.

    Gate: save_offer_active OR payment_failed_at set OR status == 'grace'.
    Returns `{"what_you_missed": None}` when the gate is closed.
    """
    qualifies = (
        bool(save_offer_active)
        or getattr(subscriber, "payment_failed_at", None) is not None
        or subscriber.status == "grace"
    )
    if not qualifies:
        return {"what_you_missed": None}

    zip_pool = list(locked_zips or [])
    if not zip_pool and subscriber.lock_candidate_zip:
        zip_pool = [subscriber.lock_candidate_zip]
    if not zip_pool:
        return {"what_you_missed": None}

    try:
        from src.services.lead_pool_service import get_lead_pool, get_zip_activity
    except Exception:
        return {"what_you_missed": None}

    top_zip = zip_pool[0]
    try:
        pool = get_lead_pool(top_zip, vertical=subscriber.vertical, min_score=60, limit=50, session=db)
    except Exception:
        pool = []

    # All leads in pool already pass min_score=60 (Gold+); count them all.
    # Checking .endswith("gold") would exclude Platinum and Ultra Platinum.
    gold_count = len(pool)
    if gold_count <= 0:
        return {"what_you_missed": None}

    try:
        activity = get_zip_activity(top_zip, vertical=subscriber.vertical, session=db)
        competing_viewers = int(activity.get("active_viewers", 0) or 0)
    except Exception:
        competing_viewers = 0

    return {
        "what_you_missed": {
            "gold_count": gold_count,
            "top_zip": top_zip,
            "competing_viewers": competing_viewers,
            "vertical": subscriber.vertical,
        }
    }


def _auto_mode_entitlement_fields(subscriber, db) -> dict:
    """Tell the frontend whether the user can flip the Auto Mode toggle for free.

    Growth/Power wallet tiers include it natively. Starter users need an
    active Stripe subscription on the auto_mode add-on price — checking
    Stripe per page load is cheap because we only call it for starter_wallet.

    Returns:
        {"auto_mode_entitled": bool}
    """
    from src.core.models import WalletBalance
    from src.services.auto_mode import _AUTO_MODE_TIERS, _has_active_auto_mode_addon

    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber.id)
    ).scalar_one_or_none()
    if wallet and wallet.wallet_tier in _AUTO_MODE_TIERS:
        return {"auto_mode_entitled": True}
    # Starter or no-wallet path: source of truth is the live Stripe subscription
    # list. Fails closed (any error → not entitled) per _has_active_auto_mode_addon.
    return {"auto_mode_entitled": _has_active_auto_mode_addon(subscriber)}


def _accelerated_wallet_offer_fields(subscriber, db) -> dict:
    """Surface the latest open accelerated_wallet_push offer to the frontend.

    Returns dict with:
      accelerated_wallet_offer_active: bool
      accelerated_wallet_offer_id:     int | None
      accelerated_wallet_offer_tier:   str | None
      accelerated_wallet_offer_credits: int | None
      accelerated_wallet_offer_price_cents: int | None
      missed_lead_count:               int
      saved_card_last4:                str | None
    """
    from config.revenue_ladder import WALLET_TIERS
    from src.core.models import WalletPushOffer

    out = {
        "accelerated_wallet_offer_active": False,
        "accelerated_wallet_offer_id": None,
        "accelerated_wallet_offer_tier": None,
        "accelerated_wallet_offer_credits": None,
        "accelerated_wallet_offer_price_cents": None,
        "missed_lead_count": int(getattr(subscriber, "missed_lead_count", 0) or 0),
        "saved_card_last4": None,
    }

    if getattr(subscriber, "wallet_opt_out", False):
        return out

    try:
        offer = db.execute(
            select(WalletPushOffer)
            .where(
                WalletPushOffer.subscriber_id == subscriber.id,
                WalletPushOffer.status == "offered",
            )
            .order_by(WalletPushOffer.offered_at.desc())
            .limit(1)
        ).scalar_one_or_none()
    except Exception:
        offer = None

    if offer is not None:
        tier_cfg = WALLET_TIERS.get(offer.tier) or WALLET_TIERS.get("starter_wallet")
        out["accelerated_wallet_offer_active"] = True
        out["accelerated_wallet_offer_id"] = offer.id
        out["accelerated_wallet_offer_tier"] = offer.tier
        if tier_cfg:
            out["accelerated_wallet_offer_credits"] = tier_cfg.get("credits_per_cycle")
            out["accelerated_wallet_offer_price_cents"] = tier_cfg.get("price_cents")

    # Saved-card last4 lookup is best-effort via Stripe. Skip if Stripe is not
    # configured — frontend just shows "your saved card" without last4.
    pm_id = getattr(subscriber, "stripe_payment_method_id", None)
    if pm_id:
        try:
            from config.settings import settings
            import stripe as _stripe
            key = settings.active_stripe_secret_key
            if key:
                _stripe.api_key = key.get_secret_value()
                pm = _stripe.PaymentMethod.retrieve(pm_id)
                out["saved_card_last4"] = (pm.get("card") or {}).get("last4")
        except Exception:
            pass

    return out


@app.get("/api/feed/{feed_uuid}")
def event_feed(
    feed_uuid: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    sort: str = Query(default="score_desc"),
    min_score: Optional[float] = Query(default=None, ge=0.0, le=100.0),
    incident_type: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None, max_length=100),
    db: Session = Depends(get_db),
    _auth=Depends(get_current_subscriber),
):
    """
    Subscriber-facing Event Feed.
    Token-gated (fa061): requires a valid subscriber session JWT whose subject
    owns this event_feed_uuid — enforced by get_current_subscriber (401/403/404).
    Returns scored leads within the subscriber's locked ZIP territories,
    filtered by their vertical's score, ordered by CDS score descending.
    """
    # 1. Authenticate subscriber by UUID
    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
        ).scalar_one_or_none()
    except OperationalError:
        logger.error("DB error authenticating feed UUID", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not subscriber:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Feed not found"})

    if subscriber.status == "paused":
        return {
            "feed_uuid": feed_uuid,
            "subscriber": {
                "id": subscriber.id,
                "tier": subscriber.tier,
                "vertical": subscriber.vertical,
                "county_id": subscriber.county_id,
                "locked_zips": [],
                "founding_member": subscriber.founding_member,
                "status": "paused",
                "has_saved_card": subscriber.has_saved_card,
                "auto_mode_enabled": subscriber.auto_mode_enabled,
                "created_at": subscriber.created_at.isoformat() if subscriber.created_at else None,
                "wallet_balance": None,
                "wallet_tier": None,
                "ap_lite_eligible": False,
                "manual_actions_this_week": 0,
                "paused_at": subscriber.paused_at.isoformat() if subscriber.paused_at else None,
                "pause_resume_at": subscriber.pause_resume_at.isoformat() if subscriber.pause_resume_at else None,
                "save_offer_active": False,
                "lock_candidate_zip": None,
                "lock_candidate_at": None,
                "wallet_to_lock_eligible": False,
                "wallet_credits_30d": None,
                "flash_scarcity_windows": [],
                **_accelerated_wallet_offer_fields(subscriber, db),
                **_auto_mode_entitlement_fields(subscriber, db),
                **_payment_recovery_fields(subscriber),
                **_what_you_missed_fields(subscriber, db, save_offer_active=False),
            },
            "total": 0,
            "page": page,
            "page_size": page_size,
            "pages": 0,
            "leads": [],
        }

    if subscriber.status not in ("active", "grace", "disputed"):
        raise HTTPException(status_code=403, detail={"error": "subscription_inactive", "message": "Subscription is not active"})

    # 2. Get subscriber's locked ZIP codes
    try:
        locked_zips = db.execute(
            select(ZipTerritory.zip_code).where(
                and_(
                    ZipTerritory.subscriber_id == subscriber.id,
                    ZipTerritory.status.in_(["locked", "grace"]),
                )
            )
        ).scalars().all()
    except OperationalError:
        logger.error("DB error fetching locked ZIPs for feed", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not locked_zips:
        from config.ap_lite import AP_LITE_ELIGIBLE_TIERS, AP_LITE_THRESHOLD_PER_WEEK
        from src.services.manual_action_counter import count_this_week as _count_actions
        _manual_actions = _count_actions(db, subscriber.id)
        _ap_lite_eligible = (
            subscriber.tier in AP_LITE_ELIGIBLE_TIERS
            and subscriber.status == "active"
            and _manual_actions >= AP_LITE_THRESHOLD_PER_WEEK
        )
        from src.core.models import WalletBalance as _WB
        _wallet = db.execute(select(_WB).where(_WB.subscriber_id == subscriber.id)).scalar_one_or_none()
        from src.services.wallet_to_lock import compute_wallet_to_lock_eligibility as _w2l_compute
        _w2l_eligible_nz, _wallet_credits_30d_nz = _w2l_compute(db, subscriber)
        try:
            from src.services.flash_scarcity import get_active_windows_for_subscriber as _get_flash_nz
            _flash_windows_nz = _get_flash_nz(db, subscriber.id)
        except Exception:
            _flash_windows_nz = []

        # Free-tier unlocked leads (from $4 unlocks). Must appear in the feed
        # even without a ZIP territory — that's the whole pay-per-lead promise.
        _unlocked_leads: list = []
        try:
            with db.begin_nested():
                unlocked_ids_no_zip = db.execute(
                    select(SentLead.property_id).where(
                        SentLead.subscriber_id == subscriber.id,
                    )
                ).scalars().all()
                if unlocked_ids_no_zip:
                    # DistressScore is 1-to-many with Property — pick the latest
                    # row per property to avoid duplicate cards.
                    _latest_score_for_unlocked = (
                        select(
                            DistressScore.property_id.label("prop_id"),
                            func.max(DistressScore.score_date).label("max_date"),
                        )
                        .where(DistressScore.property_id.in_(unlocked_ids_no_zip))
                        .group_by(DistressScore.property_id)
                        .subquery()
                    )
                    unlocked_rows = db.execute(
                        select(Property, DistressScore, Owner)
                        .join(DistressScore, DistressScore.property_id == Property.id)
                        .join(
                            _latest_score_for_unlocked,
                            and_(
                                _latest_score_for_unlocked.c.prop_id == DistressScore.property_id,
                                _latest_score_for_unlocked.c.max_date == DistressScore.score_date,
                            ),
                        )
                        .outerjoin(Owner, Owner.property_id == Property.id)
                        .where(Property.id.in_(unlocked_ids_no_zip))
                    ).all()
                    # Final-line dedupe in case two scores share score_date.
                    _seen_prop_ids = set()
                    unlocked_rows = [
                        r for r in unlocked_rows
                        if not (r[0].id in _seen_prop_ids or _seen_prop_ids.add(r[0].id))
                    ]
                    incs_for_unlocked = db.execute(
                        select(Incident).where(Incident.property_id.in_(unlocked_ids_no_zip))
                    ).scalars().all()
                    inc_map: dict = {}
                    for inc in incs_for_unlocked:
                        inc_map.setdefault(inc.property_id, []).append({
                            "type": inc.incident_type,
                            "date": inc.incident_date.isoformat() if inc.incident_date else None,
                        })
                    for prop, score, owner in unlocked_rows:
                        owner_phone, owner_phone_quality = _resolve_phone_with_quality(owner)
                        owner_email = (owner.email_1 or owner.email_2) if owner else None
                        _visible_tier, _visible_urgency = _visible_tier_fields(prop, score)
                        _unlocked_leads.append({
                            "property_id": prop.id,
                            "parcel_id": prop.parcel_id,
                            "address": prop.address,
                            "city": prop.city,
                            "state": prop.state,
                            "zip": prop.zip,
                            "property_type": prop.property_type,
                            "year_built": prop.year_built,
                            "sq_ft": prop.sq_ft,
                            "lat": float(prop.lat) if prop.lat else None,
                            "lon": float(prop.lon) if prop.lon else None,
                            "cds_score": float(score.final_cds_score) if score.final_cds_score else None,
                            "vertical_score": score.vertical_scores.get(subscriber.vertical) if score.vertical_scores else None,
                            "lead_tier": _visible_tier,
                            "urgency": _visible_urgency,
                            "distress_types": score.distress_types,
                            "est_job_value": _estimate_lead_job_value(prop, score),
                            "incidents": inc_map.get(prop.id, []),
                            "unlocked": True,
                            "owner_name": owner.owner_name if owner else None,
                            "phone": owner_phone,
                            "phone_quality": owner_phone_quality,
                            "email": owner_email,
                        })
        except Exception as exc:
            logger.warning("free-tier unlocked leads query failed for sub=%s: %s",
                           subscriber.id, exc, exc_info=True)

        # Free-tier blurred stack — gives the dashboard real content to render
        # instead of a dead empty state. Each card is "Unlock for $4".
        # Wrapped in begin_nested() so a SQL error here can't poison the
        # outer transaction (the request still needs to return + commit).
        _blurred_stack: list = []
        try:
            with db.begin_nested():
                from src.services.proof_moment import get_blurred_stack as _get_blurred_stack
                _blurred_stack = _get_blurred_stack(
                    subscriber.id, subscriber.vertical, subscriber.county_id, db, limit=5,
                )
        except Exception as exc:
            logger.warning("blurred_stack failed for sub=%s: %s", subscriber.id, exc)
            _blurred_stack = []

        try:
            with db.begin_nested():
                from src.services.business_events import log_business_event
                log_business_event(
                    "FEED_REFRESHED", subscriber_id=subscriber.id,
                    payload={"tier": subscriber.tier, "lead_count": 0,
                             "blurred_count": len(_blurred_stack)}, db=db,
                )
        except Exception:
            pass

        if subscriber.tier == "free" and _blurred_stack and page == 1:
            try:
                from src.agents.events.ingestion import publish_cora_event
                publish_cora_event({
                    "event_type": "wall_session_abandoned",
                    "subscriber_id": subscriber.id,
                    "payload": {
                        "vertical": subscriber.vertical or "",
                        "zip_code": subscriber.lock_candidate_zip or "",
                    },
                })
            except Exception:
                pass

        return {
            "feed_uuid": feed_uuid,
            "subscriber": {
                "id": subscriber.id,
                "tier": subscriber.tier,
                "vertical": subscriber.vertical,
                "county_id": subscriber.county_id,
                "locked_zips": [],
                "founding_member": subscriber.founding_member,
                "status": subscriber.status,
                "has_saved_card": subscriber.has_saved_card,
                "auto_mode_enabled": subscriber.auto_mode_enabled,
                "created_at": subscriber.created_at.isoformat() if subscriber.created_at else None,
                "wallet_balance": _wallet.credits_remaining if _wallet else None,
                "wallet_tier": _wallet.wallet_tier if _wallet else None,
                "ap_lite_eligible": _ap_lite_eligible,
                "manual_actions_this_week": _manual_actions,
                "paused_at": subscriber.paused_at.isoformat() if subscriber.paused_at else None,
                "pause_resume_at": subscriber.pause_resume_at.isoformat() if subscriber.pause_resume_at else None,
                "save_offer_active": _compute_save_offer_active(subscriber, db),
                "lock_candidate_zip": subscriber.lock_candidate_zip,
                "lock_candidate_at": subscriber.lock_candidate_at.isoformat() if subscriber.lock_candidate_at else None,
                "wallet_to_lock_eligible": _w2l_eligible_nz,
                "wallet_credits_30d": _wallet_credits_30d_nz,
                "flash_scarcity_windows": _flash_windows_nz,
                **_accelerated_wallet_offer_fields(subscriber, db),
                **_auto_mode_entitlement_fields(subscriber, db),
                **_payment_recovery_fields(subscriber),
                **_what_you_missed_fields(
                    subscriber, db,
                    save_offer_active=_compute_save_offer_active(subscriber, db),
                ),
            },
            "total": len(_unlocked_leads),
            "page": page,
            "page_size": page_size,
            "pages": 1 if _unlocked_leads else 0,
            "leads": _unlocked_leads,
            "blurred_stack": _blurred_stack,
        }

    # 3. Build lead query — properties in locked ZIPs with a distress score
    try:
        score_col = DistressScore.vertical_scores[subscriber.vertical].as_float()
    except KeyError:
        logger.error("Unknown vertical '%s' on subscriber %s", subscriber.vertical, subscriber.id)
        raise HTTPException(
            status_code=500,
            detail={"error": "configuration_error", "message": "Subscriber vertical is misconfigured"},
        )

    filters = [
        Property.zip.in_(locked_zips),
        Property.county_id == subscriber.county_id,
        DistressScore.qualified == True,
    ]

    if min_score is not None:
        filters.append(score_col >= min_score)

    # Filter leads whose distress_types JSONB array contains the requested type
    if incident_type:
        filters.append(DistressScore.distress_types.contains([incident_type]))

    # Full-text search across address, city, ZIP
    if search:
        term = f"%{search.strip()}%"
        filters.append(or_(
            Property.address.ilike(term),
            Property.city.ilike(term),
            Property.zip.ilike(term),
        ))

    # Production: require owner phone or email. Always sort phone-bearing
    # leads first; the contact filter only narrows what's eligible.
    from src.utils.lead_filters import has_contact_filter, phone_priority_order
    contact_clause = has_contact_filter(get_settings())
    if contact_clause is not None:
        filters.append(contact_clause)

    # Cross-trade exclusivity filter — exclude properties sold to other trades.
    # County-wide (covers ALL the subscriber's locked ZIPs, not just the first);
    # buyer keeps their own leads via exclude_trade=their vertical.
    from src.services.lead_exclusivity import get_exclusive_property_ids
    now = datetime.now(timezone.utc)
    excl_ids = get_exclusive_property_ids(
        db, subscriber.county_id, now, exclude_trade=subscriber.vertical
    )
    if excl_ids:
        filters.append(Property.id.not_in(excl_ids))

    # Sort order
    _sort = sort if sort in _VALID_SORTS else "score_desc"
    if _sort == "newest":
        order_cols = [desc(DistressScore.score_date)]
    elif _sort == "value_desc":
        order_cols = [desc(DistressScore.final_cds_score)]
    else:
        order_cols = phone_priority_order(score_col)

    # Dedupe to latest DistressScore row per property. `distress_scores` is
    # 1-to-many with `properties` (scoring runs accumulate history) so a
    # naive join returns duplicates. We pick MAX(score_date) per property,
    # then join back to the full row. Safety net: also dedupe in Python on
    # the rare chance two rows share the same score_date.
    latest_score_subq = (
        select(
            DistressScore.property_id.label("prop_id"),
            func.max(DistressScore.score_date).label("max_date"),
        )
        .group_by(DistressScore.property_id)
        .subquery()
    )

    base_query = (
        select(Property, DistressScore, Owner)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .join(
            latest_score_subq,
            and_(
                latest_score_subq.c.prop_id == DistressScore.property_id,
                latest_score_subq.c.max_date == DistressScore.score_date,
            ),
        )
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(and_(*filters))
        .order_by(*order_cols)
    )

    # 4. Total count — run over the deduped base_query so the pager is correct
    try:
        count_q = select(func.count()).select_from(base_query.subquery())
        total = db.execute(count_q).scalar()

        # 5. Paginate. Fetch slightly more than page_size so score_date ties
        # don't leave us short after the Python-level dedupe.
        offset = (page - 1) * page_size
        raw_rows = db.execute(base_query.offset(offset).limit(page_size + 10)).all()
        seen_ids = set()
        rows = []
        for prop, score, owner in raw_rows:
            if prop.id in seen_ids:
                continue
            seen_ids.add(prop.id)
            rows.append((prop, score, owner))
            if len(rows) >= page_size:
                break
    except OperationalError:
        logger.error("DB error fetching leads for feed", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    # 6. Fetch incidents for returned properties in one query
    property_ids = [prop.id for prop, _, _ in rows]

    try:
        incidents_raw = db.execute(
            select(Incident).where(Incident.property_id.in_(property_ids))
        ).scalars().all()
    except OperationalError:
        logger.error("DB error fetching incidents for feed", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    # Resolve which properties this subscriber has unlocked (paid or email-delivered)
    unlocked_ids: set[int] = set()
    if property_ids:
        try:
            unlocked_rows = db.execute(
                select(SentLead.property_id).where(
                    SentLead.subscriber_id == subscriber.id,
                    SentLead.property_id.in_(property_ids),
                )
            ).scalars().all()
            unlocked_ids = set(unlocked_rows)
        except OperationalError:
            logger.error("DB error fetching unlocks for feed", exc_info=True)
            # Non-fatal — render leads as locked rather than 503

    # Subscribers with locked ZIP territories see all in-territory contacts
    # revealed — independent of whether the daily-email job has stamped a
    # SentLead row yet. `locked_zips` was already resolved above for the lead
    # query; convert to a set for O(1) membership checks per lead.
    locked_zip_set: set[str] = {z for z in locked_zips if z}

    incidents_by_prop: dict = {}
    for inc in incidents_raw:
        incidents_by_prop.setdefault(inc.property_id, []).append({
            "type": inc.incident_type,
            "date": inc.incident_date.isoformat() if inc.incident_date else None,
        })

    # 7. Build response
    # Bulk-resolve portfolio sizes for every owner on this page in one query
    # (avoids N+1 across the 25-lead window). Scoped to the subscriber's
    # county so the count reflects what they can actually act on. Names absent
    # from the result map default to 1 (the lead's own row).
    from src.services.owner_lookup import portfolio_sizes_for_names
    _portfolio_map = portfolio_sizes_for_names(
        db,
        (owner.owner_name for _, _, owner in rows if owner),
        county_id=subscriber.county_id,
    )

    leads = []
    for prop, score, owner in rows:
        is_unlocked = (prop.id in unlocked_ids) or (prop.zip in locked_zip_set)
        owner_phone, owner_phone_quality = _resolve_phone_with_quality(owner)
        owner_email = (owner.email_1 or owner.email_2) if owner else None
        _visible_tier, _visible_urgency = _visible_tier_fields(prop, score)
        portfolio = _portfolio_map.get(owner.owner_name, 1) if owner else 1
        leads.append({
            "property_id": prop.id,
            "parcel_id": prop.parcel_id,
            "address": prop.address,
            "city": prop.city,
            "state": prop.state,
            "zip": prop.zip,
            "property_type": prop.property_type,
            "year_built": prop.year_built,
            "sq_ft": prop.sq_ft,
            "lat": float(prop.lat) if prop.lat else None,
            "lon": float(prop.lon) if prop.lon else None,
            "cds_score": float(score.final_cds_score) if score.final_cds_score else None,
            "vertical_score": score.vertical_scores.get(subscriber.vertical) if score.vertical_scores else None,
            "lead_tier": _visible_tier,
            "urgency": _visible_urgency,
            "distress_types": score.distress_types,
            "est_job_value": _estimate_lead_job_value(prop, score),
            "incidents": incidents_by_prop.get(prop.id, []),
            "unlocked": is_unlocked,
            "owner_name": owner.owner_name if (owner and is_unlocked) else None,
            "phone": owner_phone if is_unlocked else None,
            "phone_quality": owner_phone_quality if is_unlocked else None,
            "email": owner_email if is_unlocked else None,
            "portfolio_size": portfolio,
        })

    from src.core.models import WalletBalance as _WalletBalance
    wallet = db.execute(
        select(_WalletBalance).where(_WalletBalance.subscriber_id == subscriber.id)
    ).scalar_one_or_none()

    from config.ap_lite import AP_LITE_ELIGIBLE_TIERS, AP_LITE_THRESHOLD_PER_WEEK
    from src.services.manual_action_counter import count_this_week as _count_actions
    manual_actions_this_week = _count_actions(db, subscriber.id)
    ap_lite_eligible = (
        subscriber.tier in AP_LITE_ELIGIBLE_TIERS
        and subscriber.status == "active"
        and manual_actions_this_week >= AP_LITE_THRESHOLD_PER_WEEK
    )

    # Wallet-to-Lock eligibility — wallet-tier sub (free/starter w/ WalletBalance)
    # who hit 40+ debits in a single available ZIP within the last 30d. The
    # legacy check `tier == "wallet"` was a dead branch — "wallet" isn't a
    # valid Subscriber.tier value.
    from src.services.wallet_to_lock import compute_wallet_to_lock_eligibility as _w2l_compute
    _w2l_eligible, _wallet_credits_30d = _w2l_compute(db, subscriber)

    # Flash scarcity windows
    try:
        from src.services.flash_scarcity import get_active_windows_for_subscriber as _get_flash
        _flash_windows = _get_flash(db, subscriber.id)
    except Exception:
        _flash_windows = []

    try:
        with db.begin_nested():
            from src.services.business_events import log_business_event
            log_business_event(
                "FEED_REFRESHED", subscriber_id=subscriber.id,
                payload={"tier": subscriber.tier, "lead_count": len(leads)}, db=db,
            )
    except Exception:
        pass

    return {
        "feed_uuid": feed_uuid,
        "subscriber": {
            "id": subscriber.id,
            "tier": subscriber.tier,
            "vertical": subscriber.vertical,
            "county_id": subscriber.county_id,
            "locked_zips": list(locked_zips),
            "founding_member": subscriber.founding_member,
            "status": subscriber.status,
            "disputed_at": subscriber.disputed_at.isoformat() if subscriber.disputed_at else None,
            "has_saved_card": subscriber.has_saved_card,
            "auto_mode_enabled": subscriber.auto_mode_enabled,
            "created_at": subscriber.created_at.isoformat() if subscriber.created_at else None,
            "wallet_balance": wallet.credits_remaining if wallet else None,
            "wallet_tier": wallet.wallet_tier if wallet else None,
            "ap_lite_eligible": ap_lite_eligible,
            "manual_actions_this_week": manual_actions_this_week,
            "paused_at": subscriber.paused_at.isoformat() if subscriber.paused_at else None,
            "pause_resume_at": subscriber.pause_resume_at.isoformat() if subscriber.pause_resume_at else None,
            "save_offer_active": _compute_save_offer_active(subscriber, db),
            "lock_candidate_zip": subscriber.lock_candidate_zip,
            "lock_candidate_at": subscriber.lock_candidate_at.isoformat() if subscriber.lock_candidate_at else None,
            "wallet_to_lock_eligible": _w2l_eligible,
            "wallet_credits_30d": _wallet_credits_30d,
            "flash_scarcity_windows": _flash_windows,
            **_accelerated_wallet_offer_fields(subscriber, db),
            **_payment_recovery_fields(subscriber),
            **_what_you_missed_fields(
                subscriber, db,
                save_offer_active=_compute_save_offer_active(subscriber, db),
                locked_zips=locked_zips,
            ),
        },
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": -(-total // page_size),  # ceiling division
        "leads": leads,
    }


# ---------------------------------------------------------------------------
# GET /api/feed/{uuid}/stats
# ---------------------------------------------------------------------------

_RESCORE_FLAG = Path(__file__).resolve().parent.parent.parent / "data" / "rescore_in_progress.flag"


@app.get("/api/feed/{feed_uuid}/stats")
def feed_stats(feed_uuid: str, db: Session = Depends(get_db), _auth=Depends(get_current_subscriber)):
    """Aggregate stats for the subscriber's feed: totals, new today, tier breakdown.

    Token-gated (fa061) via get_current_subscriber — same auth as the feed."""
    from datetime import date, timezone, datetime as _dt

    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
        ).scalar_one_or_none()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not subscriber:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Feed not found"})

    if subscriber.status not in ("active", "grace", "disputed"):
        raise HTTPException(status_code=403, detail={"error": "subscription_inactive", "message": "Subscription is not active"})

    try:
        locked_zips = db.execute(
            select(ZipTerritory.zip_code).where(
                and_(
                    ZipTerritory.subscriber_id == subscriber.id,
                    ZipTerritory.status.in_(["locked", "grace"]),
                )
            )
        ).scalars().all()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not locked_zips:
        return {"total_leads": 0, "new_today": 0, "tier_distribution": {}, "last_updated": None}

    base_filter = [
        Property.zip.in_(locked_zips),
        Property.county_id == subscriber.county_id,
        DistressScore.qualified == True,
    ]

    try:
        base_q = (
            select(DistressScore)
            .join(Property, Property.id == DistressScore.property_id)
            .where(and_(*base_filter))
        )

        total = db.execute(
            select(func.count(func.distinct(DistressScore.property_id)))
            .select_from(base_q.subquery())
        ).scalar()

        today_start = _dt.combine(date.today(), _dt.min.time())
        new_today = db.execute(
            select(func.count(func.distinct(DistressScore.property_id))).select_from(
                base_q.where(DistressScore.score_date >= today_start).subquery()
            )
        ).scalar()

        # Suppress the tier breakdown for counties whose tier distribution
        # isn't yet trustworthy (see config.scoring.COUNTY_OVERRIDES). The
        # raw counts stay in distress_scores for internal analytics.
        _county_cfg = for_county(subscriber.county_id)
        if _county_cfg.tier_visibility == "internal":
            tier_distribution: dict = {}
        else:
            tier_rows = db.execute(
                select(DistressScore.lead_tier, func.count().label("cnt"))
                .join(Property, Property.id == DistressScore.property_id)
                .where(and_(*base_filter))
                .group_by(DistressScore.lead_tier)
            ).all()
            tier_distribution = {row.lead_tier: row.cnt for row in tier_rows if row.lead_tier}

        last_updated_row = db.execute(
            select(func.max(DistressScore.score_date))
            .join(Property, Property.id == DistressScore.property_id)
            .where(and_(*base_filter))
        ).scalar()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    rescore_notice = (
        "Lead pool is being refreshed with improved scoring. "
        "Counts will be lower than usual during this window and will update within the hour."
        if _RESCORE_FLAG.exists() else None
    )

    return {
        "total_leads": total,
        "new_today": new_today,
        "tier_distribution": tier_distribution,
        "last_updated": last_updated_row.isoformat() if last_updated_row else None,
        "rescore_notice": rescore_notice,
    }


# ---------------------------------------------------------------------------
# POST /api/resend-confirmation
# ---------------------------------------------------------------------------

class ResendConfirmationRequest(BaseModel):
    feed_uuid: str


@app.post("/api/resend-confirmation")
def resend_confirmation(payload: ResendConfirmationRequest, db: Session = Depends(get_db)):
    """Re-send the welcome/confirmation email for a subscriber by feed_uuid."""
    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == payload.feed_uuid)
        ).scalar_one_or_none()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not subscriber:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Feed not found"})

    if not subscriber.email:
        raise HTTPException(status_code=422, detail={"error": "no_email", "message": "No email address on record"})

    try:
        from src.services.email import send_welcome_email
        from src.services import subscriber_auth as _sub_auth
        feed_password = None
        if not subscriber.password_hash:
            feed_password = _sub_auth.generate_random_password()
            subscriber.password_hash = _sub_auth.hash_password(feed_password)
            subscriber.password_set_at = datetime.now(timezone.utc)
            db.flush()
        send_welcome_email(subscriber, plaintext_password=feed_password)
    except Exception:
        logger.error("Failed to resend confirmation for feed %s", payload.feed_uuid, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "send_failed", "message": "Failed to send email"})

    return {"ok": True}


# _resolve_phone_with_quality, _estimate_lead_job_value, _visible_tier_fields
# imported from src.api.deps


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# GET /api/counties/{county_id}/landing  — server-authoritative landing state
# POST /api/waitlist                     — write WaitlistEntry
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_WAITLIST_TRADE_LABELS = {
    "roofing": "Roofing",
    "restoration": "Restoration",
    "public_adjusters": "Public Adjuster",
    "wholesalers": "Wholesaler",
    "fix_flip": "Fix & Flip",
    "attorneys": "Attorney",
}


def _resolve_county_landing_state(county_id: str, db: Session) -> tuple[str, str, int, dict]:
    """
    Returns (waitlist_type, county_display_name, zip_count, vertical_waitlist_counts)
    or raises HTTPException(404).

    Resolution rules (ADR-0004):
    - ExpansionCandidate status in (queued, approved) → coming_soon
    - ExpansionCandidate status launched, OR no candidate row but county exists
      AND ≥1 taken/grace ZIP → sold_out
    - Anything else → 404
    """
    county = db.execute(
        select(County).where(County.county_id == county_id)
    ).scalar_one_or_none()
    if not county:
        raise HTTPException(status_code=404, detail={"error": "unknown_county"})

    candidate = db.execute(
        select(ExpansionCandidate).where(ExpansionCandidate.county_id == county_id)
    ).scalar_one_or_none()

    if candidate and candidate.status in ("queued", "approved"):
        waitlist_type = "coming_soon"
    else:
        # launched candidate OR source county (no candidate row) — check ZIP availability
        any_taken = db.execute(
            select(func.count(ZipTerritory.id)).where(
                ZipTerritory.county_id == county_id,
                ZipTerritory.status.in_(("locked", "grace")),
            )
        ).scalar_one()
        if any_taken == 0:
            raise HTTPException(
                status_code=404,
                detail={"error": "no_waitlist_state",
                        "message": "County has no taken territories and is not pre-launch."},
            )
        waitlist_type = "sold_out"

    zip_count = db.execute(
        select(func.count(distinct(ZipTerritory.zip_code))).where(
            ZipTerritory.county_id == county_id
        )
    ).scalar_one()

    vertical_counts = dict(
        db.execute(
            select(WaitlistEntry.vertical, func.count())
            .where(
                WaitlistEntry.county_id == county_id,
                WaitlistEntry.status == "waiting",
            )
            .group_by(WaitlistEntry.vertical)
        ).all()
    )

    return waitlist_type, county.display_name, zip_count, vertical_counts


@app.get("/api/counties/{county_id}/landing")
def get_county_landing(county_id: str, db: Session = Depends(get_db)):
    """
    Server-authoritative county landing page state (ADR-0004).
    Returns waitlist_type, display metadata, and waitlist counts.
    404 when the county has no waitlistable state.
    """
    try:
        wl_type, display_name, zip_count, vert_counts = _resolve_county_landing_state(county_id, db)
    except HTTPException:
        raise
    except OperationalError:
        logger.error("DB error in get_county_landing", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable"})

    return {
        "county_id": county_id,
        "county_display_name": display_name,
        "waitlist_type": wl_type,
        "zip_count": zip_count,
        "vertical_waitlist_counts": vert_counts,
        "trade_labels": _WAITLIST_TRADE_LABELS,
    }


_ALLOWED_WAITLIST_COUNTIES = { "pinellas"}


_ALLOWED_LANDING_COUNTIES = {"hillsborough", "pinellas"}


@app.get("/api/landing-data")
def get_landing_data(county_id: Optional[str] = Query(default=None), db: Session = Depends(get_db)):
    """
    Aggregated county-specific landing page data.

    Returns all metrics needed to render the landing page for a single county.
    All counts and queries are filtered by county_id — no global numbers returned.

    400  county_id missing
    404  county not found in counties table
    200  county_status=unavailable when county exists but is not in _ALLOWED_LANDING_COUNTIES
    200  full response otherwise
    """
    if not county_id:
        raise HTTPException(status_code=400, detail={"error": "county_id_required"})

    from src.core.redis_client import redis_available, rget, rset
    _cache_key = f"landing_data:{county_id}"
    if redis_available():
        cached = rget(_cache_key)
        if cached:
            return json.loads(cached)

    county = db.execute(
        select(County).where(County.county_id == county_id)
    ).scalar_one_or_none()
    if not county:
        raise HTTPException(status_code=404, detail={"error": "county_not_found", "county_id": county_id})

    if county_id not in _ALLOWED_LANDING_COUNTIES:
        _unavailable = {
            "county_id": county_id,
            "county_name": county.display_name,
            "county_status": "unavailable",
            "cta_mode": None,
            "hero": {"headline": None, "subtitle": None},
            "stats": None,
            "top_zips": [],
            "territory_availability": None,
            "scraper_health": None,
            "coming_soon": None,
        }
        if redis_available():
            rset(_cache_key, json.dumps(_unavailable), ttl_seconds=60)
        return _unavailable

    # ── county_status + cta_mode from expansion_candidates ─────────────────
    candidate = db.execute(
        select(ExpansionCandidate).where(ExpansionCandidate.county_id == county_id)
    ).scalar_one_or_none()

    if candidate is None:
        # No expansion_candidate row. Source counties (e.g. Hillsborough) have
        # active subscribers; unconfigured or pre-launch counties do not.
        active_subs = db.execute(
            select(func.count(Subscriber.id)).where(
                Subscriber.county_id == county_id,
                Subscriber.status == "active",
            )
        ).scalar_one_or_none() or 0
        if active_subs > 0:
            county_status = "active"
            cta_mode = "signup"
        else:
            county_status = "unavailable"
            cta_mode = None
        coming_soon = None
    elif candidate.status == "launched":
        county_status = "launched"
        cta_mode = "signup"
        coming_soon = None
    elif candidate.status in ("queued", "approved", "launching"):
        county_status = "coming_soon"
        cta_mode = "waitlist"
        coming_soon = {"expected_launch": None, "waitlist_open": True}
    else:
        county_status = "unavailable"
        cta_mode = None
        coming_soon = None

    today = date.today()

    # ── stats ───────────────────────────────────────────────────────────────
    gold_plus = db.execute(
        select(func.count(DistressScore.id)).where(
            DistressScore.county_id == county_id,
            DistressScore.score_date >= datetime.combine(today, datetime.min.time()),
            DistressScore.lead_tier.in_(["Gold", "Platinum", "Ultra Platinum"]),
        )
    ).scalar_one_or_none() or 0

    total_scored = db.execute(
        select(func.count(DistressScore.id)).where(
            DistressScore.county_id == county_id,
            DistressScore.score_date >= datetime.combine(today, datetime.min.time()),
            DistressScore.qualified == True,  # noqa: E712
        )
    ).scalar_one_or_none() or 0

    prop_count = db.execute(
        select(func.count(Property.id)).where(Property.county_id == county_id)
    ).scalar_one_or_none() or 0

    enriched_count = db.execute(
        select(func.count(EnrichedContact.id)).where(EnrichedContact.county_id == county_id)
    ).scalar_one_or_none() or 0

    enrichment_rate = round((enriched_count / prop_count) * 100) if prop_count > 0 else None

    active_sub_count = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
            Subscriber.status == "active",
        )
    ).scalar_one_or_none() or 0

    waitlist_count = db.execute(
        select(func.count(WaitlistEntry.id)).where(
            WaitlistEntry.county_id == county_id,
            WaitlistEntry.status == "waiting",
        )
    ).scalar_one_or_none() or 0 if county_status == "coming_soon" else None

    # ── top ZIPs ────────────────────────────────────────────────────────────
    top_zip_rows2 = db.execute(
        text("""
            SELECT p.zip, COUNT(ds.id) AS lead_count,
                   COALESCE(zt.status, 'available') AS status
            FROM distress_scores ds
            JOIN properties p ON p.id = ds.property_id
            LEFT JOIN zip_territories zt
                ON zt.zip_code = p.zip AND zt.county_id = :cid
                AND zt.vertical = 'roofing'
            WHERE ds.county_id = :cid
              AND ds.score_date >= :today
              AND ds.qualified = true
              AND p.zip IS NOT NULL
            GROUP BY p.zip, zt.status
            ORDER BY lead_count DESC
            LIMIT 5
        """),
        {"cid": county_id, "today": today},
    ).mappings().all()

    top_zips = [
        {"zip_code": r["zip"], "lead_count": r["lead_count"], "status": r["status"]}
        for r in top_zip_rows2
    ]

    # ── territory availability ───────────────────────────────────────────────
    terr_row = db.execute(
        text("""
            SELECT
                COUNT(DISTINCT zip_code) AS total_zips,
                COUNT(DISTINCT zip_code) FILTER (WHERE status = 'locked') AS locked_zips,
                COUNT(DISTINCT zip_code) FILTER (WHERE status NOT IN ('locked','grace')) AS available_zips
            FROM zip_territories
            WHERE county_id = :cid
        """),
        {"cid": county_id},
    ).mappings().first()

    territory_availability = {
        "total_zips": terr_row["total_zips"] or 0,
        "available_zips": terr_row["available_zips"] or 0,
        "locked_zips": terr_row["locked_zips"] or 0,
    } if terr_row else None

    # ── scraper health ───────────────────────────────────────────────────────
    health_row = db.execute(
        select(ScraperRunStats)
        .where(ScraperRunStats.county_id == county_id)
        .order_by(ScraperRunStats.run_date.desc(), ScraperRunStats.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if health_row:
        # Count distinct source_types active in the last 30 days
        signals_active = db.execute(
            select(func.count(func.distinct(ScraperRunStats.source_type))).where(
                ScraperRunStats.county_id == county_id,
                ScraperRunStats.run_success == True,  # noqa: E712
                ScraperRunStats.run_date >= (today - timedelta(days=30)),
            )
        ).scalar_one_or_none() or 0

        signals_total = db.execute(
            select(func.count(func.distinct(ScraperRunStats.source_type))).where(
                ScraperRunStats.county_id == county_id,
            )
        ).scalar_one_or_none() or 0

        scraper_health = {
            "last_run_at": health_row.run_date.isoformat() if health_row.run_date else None,
            "signals_active": signals_active,
            "signals_total": signals_total,
        }
    else:
        scraper_health = None

    _result = {
        "county_id": county_id,
        "county_name": county.display_name,
        "county_status": county_status,
        "cta_mode": cta_mode,
        "hero": {"headline": None, "subtitle": None},
        "stats": {
            "gold_plus_lead_count": gold_plus,
            "total_scored_count": total_scored,
            "enrichment_rate_pct": enrichment_rate,
            "active_subscriber_count": active_sub_count,
            "waitlist_count": waitlist_count,
        },
        "top_zips": top_zips,
        "territory_availability": territory_availability,
        "scraper_health": scraper_health,
        "coming_soon": coming_soon,
    }
    if redis_available():
        rset(_cache_key, json.dumps(_result, default=str), ttl_seconds=300)
    return _result



class WaitlistRequest(BaseModel):
    zip_code: str
    vertical: str
    county_id: str = "hillsborough"
    name: str
    email: str
    phone: Optional[str] = None
    sms_opt_in: bool = False
    # waitlist_type is accepted from client but always server-verified
    waitlist_type: Optional[str] = None
    consent_acceptance: Optional[ConsentAcceptanceRequest] = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip().lower()
        if not _EMAIL_RE.match(v):
            raise ValueError("Invalid email address")
        return v

    @field_validator("zip_code")
    @classmethod
    def validate_zip_code(cls, v: str) -> str:
        v = v.strip()
        if not _ZIP_RE.match(v):
            raise ValueError("ZIP code must be exactly 5 digits")
        return v

    @field_validator("vertical")
    @classmethod
    def validate_vertical(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in VALID_VERTICALS:
            raise ValueError(f"Invalid vertical '{v}'. Must be one of: {sorted(VALID_VERTICALS)}")
        return v

    @field_validator("county_id")
    @classmethod
    def validate_county(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in _ALLOWED_WAITLIST_COUNTIES:
            raise ValueError(f"county_id must be one of: {sorted(_ALLOWED_WAITLIST_COUNTIES)}")
        return v

    from pydantic import model_validator

    @model_validator(mode="after")
    def validate_sms_requires_phone(self) -> "WaitlistRequest":
        if self.sms_opt_in and not (self.phone and self.phone.strip()):
            raise ValueError("phone is required when sms_opt_in is True")
        return self

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        return v

    @model_validator(mode="after")
    def sms_opt_in_requires_phone(self) -> "WaitlistRequest":
        if self.sms_opt_in and not self.phone:
            raise ValueError("phone is required when sms_opt_in is True")
        return self


def _client_ip_for_waitlist(request: Request) -> str:
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


@app.post("/api/waitlist", status_code=201)
def join_waitlist(payload: WaitlistRequest, request: Request, db: Session = Depends(get_db)):
    """
    Write a WaitlistEntry. Writes to waitlist_entries table.
    waitlist_type is server-resolved — client value is a hint only.
    """
    enforce_or_429(request, scope="waitlist_post", limit=3, window_seconds=3600)

    # Normalize phone
    phone_e164: Optional[str] = None
    if payload.phone:
        phone_e164 = normalize_phone(payload.phone)
        if not phone_e164:
            raise HTTPException(
                status_code=422,
                detail={"error": "invalid_phone",
                        "message": "Phone must be a valid US number (E.164)."},
            )

    # Server-authoritative waitlist_type — ADR-0004
    try:
        server_wl_type, _, _, _ = _resolve_county_landing_state(payload.county_id, db)
    except HTTPException as e:
        if e.status_code == 404:
            # County has no waitlistable state — still allow the submit,
            # default to sold_out so the row lands somewhere useful.
            server_wl_type = "sold_out"
        else:
            raise

    if payload.waitlist_type and payload.waitlist_type != server_wl_type:
        logger.warning(
            "waitlist_type mismatch county=%s client=%s server=%s email_hash=%s",
            payload.county_id,
            payload.waitlist_type,
            server_wl_type,
            hash(payload.email),
        )

    signup_ip = _client_ip_for_waitlist(request)

    # ── T&C / TCPA Consent Validation ──────────────────────────────────────────
    consent = payload.consent_acceptance
    if not consent or not consent.terms_accepted:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "terms_not_accepted",
                "message": "You must accept the Terms & Conditions and Privacy Policy to join the waitlist.",
            },
        )
    # TCPA is explicitly optional — no validation required.

    # Dedup by phone — check before insert to give a clean already_registered response
    if phone_e164:
        phone_dupe = db.execute(
            select(WaitlistEntry).where(
                WaitlistEntry.phone_e164 == phone_e164,
                WaitlistEntry.county_id == payload.county_id,
            )
        ).scalar_one_or_none()
        if phone_dupe:
            return {"status": "already_registered",
                    "zip_code": payload.zip_code,
                    "email": payload.email}

    try:
        entry = WaitlistEntry(
            zip_code=payload.zip_code,
            vertical=payload.vertical,
            county_id=payload.county_id,
            name=payload.name,
            email=payload.email,
            phone_e164=phone_e164,
            sms_opt_in=bool(payload.sms_opt_in and phone_e164),
            waitlist_type=server_wl_type,
            signup_ip=signup_ip,
            status="waiting",
        )
        db.add(entry)
        db.flush()  # get id before TCPA write

        # TCPA: write SmsOptIn row when phone + consent present
        if phone_e164 and payload.sms_opt_in:
            existing_opt_in = db.execute(
                select(SmsOptIn).where(SmsOptIn.phone == phone_e164)
            ).scalar_one_or_none()
            if not existing_opt_in:
                opt_in = SmsOptIn(
                    phone=phone_e164,
                    subscriber_id=None,
                    source="waitlist_form",
                    opt_in_message=(
                        f"Waitlist signup for {payload.county_id} county — "
                        f"user checked SMS consent box. "
                        f"Reply STOP to unsubscribe, HELP for help."
                    ),
                    ip_address=signup_ip,
                    consent_scope="other",
                )
                db.add(opt_in)

        # ── Write ConsentAcceptance row ─────────────────────────────────────────
        try:
            from datetime import datetime

            def _parse_iso(s):
                if not s:
                    return None
                try:
                    return datetime.fromisoformat(s.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    return None

            ca = ConsentAcceptance(
                email=payload.email,
                phone=phone_e164,
                waitlist_entry_id=entry.id,
                terms_version=consent.terms_version or "2026.06",
                privacy_version=consent.privacy_version or "2026.06",
                accepted_at=datetime.now(timezone.utc),
                source_flow="waitlist",
                ip_address=signup_ip,
                user_agent=consent.user_agent,
                modal_opened_at=_parse_iso(consent.modal_opened_at),
                modal_scrolled_to_end_at=_parse_iso(consent.modal_scrolled_to_end_at),
                accepted_text_hash=consent.accepted_text_hash or "",
                tcpa_consent_text=consent.tcpa_consent_text if consent.tcpa_accepted else None,
                tcpa_consent_version=consent.tcpa_consent_version if consent.tcpa_accepted else None,
                tcpa_checked_at=datetime.now(timezone.utc) if consent.tcpa_accepted else None,
                consent_scope="marketing" if consent.tcpa_accepted else None,
                not_condition_of_purchase_ack=consent.tcpa_accepted or None,
                county_id=payload.county_id,
            )
            db.add(ca)
        except Exception:
            logger.warning("ConsentAcceptance write failed (non-fatal):", exc_info=True)

        db.commit()
    except IntegrityError:
        db.rollback()
        logger.info("Duplicate waitlist entry for %s / %s / %s / %s",
                    payload.zip_code, payload.vertical, payload.county_id, payload.email)
        return {"status": "already_registered",
                "zip_code": payload.zip_code,
                "email": payload.email}
    except OperationalError:
        db.rollback()
        logger.error("DB error committing waitlist entry", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable",
                                                      "message": "Database temporarily unavailable"})

    return {
        "status": "added",
        "zip_code": payload.zip_code,
        "email": payload.email,
        "waitlist_type": server_wl_type,
    }


# ---------------------------------------------------------------------------
# GET /api/sample-leads — 3 top-scored properties for a ZIP (phone blurred)
# ---------------------------------------------------------------------------

@app.get("/api/sample-leads")
def sample_leads(
    zip_code: str,
    vertical: str = "roofing",
    county_id: str = "hillsborough",
    feed_uuid: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    """
    Returns up to 3 real top-scored properties from a ZIP. Phone blurred for
    anonymous visitors; when feed_uuid is supplied and the subscriber has a
    SentLead row for a property (e.g. via the $4 unlock), that lead's contact
    is unblurred and unlocked=true is returned.
    """
    if not _ZIP_RE.match(zip_code):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_zip", "message": "ZIP code must be exactly 5 digits"},
        )
    if vertical not in VALID_VERTICALS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"vertical must be one of: {sorted(VALID_VERTICALS)}"},
        )

    try:
        score_col = DistressScore.vertical_scores[vertical].as_float()
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"vertical must be one of: {sorted(VALID_VERTICALS)}"},
        )

    # Resolve the viewing subscriber (if any) so we can mark unlocked leads
    viewing_subscriber: Optional[Subscriber] = None
    if feed_uuid:
        try:
            viewing_subscriber = db.execute(
                select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
            ).scalar_one_or_none()
        except OperationalError:
            logger.error("DB error resolving feed_uuid for sample leads", exc_info=True)
            viewing_subscriber = None

    from src.utils.lead_filters import has_contact_filter, phone_priority_order
    contact_clause = has_contact_filter(get_settings())

    filters = [
        Property.zip == zip_code,
        Property.county_id == county_id,
        DistressScore.qualified == True,
    ]
    if contact_clause is not None:
        filters.append(contact_clause)

    # Cross-trade exclusivity filter
    now = datetime.now(timezone.utc)
    excl_ids = lead_exclusivity.get_exclusive_property_ids(
        db, county_id, now, zip_code=zip_code, exclude_trade=vertical
    )
    if excl_ids:
        filters.append(Property.id.not_in(excl_ids))

    try:
        rows = db.execute(
            select(Property, DistressScore, Owner)
            .join(DistressScore, DistressScore.property_id == Property.id)
            .outerjoin(Owner, Owner.property_id == Property.id)
            .where(and_(*filters))
            .order_by(*phone_priority_order(score_col))
            .limit(3)
        ).all()
    except OperationalError:
        logger.error("DB error fetching sample leads", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    # Resolve unlocked property IDs for this subscriber
    unlocked_ids: set[int] = set()
    if viewing_subscriber and rows:
        property_ids = [prop.id for prop, _, _ in rows]
        try:
            unlocked_rows = db.execute(
                select(SentLead.property_id).where(
                    SentLead.subscriber_id == viewing_subscriber.id,
                    SentLead.property_id.in_(property_ids),
                )
            ).scalars().all()
            unlocked_ids = set(unlocked_rows)
        except OperationalError:
            logger.error("DB error fetching unlocks for sample leads", exc_info=True)

    leads = []
    for prop, score, owner in rows:
        try:
            inc = db.execute(
                select(Incident)
                .where(Incident.property_id == prop.id)
                .order_by(desc(Incident.incident_date))
                .limit(1)
            ).scalar_one_or_none()
        except OperationalError:
            inc = None  # non-fatal — degrade gracefully

        is_unlocked = prop.id in unlocked_ids
        owner_phone, owner_phone_quality = _resolve_phone_with_quality(owner)
        owner_email = (owner.email_1 or owner.email_2) if owner else None
        _visible_tier, _ = _visible_tier_fields(prop, score)

        leads.append({
            "property_id": prop.id,
            "address": prop.address,
            "city": prop.city,
            "zip": prop.zip,
            "year_built": prop.year_built,
            "sq_ft": prop.sq_ft,
            "cds_score": float(score.final_cds_score) if score.final_cds_score else None,
            "vertical_score": score.vertical_scores.get(vertical) if score.vertical_scores else None,
            "lead_tier": _visible_tier,
            "distress_types": score.distress_types,
            "latest_incident": inc.incident_type if inc else None,
            "latest_incident_date": inc.incident_date.isoformat() if inc and inc.incident_date else None,
            "unlocked": is_unlocked,
            "owner_name": owner.owner_name if (owner and is_unlocked) else None,
            "phone": owner_phone if is_unlocked else "•••-•••-••••",
            "phone_quality": owner_phone_quality if is_unlocked else None,
            "email": owner_email if is_unlocked else None,
        })

    # Register this visitor as an active viewer and return the live count
    # in the same response so the frontend has it immediately (no race).
    active_viewers = 0
    try:
        from src.services.urgency_engine import _increment_zip_counter, get_active_count
        _increment_zip_counter(zip_code, ttl_seconds=1200)  # 20-min active window
        active_viewers = get_active_count(zip_code)
    except Exception:
        pass  # never block the response over a Redis counter

    return {"zip_code": zip_code, "vertical": vertical, "leads": leads, "active_viewers": active_viewers}


# ---------------------------------------------------------------------------
# POST /api/lead-pack/checkout — Create PaymentIntent for a lead pack purchase
# ---------------------------------------------------------------------------

class LeadPackCheckoutRequest(BaseModel):
    feed_uuid: str
    zip_code: str
    vertical: str
    county_id: str = "hillsborough"
    attribution: Optional[dict] = None  # Meta Ads attribution (utm_*, campaign_id, fbclid, ...)


@app.post("/api/lead-pack/checkout")
def lead_pack_checkout(payload: LeadPackCheckoutRequest, request: Request, db: Session = Depends(get_db)):
    """
    Create a Stripe PaymentIntent for a $99 lead pack.
    Returns { client_secret, publishable_key, amount, currency }.
    
    Checkout gates:
    - ZIP must have at least 5 available qualified leads after exclusivity filtering
    - Subscriber's vertical must match their subscription
    """
    _s = get_settings()

    if not _s.active_stripe_secret_key:
        raise HTTPException(status_code=503, detail={"error": "payment_unavailable", "message": "Payment not configured"})

    # Validate subscriber
    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == payload.feed_uuid)
        ).scalar_one_or_none()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not subscriber or subscriber.status not in ("active", "grace"):
        raise HTTPException(status_code=403, detail={"error": "unauthorized", "message": "Active subscription required"})

    if payload.vertical not in VALID_VERTICALS:
        raise HTTPException(status_code=400, detail={"error": "invalid_vertical", "message": f"Unknown vertical '{payload.vertical}'"})

    # Reject if subscriber's vertical doesn't match requested vertical
    if subscriber.vertical and subscriber.vertical != payload.vertical:
        raise HTTPException(status_code=400, detail={"error": "vertical_mismatch", "message": f"Your subscription is for {subscriber.vertical}, not {payload.vertical}"})

    # Reject if subscriber already owns this ZIP — they get those leads free
    owned = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.subscriber_id == subscriber.id,
            ZipTerritory.zip_code == payload.zip_code,
            ZipTerritory.vertical == payload.vertical,
            ZipTerritory.status.in_(["locked", "grace"]),
        )
    ).scalar_one_or_none()
    if owned:
        raise HTTPException(status_code=400, detail={"error": "zip_already_owned", "message": "You already receive leads for this ZIP in your feed."})

    # Checkout gate 1: county must be launched (ADR 0002 — derived status).
    from src.utils.county_config import is_county_launched
    if not is_county_launched(payload.county_id, db):
        raise HTTPException(status_code=403, detail={
            "error": "county_not_launched",
            "message": "This county is not yet live for lead pack purchases.",
        })

    # Checkout gate 2: verify ≥5 available qualified leads (each with a contact
    # on file) exist after cross-trade exclusivity. Uses the SAME predicate as
    # the webhook reservation so the gate cannot lie. The authoritative quality
    # bar is the post-payment 100% Tracerfy Quality Floor (ADR 0018) — there is
    # deliberately no pre-payment contactability-percentage gate here.
    from src.services.lead_exclusivity import get_exclusive_property_ids
    from src.core.models import DistressScore, Owner
    from src.utils.lead_filters import has_contact_filter, phone_priority_order
    from datetime import datetime, timezone as tz

    now = datetime.now(tz.utc)
    excl_ids = get_exclusive_property_ids(db, payload.county_id, now, zip_code=payload.zip_code)

    try:
        score_col = DistressScore.vertical_scores[payload.vertical].as_float()
    except KeyError:
        raise HTTPException(status_code=400, detail={"error": "invalid_vertical", "message": f"Unknown vertical '{payload.vertical}'"})

    filters = [
        Property.zip == payload.zip_code,
        Property.county_id == payload.county_id,
        DistressScore.qualified == True,
    ]
    contact_clause = has_contact_filter(_s)
    if contact_clause is not None:
        filters.append(contact_clause)
    if excl_ids:
        filters.append(Property.id.not_in(excl_ids))

    candidate_ids = db.execute(
        select(Property.id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(and_(*filters))
        .order_by(*phone_priority_order(score_col))
        .limit(5)
    ).scalars().all()

    if len(candidate_ids) < 5:
        raise HTTPException(status_code=422, detail={
            "error": "insufficient_leads",
            "message": f"Only {len(candidate_ids)} qualified leads available for this ZIP/vertical combination",
        })

    # Look up price amount from Stripe
    stripe.api_key = _s.active_stripe_secret_key.get_secret_value()
    price_id = _s.active_stripe_price("lead_pack")
    if not price_id:
        raise HTTPException(status_code=503, detail={"error": "price_not_configured", "message": "Lead pack price not configured"})

    try:
        price = stripe.Price.retrieve(price_id)
        amount = price["unit_amount"]
        currency = price["currency"]
    except stripe.StripeError as exc:
        logger.error("Stripe error retrieving lead pack price: %s", exc)
        raise HTTPException(status_code=502, detail={"error": "payment_unavailable", "message": "Could not retrieve price"})

    lead_pack_metadata = {
        "product":   "lead_pack",
        "feed_uuid": payload.feed_uuid,
        "zip_code":  payload.zip_code,
        "vertical":  payload.vertical,
        "county_id": payload.county_id,
    }
    # Meta Ads attribution + buyer IP/UA captured from the buyer's request.
    lead_pack_metadata.update(_attribution_stripe_metadata(request, payload.attribution))

    try:
        intent = stripe.PaymentIntent.create(
            amount=amount,
            currency=currency,
            metadata=lead_pack_metadata,
            description=f"Lead Pack — {payload.zip_code} / {payload.vertical}",
        )
    except stripe.StripeError as exc:
        logger.error("Stripe error creating lead pack PaymentIntent: %s", exc)
        raise HTTPException(status_code=502, detail={"error": "payment_unavailable", "message": "Could not create payment"})

    return {
        "client_secret":    intent["client_secret"],
        "publishable_key":  _s.active_stripe_publishable_key,
        "amount":           amount,
        "currency":         currency,
    }


# ---------------------------------------------------------------------------
# POST /api/checkout/auto-mode — Stripe Checkout Session for Auto Mode add-on
# ---------------------------------------------------------------------------
# Authed-user pattern (feed_uuid in body). Creates a Stripe subscription
# checkout for the $79–$99/mo Auto Mode add-on (Starter tier's paywall path).
# Growth/Power wallets get Auto Mode included — they hit the toggle endpoint
# below instead. Webhook entitlement activation lives in stripe_webhooks.

class AutoModeCheckoutRequest(BaseModel):
    feed_uuid: str


@app.post("/api/checkout/auto-mode")
def auto_mode_checkout(payload: AutoModeCheckoutRequest, db: Session = Depends(get_db)):
    _s = get_settings()
    if not _s.active_stripe_secret_key:
        raise HTTPException(status_code=503, detail={"error": "payment_unavailable", "message": "Payment not configured"})

    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == payload.feed_uuid)
        ).scalar_one_or_none()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not subscriber or subscriber.status not in ("active", "grace"):
        raise HTTPException(status_code=403, detail={"error": "unauthorized", "message": "Active subscription required"})

    # Confirmed policy: do NOT create the Stripe customer inline. Subscriber
    # must already have one (means they paid for a main subscription first).
    if not subscriber.stripe_customer_id:
        raise HTTPException(
            status_code=400,
            detail={"error": "no_stripe_customer", "message": "An active subscription is required before adding Auto Mode."},
        )

    price_id = _s.active_stripe_price("auto_mode")
    if not price_id:
        raise HTTPException(status_code=503, detail={"error": "price_not_configured", "message": "Auto Mode price not configured"})

    stripe.api_key = _s.active_stripe_secret_key.get_secret_value()
    base_url = _s.app_base_url.rstrip("/")

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            ui_mode="embedded",
            customer=subscriber.stripe_customer_id,
            line_items=[{"price": price_id, "quantity": 1}],
            return_url=f"{base_url}/dashboard/{payload.feed_uuid}/settings?auto_mode=success&session_id={{CHECKOUT_SESSION_ID}}",
            metadata={
                "product":        "auto_mode_addon",
                "subscriber_id":  str(subscriber.id),
                "feed_uuid":      payload.feed_uuid,
            },
            subscription_data={
                "metadata": {
                    "product":       "auto_mode_addon",
                    "subscriber_id": str(subscriber.id),
                }
            },
        )
    except stripe.StripeError as exc:
        logger.error("Stripe error creating auto_mode checkout: %s", exc)
        raise HTTPException(status_code=502, detail={"error": "payment_unavailable", "message": "Could not create checkout session"})

    return {
        "client_secret":   session.client_secret,
        "publishable_key": _s.active_stripe_publishable_key,
        "session_id":      session.id,
    }


# ---------------------------------------------------------------------------
# POST /api/auto-mode/toggle — Enable/disable Auto Mode (REST counterpart of SMS AUTO ON/OFF)
# ---------------------------------------------------------------------------
# Routes through src.services.auto_mode.toggle() which enforces tier gating.
# Returns 402 Payment Required when a non-entitled Starter tries to enable.

class AutoModeToggleRequest(BaseModel):
    feed_uuid: str
    enabled: bool


@app.post("/api/auto-mode/toggle")
def auto_mode_toggle(payload: AutoModeToggleRequest, db: Session = Depends(get_db)):
    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == payload.feed_uuid)
        ).scalar_one_or_none()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})
    if not subscriber:
        raise HTTPException(status_code=404, detail={"error": "subscriber_not_found", "message": "Unknown feed_uuid"})

    from src.services.auto_mode import toggle as auto_mode_toggle_fn
    try:
        auto_mode_toggle_fn(subscriber.id, payload.enabled, db)
    except PermissionError as exc:
        raise HTTPException(
            status_code=402,
            detail={"error": "requires_addon", "message": str(exc)},
        )
    db.commit()
    return {"auto_mode_enabled": payload.enabled}


# ---------------------------------------------------------------------------
# GET /api/lead-pack/{purchase_id} — Retrieve lead pack delivery (fallback)
# ---------------------------------------------------------------------------

@app.get("/api/lead-pack-history/{feed_uuid}")
def lead_pack_history(feed_uuid: str, db: Session = Depends(get_db)):
    """Return all lead pack purchases for a subscriber."""
    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
        ).scalar_one_or_none()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable"})

    if not subscriber:
        raise HTTPException(status_code=404, detail={"error": "not_found"})

    try:
        purchases = db.execute(
            select(LeadPackPurchase)
            .where(LeadPackPurchase.subscriber_id == subscriber.id)
            .order_by(desc(LeadPackPurchase.purchased_at))
        ).scalars().all()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable"})

    from datetime import datetime as _dt, timezone as _tz
    now = _dt.now(_tz.utc)
    return {
        "purchases": [
            {
                "id":              p.id,
                "zip_code":        p.zip_code,
                "vertical":        p.vertical,
                "status":          p.status,
                "purchased_at":    p.purchased_at.isoformat() if p.purchased_at else None,
                "exclusive_until": p.exclusive_until.isoformat() if p.exclusive_until else None,
                "exclusive_active": bool(p.exclusive_until and p.exclusive_until.replace(tzinfo=_tz.utc) > now),
                "lead_count":      len(p.lead_ids) if p.lead_ids else 0,
            }
            for p in purchases
        ]
    }


@app.get("/api/lead-pack/{purchase_id}")
def lead_pack_detail(purchase_id: int, db: Session = Depends(get_db)):
    """
    Return the 5 leads for a given lead pack purchase.
    Authenticated by purchase_id (secret by obscurity — no subscriber login needed for MVP).
    Used as a fallback if the delivery email is not received.
    """
    try:
        purchase = db.execute(
            select(LeadPackPurchase).where(LeadPackPurchase.id == purchase_id)
        ).scalar_one_or_none()
    except OperationalError:
        logger.error("DB error fetching lead pack %s", purchase_id, exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not purchase:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Lead pack not found"})

    if not purchase.lead_ids:
        return {
            "purchase_id": purchase_id,
            "status": purchase.status,
            "leads": [],
        }

    try:
        score_col = DistressScore.vertical_scores[purchase.vertical].as_float()
        rows = db.execute(
            select(Property, DistressScore)
            .join(DistressScore, DistressScore.property_id == Property.id)
            .where(Property.id.in_(purchase.lead_ids))
            .order_by(desc(score_col))
        ).all()
    except OperationalError:
        logger.error("DB error fetching leads for pack %s", purchase_id, exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    leads = []
    for prop, score in rows:
        _visible_tier, _ = _visible_tier_fields(prop, score)
        leads.append({
            "property_id": prop.id,
            "address": prop.address,
            "city": prop.city,
            "state": prop.state,
            "zip": prop.zip,
            "property_type": prop.property_type,
            "year_built": prop.year_built,
            "sq_ft": prop.sq_ft,
            "cds_score": float(score.final_cds_score) if score.final_cds_score else None,
            "vertical_score": score.vertical_scores.get(purchase.vertical) if score.vertical_scores else None,
            "lead_tier": _visible_tier,
            "distress_types": score.distress_types,
        })

    return {
        "purchase_id": purchase_id,
        "zip_code": purchase.zip_code,
        "vertical": purchase.vertical,
        "status": purchase.status,
        "purchased_at": purchase.purchased_at.isoformat() if purchase.purchased_at else None,
        "exclusive_until": purchase.exclusive_until.isoformat() if purchase.exclusive_until else None,
        "leads": leads,
    }


# ---------------------------------------------------------------------------
# POST /api/hot-lead-unlock — Create Stripe checkout for hot lead unlock
# ---------------------------------------------------------------------------

class HotLeadUnlockRequest(BaseModel):
    feed_uuid: str
    lead_id: str
    reduced: bool = False

@app.post("/api/hot-lead-unlock")
def hot_lead_unlock(payload: HotLeadUnlockRequest, db: Session = Depends(get_db)):
    """Create a one-time Stripe Checkout Session ($150 or $99 reduced) for hot lead unlock."""
    subscriber = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == payload.feed_uuid)
    ).scalar_one_or_none()
    if not subscriber:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    if subscriber.status not in ("active", "grace"):
        raise HTTPException(status_code=403, detail="Active subscription required")
    if not subscriber.stripe_customer_id:
        raise HTTPException(status_code=400, detail="No Stripe customer linked")

    from src.services.stripe_service import create_hot_lead_unlock_link
    try:
        result = create_hot_lead_unlock_link(
            subscriber_stripe_customer_id=subscriber.stripe_customer_id,
            lead_id=payload.lead_id,
            reduced=payload.reduced,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    if subscriber.tier == "free":
        try:
            from src.agents.events.ingestion import publish_cora_event
            publish_cora_event({
                "event_type": "abandonment_click_no_complete",
                "subscriber_id": subscriber.id,
                "payload": {
                    "lead_id": payload.lead_id,
                    "vertical": subscriber.vertical or "",
                },
            })
        except Exception:
            pass

    return {"checkout_url": result["url"]}


# ---------------------------------------------------------------------------
# GET /api/synthflow/lead-count — Live lead count for Synthflow agent script
# ---------------------------------------------------------------------------

@app.get("/api/synthflow/lead-count")
def synthflow_lead_count(
    zip_code: str,
    vertical: str = "roofing",
    county_id: str = "hillsborough",
):
    """
    Called by the Synthflow AI agent mid-call to fill the [X] placeholder.
    Returns qualified lead count + top signal type for the given ZIP/vertical.

    Example: GET /api/synthflow/lead-count?zip_code=33601&vertical=roofing
    Response: {"count": 8, "top_signal": "insurance_claims", "zip_available": true}
    """
    if not _ZIP_RE.match(zip_code):
        raise HTTPException(status_code=400, detail={"error": "invalid_zip"})
    if vertical not in VALID_VERTICALS:
        raise HTTPException(status_code=400, detail={"error": "invalid_vertical"})

    from src.services.synthflow_service import get_live_lead_count
    try:
        result = get_live_lead_count(zip_code=zip_code, vertical=vertical, county_id=county_id)
    except Exception:
        logger.error("synthflow lead-count error", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable"})

    return {
        "zip_code": zip_code,
        "vertical": vertical,
        **result,
    }


# ---------------------------------------------------------------------------
# POST /webhooks/synthflow — Post-call outcome from Synthflow / Finetuner.ai
# ---------------------------------------------------------------------------

class SynthflowWebhookPayload(BaseModel):
    """
    Models the Finetuner.ai post-call webhook envelope (also accepts the older
    flat Synthflow shape for backward compatibility).

    Finetuner.ai sends:
      {
        "status": "completed" | "failed" | "no-answer" | ...
        "lead":   { "name": "...", "phone_number": "...", "prompt_variables": {...} },
        "call":   { "status": "...", "end_call_reason": "...", "call_id": "...",
                    "duration": 113, "recording_url": "...", "transcript": "...", ... },
        "executed_actions":    { ... },
        "analysis":            { "goal": "true|partial|false", "call_summary_feedback": "...", ... },
        "metadata":            { ... },
        "collected_variables": { "<name>": { "value": ..., "collected": true } }
      }

    All outcome-critical fields are optional so the handler degrades gracefully
    when a field is missing.
    """
    # ── Finetuner.ai envelope ─────────────────────────────────────────────────
    status: Optional[str] = None              # top-level call disposition
    error_message: Optional[str] = None
    lead: Optional[dict] = None               # { name, phone_number, prompt_variables }
    call: Optional[dict] = None               # { status, end_call_reason, call_id, duration, ... }
    executed_actions: Optional[dict] = None
    analysis: Optional[dict] = None
    metadata: Optional[dict] = None
    collected_variables: Optional[dict] = None  # { <name>: { value, collected } }

    # ── Legacy/flat Synthflow shape (kept for backward compat) ────────────────
    call_id: Optional[str] = None
    to: Optional[str] = None
    to_number: Optional[str] = None
    prospect_phone: Optional[str] = None
    phone_number: Optional[str] = None
    phone: Optional[str] = None
    caller_phone: Optional[str] = None
    contact_phone: Optional[str] = None
    outcome: Optional[str] = None
    call_status: Optional[str] = None
    zip_code: Optional[str] = None
    vertical: Optional[str] = None
    prospect_name: Optional[str] = None
    notes: Optional[str] = None
    duration: Optional[int] = None
    recording_url: Optional[str] = None
    variables: Optional[dict] = None
    call_variables: Optional[dict] = None

    # ── Helpers ───────────────────────────────────────────────────────────────

    @property
    def _flat_collected(self) -> dict:
        """
        Flatten `collected_variables` (Finetuner.ai shape) into a plain dict.
        Each entry is `{ "value": X, "collected": bool }` — pull the value out
        and ignore non-collected slots.
        """
        out: dict = {}
        for key, slot in (self.collected_variables or {}).items():
            if isinstance(slot, dict):
                if slot.get("collected") and slot.get("value") is not None:
                    out[key] = slot.get("value")
            else:
                out[key] = slot
        return out

    @property
    def _flat_executed(self) -> dict:
        """
        Pull return_values out of executed_actions[*].return_value so we can
        access values from extract_info_* actions by their identifier.
        """
        out: dict = {}
        for action in (self.executed_actions or {}).values():
            if not isinstance(action, dict):
                continue
            rv = action.get("return_value")
            if isinstance(rv, dict):
                for k, v in rv.items():
                    if v is not None:
                        # normalize key: "zip code" → "zip_code"
                        out[k.replace(" ", "_").lower()] = v
        return out

    @property
    def _vars(self) -> dict:
        """
        Merged variable view across every container Finetuner/Synthflow uses:
          1. legacy `call_variables`
          2. legacy `variables`
          3. Finetuner `collected_variables` (after flattening)
          4. extract_info_* return_values from executed_actions
          5. `lead.prompt_variables`
        Later sources win on conflict.
        """
        prompt_vars = (self.lead or {}).get("prompt_variables") or {}
        return {
            **(self.call_variables or {}),
            **(self.variables or {}),
            **self._flat_collected,
            **self._flat_executed,
            **(prompt_vars if isinstance(prompt_vars, dict) else {}),
        }

    @property
    def resolved_phone(self) -> Optional[str]:
        v = self._vars
        lead = self.lead or {}
        call = self.call or {}
        return (
            self.prospect_phone or self.phone_number or self.to or self.to_number
            or self.phone or self.caller_phone or self.contact_phone
            or lead.get("phone_number") or lead.get("phone")
            or call.get("to") or call.get("to_number") or call.get("phone_number")
            or v.get("prospect_phone") or v.get("phone_number")
            or v.get("to") or v.get("phone")
        )

    @property
    def resolved_call_id(self) -> Optional[str]:
        return self.call_id or (self.call or {}).get("call_id")

    @property
    def resolved_outcome(self) -> str:
        """
        Map Finetuner/Synthflow call disposition to our outcome taxonomy.
        Priority: agent-set `outcome` variable → end_call_reason → call.status → top-level status.
        """
        outcome = self.outcome or self._vars.get("outcome")
        if outcome:
            return outcome

        call = self.call or {}
        end_reason = (call.get("end_call_reason") or "").lower()
        if end_reason == "voicemail_message_left":
            return "voicemail"
        if end_reason == "voicemail":
            return "no_answer"
        if end_reason == "human_pick_up_cut_off":
            return "no_answer"

        status_map = {
            "no_answer":        "no_answer",
            "no-answer":        "no_answer",
            "busy":             "no_answer",
            "voicemail":        "voicemail",
            "hangup_on_voicemail":   "no_answer",
            "left_voicemail":   "voicemail",
            "failed":           "no_answer",
            "completed":        "completed",
        }
        cs = (
            self.call_status
            or call.get("status")
            or self.status
            or self._vars.get("call_status")
            or self._vars.get("status")
            or ""
        ).lower()
        return status_map.get(cs, "completed")


@app.get("/webhooks/synthflow/sample-leads-text")
def synthflow_sample_leads_text(
    zip_code: str,
    vertical: str = "roofing",
):
    """
    Called by fine-tuner.ai during a call when the prospect says YES to sample leads.
    Returns a formatted SMS message string — agent maps it into the Send SMS action.

    Example: GET /webhooks/synthflow/sample-leads-text?zip_code=33612&vertical=roofing
    Returns: { "message": "Forced Action — Roofing leads in 33612:\n1. 123 Main St..." }
    """
    from src.services.sample_leads_sms import get_sample_leads, format_sms_body
    leads = get_sample_leads(zip_code=zip_code, vertical=vertical)
    message = format_sms_body(leads, zip_code=zip_code, vertical=vertical)
    return {"message": message, "lead_count": len(leads)}


@app.post("/webhooks/synthflow", status_code=200)
async def synthflow_webhook(request: Request):
    """
    Receives post-call events from Synthflow / Finetuner.ai.

    On each call end:
      1. Resolves the prospect's phone number
      2. Looks up or creates the GHL contact
      3. Applies outcome tags that trigger GHL automations:
           sample_leads_requested → GHL sends SMS with 3 blurred sample leads
           demo_requested         → GHL sends Calendly link SMS
           not_interested         → removes from active sequences

    Configure in Synthflow/Finetuner as the post-call webhook URL:
        https://your-domain.com/webhooks/synthflow
    """
    raw_body = await request.body()
    try:
        raw_json = json.loads(raw_body.decode("utf-8") or "{}")
    except Exception:
        raw_json = {"_unparseable": raw_body.decode("utf-8", errors="replace")[:2000]}

    from src.services.webhook_log import log_webhook_event
    log_webhook_event(
        source="synthflow",
        event_type=(raw_json.get("event") or raw_json.get("event_type") or "post_call"),
        source_event_id=(raw_json.get("call_id") or raw_json.get("id")),
        status="received",
        payload=raw_json,
        payload_kind="synthflow",
    )

    try:
        payload = SynthflowWebhookPayload(**raw_json)
    except Exception as exc:
        logger.error("[Synthflow webhook] payload validation failed: %s — keys=%s", exc, list(raw_json.keys()))
        log_webhook_event(
            source="synthflow", event_type="payload_validation_failed",
            status="failed", status_detail=str(exc)[:500],
        )
        return {"status": "error", "reason": "invalid_payload"}

    phone = payload.resolved_phone
    if not phone:
        logger.warning(
            "[Synthflow webhook] no phone resolved — top_level_keys=%s var_keys=%s",
            list(raw_json.keys()), list(payload._vars.keys()),
        )
        return {"status": "ignored", "reason": "no phone"}

    v = payload._vars
    lead = payload.lead or {}
    from src.services.synthflow_service import process_call_outcome
    try:
        result = process_call_outcome(
            prospect_phone=phone,
            outcome=payload.resolved_outcome,
            vertical=payload.vertical or v.get("vertical") or "roofing",
            zip_code=payload.zip_code or v.get("zip_code") or v.get("zip") or "",
            prospect_name=payload.prospect_name or v.get("prospect_name") or lead.get("name") or "",
            notes=payload.notes or v.get("notes") or "",
        )
    except Exception:
        logger.error("[Synthflow webhook] processing error for %s", phone, exc_info=True)
        # Always return 200 to Synthflow — retries on non-200 flood the queue
        return {"status": "error", "reason": "internal"}

    logger.info(
        "[Synthflow webhook] call_id=%s phone=%s outcome=%s contact=%s tags=%s",
        payload.resolved_call_id, phone, payload.resolved_outcome,
        result.get("contact_id"), result.get("tags_applied"),
    )
    return {"status": "ok", **result}


# ---------------------------------------------------------------------------
# POST /webhooks/synthflow/inbound — Synthflow inbound missed-call signup
# ---------------------------------------------------------------------------

class SynthflowInboundPayload(BaseModel):
    """
    Fields the Synthflow inbound agent posts after the call ends.

    Synthflow's native post-call webhook nests the data:
      - phone   → lead.phone_number
      - call_id → call.call_id
      - slots   → collected_variables.<name>.value  (zip_code, vertical, ...)
      - actions → executed_actions.<name>.return_value
    We also keep the flat top-level keys for direct/custom posts and tests.
    The resolved_* properties read flat first, then fall back to the nested
    Synthflow shape.
    """
    # Flat (direct-post / test compatibility)
    phone: Optional[str] = None
    from_number: Optional[str] = None
    caller_phone: Optional[str] = None
    user_phone_number: Optional[str] = None
    zip_code: Optional[str] = None
    zip: Optional[str] = None
    vertical: Optional[str] = None
    call_id: Optional[str] = None
    event_id: Optional[str] = None
    id: Optional[str] = None
    # Synthflow native nested objects
    lead: Optional[Dict[str, Any]] = None
    call: Optional[Dict[str, Any]] = None
    collected_variables: Optional[Dict[str, Any]] = None
    executed_actions: Optional[Dict[str, Any]] = None
    # Extra context (ignored but accepted to avoid validation errors on unknown fields)
    model_config = {"extra": "allow"}

    @staticmethod
    def _slot(container: Optional[Dict[str, Any]], *keys: str) -> Optional[str]:
        """Pull a value from a Synthflow slot dict ({name: {"value": ...}} or {name: ...})."""
        if not isinstance(container, dict):
            return None
        for key in keys:
            v = container.get(key)
            if isinstance(v, dict):
                v = v.get("value") or v.get("return_value")
            if v not in (None, ""):
                return str(v)
        return None

    @property
    def resolved_phone(self) -> Optional[str]:
        # Flat top-level keys (Synthflow post-call, direct-post, or tests)
        flat = self.phone or self.from_number or self.caller_phone or self.user_phone_number
        if flat:
            return flat
        # Nested Synthflow post-call shape: lead.phone_number
        if isinstance(self.lead, dict):
            lead_phone = self.lead.get("phone_number") or self.lead.get("phone")
            if lead_phone:
                return lead_phone
        # Nested call / call_inbound (varies by webhook type)
        for container_key in ("call", "call_inbound"):
            container = getattr(self, container_key, None)
            if isinstance(container, dict):
                cpn = container.get("from_number") or container.get("phone_number") or container.get("phone")
                if cpn:
                    return cpn
        return None

    @property
    def _transcript(self) -> Any:
        return self.call.get("transcript") if isinstance(self.call, dict) else None

    @property
    def resolved_zip(self) -> Optional[str]:
        structured = (
            self.zip_code or self.zip
            or self._slot(self.collected_variables, "zip_code", "zip")
            or self._slot(self.executed_actions, "zip_code", "zip")
            or self._slot((self.lead or {}).get("prompt_variables"), "zip_code", "zip")
        )
        if structured:
            return structured
        # Fallback: flat-prompt agent has no Flow-Designer slots → parse transcript.
        from src.services.synthflow_transcript import extract_zip
        return extract_zip(self._transcript)

    @property
    def resolved_vertical(self) -> Optional[str]:
        structured = (
            self.vertical
            or self._slot(self.collected_variables, "vertical", "trade")
            or self._slot(self.executed_actions, "vertical", "trade")
            or self._slot((self.lead or {}).get("prompt_variables"), "vertical", "trade")
        )
        if structured:
            return structured
        from src.services.synthflow_transcript import extract_vertical
        return extract_vertical(self._transcript)

    @property
    def resolved_call_id(self) -> Optional[str]:
        flat = self.call_id or self.event_id or self.id
        if flat:
            return flat
        if isinstance(self.call, dict):
            return self.call.get("call_id") or self.call.get("id")
        return None


def _verify_synthflow_secret(request: Request) -> bool:
    """Accept X-Synthflow-Secret or Authorization: Bearer <secret>."""
    settings_obj = get_settings()
    secret = settings_obj.synthflow_webhook_secret
    if secret is None:
        # Not configured — log a warning but allow through (dev/unconfigured envs).
        logger.warning("[SynthflowInbound] SYNTHFLOW_WEBHOOK_SECRET not set — auth skipped")
        return True
    expected = secret.get_secret_value()
    # Header: X-Synthflow-Secret: <secret>
    if request.headers.get("X-Synthflow-Secret") == expected:
        return True
    # Header: Authorization: Bearer <secret>
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer ") and auth[7:] == expected:
        return True
    return False


@app.post("/webhooks/synthflow/inbound", status_code=200)
async def synthflow_inbound_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Synthflow inbound missed-call signup endpoint (primary inbound path).

    Synthflow posts here after the inbound agent collects the caller's phone,
    ZIP, and vertical. This handler:
      1. Verifies shared-secret auth.
      2. Checks call_id idempotency — replays are 200 no-ops.
      3. Calls onboard_inbound_caller → account create/resolve + SmsOptIn +
         First Leads (marketing SMS) + welcome link (transactional SMS).
      4. Always returns 200 to avoid Synthflow retry floods.

    SLA: First Leads SMS enqueued within 60s of this webhook being received.
    Measurement: webhook_log.created_at → message_outcomes.sent_at (task_type='first_leads').
    """
    from src.services.signup_engine import onboard_inbound_caller
    from src.services.webhook_log import log_webhook_event

    raw_body = await request.body()

    if not _verify_synthflow_secret(request):
        log_webhook_event(
            source="synthflow_inbound",
            event_type="call_ended",
            status="failed",
            status_detail="auth_failed",
        )
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        raw_json = json.loads(raw_body.decode("utf-8") or "{}")
    except Exception:
        raw_json = {}

    # Diagnostic: log safe payload key names (no values) so we can see
    # exactly which fields Synthflow sent in the real webhook body.
    logger.info(
        "[SynthflowInbound] payload keys=%s",
        sorted(  # noqa: C414
            key for key in (raw_json or {}).keys()
            if key not in ("collected_variables", "lead", "call", "call_inbound", "executed_actions")
        ),
    )

    try:
        payload = SynthflowInboundPayload(**raw_json)
    except Exception as exc:
        logger.error("[SynthflowInbound] payload validation failed: %s", exc)
        return {"status": "error", "reason": "invalid_payload"}

    call_id = payload.resolved_call_id
    phone = payload.resolved_phone

    # Idempotency: reject replays of the same call_id.
    if call_id:
        from src.services.webhook_log import already_logged
        if already_logged(source="synthflow_inbound", source_event_id=call_id):
            logger.info("[SynthflowInbound] duplicate call_id=%s — no-op", call_id)
            return {"status": "duplicate", "call_id": call_id}

    log_webhook_event(
        source="synthflow_inbound",
        event_type="call_ended",
        source_event_id=call_id,
        status="received",
        payload=raw_json,
        payload_kind="synthflow",
    )

    if not phone:
        logger.warning("[SynthflowInbound] no phone in payload — ignored")
        return {"status": "ignored", "reason": "no_phone"}

    result = onboard_inbound_caller(
        phone=phone,
        source="missed_call",
        db=db,
        zip_code=payload.resolved_zip,
        vertical=payload.resolved_vertical,
        call_id=call_id,
    )

    logger.info(
        "[SynthflowInbound] call_id=%s phone=%s sub=%s is_new=%s leads=%s capture_complete=%s",
        call_id, phone, result.get("subscriber_id"),
        result.get("is_new"), result.get("lead_count"), result.get("capture_complete"),
    )
    return {"status": "ok", **result}


# ---------------------------------------------------------------------------
# POST /webhooks/synthflow/call-completed — Transcript + outcome ingestion
# ---------------------------------------------------------------------------

class SynthflowCallCompletedPayload(BaseModel):
    call_id: Optional[str] = None
    prospect_phone: str
    call_outcome: Optional[str] = None
    # Expected values: answered | voicemail | no_answer | opt_out | busy | invalid_number
    transcript_text: Optional[str] = None
    recording_url: Optional[str] = None
    duration_seconds: Optional[int] = None
    zip_code: Optional[str] = None
    vertical: Optional[str] = None


@app.post("/webhooks/synthflow/call-completed", status_code=200)
async def synthflow_call_completed(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Receives completed call data from Synthflow (transcript, outcome, recording).
    Stores in synthflow_calls and writes to agent_decisions for Cora learning.
    """
    from src.services.webhook_log import log_webhook_event

    if not _verify_synthflow_secret(request):
        log_webhook_event(source="synthflow", event_type="call_completed_auth_failed", status="failed")
        raise HTTPException(status_code=401, detail="Unauthorized")

    raw_body = await request.body()
    try:
        raw_json = json.loads(raw_body.decode("utf-8") or "{}")
    except Exception:
        return {"status": "error", "reason": "invalid_json"}

    try:
        payload = SynthflowCallCompletedPayload(**raw_json)
    except Exception:
        log_webhook_event(source="synthflow", event_type="call_completed_parse_failed", status="failed")
        return {"status": "error", "reason": "invalid_payload"}

    from src.services.phone_utils import normalize as normalize_phone
    normalized_phone = normalize_phone(payload.prospect_phone)
    if not normalized_phone:
        return {"status": "ignored", "reason": "invalid_phone"}

    # Upsert SynthflowCall
    from src.core.models import SynthflowCall
    from datetime import date as date_type
    if payload.call_id:
        existing_call = db.execute(
            text("SELECT id FROM synthflow_calls WHERE call_id = :cid"),
            {"cid": payload.call_id},
        ).fetchone()
        if existing_call:
            db.execute(
                text("""
                    UPDATE synthflow_calls SET
                        transcript_text  = :transcript,
                        recording_url    = :recording,
                        duration_seconds = :duration,
                        outcome          = :outcome
                    WHERE call_id = :cid
                """),
                {
                    "transcript": payload.transcript_text,
                    "recording":  payload.recording_url,
                    "duration":   payload.duration_seconds,
                    "outcome":    payload.call_outcome,
                    "cid":        payload.call_id,
                },
            )
        else:
            db.add(SynthflowCall(
                prospect_phone=normalized_phone,
                outcome=payload.call_outcome,
                vertical=payload.vertical,
                zip_code=payload.zip_code,
                call_date=date_type.today(),
                call_id=payload.call_id,
                transcript_text=payload.transcript_text,
                recording_url=payload.recording_url,
                duration_seconds=payload.duration_seconds,
            ))
    else:
        db.add(SynthflowCall(
            prospect_phone=normalized_phone,
            outcome=payload.call_outcome,
            vertical=payload.vertical,
            zip_code=payload.zip_code,
            call_date=date_type.today(),
            transcript_text=payload.transcript_text,
            recording_url=payload.recording_url,
            duration_seconds=payload.duration_seconds,
        ))

    # Write to agent_decisions for Cora learning (ORM applies Python-side defaults)
    # decision_id is always a fresh uuid4 — call_id lives in summary JSONB for correlation.
    # Idempotency: if call_id is known, skip duplicate via summary->>'call_id' lookup.
    from uuid import uuid4
    from src.core.models import AgentDecision
    existing_decision = None
    if payload.call_id:
        existing_decision = db.execute(
            text("SELECT 1 FROM agent_decisions WHERE summary->>'call_id' = :cid LIMIT 1"),
            {"cid": payload.call_id},
        ).fetchone()
    if not existing_decision:
        transcript_excerpt = (payload.transcript_text or "")[:500]
        db.add(AgentDecision(
            decision_id=str(uuid4()),
            graph_name="synthflow_ivr",
            event_type=payload.call_outcome,
            terminal_status="completed",
            summary={
                "call_id":           payload.call_id,
                "outcome":           payload.call_outcome,
                "duration_seconds":  payload.duration_seconds,
                "zip_code":          payload.zip_code,
                "vertical":          payload.vertical,
                "transcript_excerpt": transcript_excerpt,
            },
            tokens_used=0,
            cost_usd=0,
            was_autonomous=False,
            requires_approval=False,
        ))

    # IVR opt-out — write to sms_opt_outs (blocks future calls AND SMS)
    if payload.call_outcome == "opt_out":
        from src.services.compliance_gator import record_ivr_opt_out
        record_ivr_opt_out(normalized_phone, db)

    db.commit()

    log_webhook_event(
        source="synthflow",
        event_type="call_completed",
        source_event_id=payload.call_id,
        status="processed",
        payload=raw_json,
        payload_kind="synthflow",
    )
    logger.info(
        "[Synthflow call-completed] phone=%s outcome=%s call_id=%s",
        normalized_phone, payload.call_outcome, payload.call_id,
    )
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# POST /webhooks/ghl/sample-leads — GHL workflow webhook for sample lead SMS
# ---------------------------------------------------------------------------

class GHLSampleLeadsPayload(BaseModel):
    """
    Payload sent by the GHL workflow when the sample_leads_requested tag fires.
    GHL custom webhooks send contact data as top-level fields.
    """
    contact_id:     Optional[str] = Field(default=None, alias="contactId")
    phone:          Optional[str] = None
    zip_code:       Optional[str] = Field(default=None, alias="zipCode")
    vertical:       Optional[str] = None
    first_name:     Optional[str] = Field(default=None, alias="firstName")
    last_name:      Optional[str] = Field(default=None, alias="lastName")

    model_config = {"populate_by_name": True}

    @property
    def prospect_name(self) -> str:
        parts = [self.first_name or "", self.last_name or ""]
        return " ".join(p for p in parts if p).strip()


@app.post("/webhooks/ghl/sample-leads", status_code=200)
async def ghl_sample_leads_webhook(payload: GHLSampleLeadsPayload):
    """
    Triggered by a GHL workflow when contact tag sample_leads_requested is applied.
    Queries top 3 Gold+ leads for the prospect's ZIP/vertical and sends them via SMS.

    Configure in GHL workflow:
        Trigger: Tag added = sample_leads_requested
        Action: Custom Webhook → POST https://forcedactionleads.com/webhooks/ghl/sample-leads
        Body: { "contactId": "{{contact.id}}", "phone": "{{contact.phone}}",
                "zipCode": "{{contact.postalCode}}", "vertical": "{{contact.tags}}",
                "firstName": "{{contact.firstName}}", "lastName": "{{contact.lastName}}" }
    """
    from src.services.webhook_log import log_webhook_event
    log_webhook_event(
        source="ghl_inbound",
        event_type="sample_leads_requested",
        source_event_id=payload.contact_id,
        status="received",
        payload={
            "contactId":  payload.contact_id,
            "zipCode":    payload.zip_code,
            "vertical":   payload.vertical,
        },
        payload_kind="ghl",
    )

    if not payload.contact_id:
        logger.warning("[GHL sample-leads] Missing contact_id — skipping")
        return {"status": "skipped", "reason": "no_contact_id"}

    zip_code = (payload.zip_code or "").strip()
    vertical = (payload.vertical or "roofing").strip().lower()

    if not zip_code:
        logger.warning("[GHL sample-leads] No zip_code for contact %s — skipping", payload.contact_id)
        return {"status": "skipped", "reason": "no_zip_code"}

    try:
        from src.services.sample_leads_sms import send_sample_leads
        result = send_sample_leads(
            contact_id=payload.contact_id,
            zip_code=zip_code,
            vertical=vertical,
            prospect_name=payload.prospect_name,
        )
    except Exception:
        logger.error("[GHL sample-leads] Error for contact %s", payload.contact_id, exc_info=True)
        return {"status": "error"}

    logger.info(
        "[GHL sample-leads] contact=%s zip=%s vertical=%s sent=%s leads=%d",
        payload.contact_id, zip_code, vertical,
        result.get("sent"), result.get("lead_count"),
    )
    return {"status": "ok", **result}


# ── Phase 2B: Twilio inbound SMS ──────────────────────────────────────────────

def _twiml_ok() -> str:
    return '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'


@app.post("/webhooks/telnyx/inbound")
async def telnyx_inbound(request: Request, db: Session = Depends(get_db)):
    """
    Telnyx inbound SMS webhook (replaces /webhooks/twilio/inbound).

    Verifies the Ed25519 signature, extracts (from, body) from the nested
    Telnyx event envelope, then routes through the existing compliance
    handler — STOP keywords still flow to sms_compliance.handle_inbound()
    and product commands still flow to sms_commands.dispatch().

    The handlers downstream are vendor-neutral; only the payload shape and
    signature scheme change here.
    """
    from src.services.sms_compliance import handle_inbound, handle_opt_in_reply, send_sms
    from src.services import sms_commands
    from src.services.telnyx_signature import (
        SIGNATURE_HEADER, TIMESTAMP_HEADER, verify as verify_telnyx_signature,
    )
    from src.services.webhook_log import log_webhook_event
    from fastapi.responses import Response

    raw_body = await request.body()
    settings_obj = get_settings()

    # 1. Signature verification (Ed25519). Reject before parsing the body.
    if not verify_telnyx_signature(
        body=raw_body,
        signature_b64=request.headers.get(SIGNATURE_HEADER),
        timestamp=request.headers.get(TIMESTAMP_HEADER),
        public_key_b64=settings_obj.telnyx_public_key,
    ):
        log_webhook_event(
            source="telnyx_inbound",
            event_type="sms_received",
            status="failed",
            status_detail="signature_verification_failed",
        )
        raise HTTPException(status_code=403, detail="Invalid signature")

    # 2. Parse Telnyx event envelope:
    #    { "data": { "event_type": "message.received",
    #                "payload": { "from": {"phone_number": ...}, "to": {...}, "text": ... } } }
    try:
        envelope = json.loads(raw_body.decode("utf-8") or "{}")
    except json.JSONDecodeError as exc:
        logger.warning("Telnyx inbound body not JSON-decodable: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    data = envelope.get("data") or {}
    event_type = data.get("event_type", "")
    payload = data.get("payload") or {}
    from_number = (payload.get("from") or {}).get("phone_number", "")
    body = payload.get("text", "") or ""
    msg_id = payload.get("id")

    log_webhook_event(
        source="telnyx_inbound",
        event_type=event_type or "sms_received",
        source_event_id=msg_id,
        status="received",
        payload=envelope,
        payload_kind="telnyx",
    )

    # Only act on inbound message events. Delivery-status callbacks (e.g.
    # "message.sent", "message.finalized") share the same webhook URL but
    # don't need handler routing — we just audit-log them above.
    if event_type != "message.received":
        return Response(content="", media_type="application/json")

    if not from_number:
        logger.warning("Telnyx inbound with no from-number: %s", payload)
        return Response(content="", media_type="application/json")

    # 3. STOP / HELP compliance handling — unchanged from the Twilio path.
    twiml_reply = handle_inbound(from_number, body, db)
    if twiml_reply:
        return Response(content=twiml_reply, media_type="application/xml")

    # 3b. Opt-in confirmation (V5 sentinel-gated). Returns TwiML only when
    # send_opt_in_prompt previously set the Redis key for this number.
    # If the key is absent the call returns None and falls through to product commands.
    opt_in_reply = handle_opt_in_reply(from_number, body, db)
    if opt_in_reply:
        return Response(content=opt_in_reply, media_type="application/xml")

    # 4. Product command routing — unchanged from the Twilio path.
    command = sms_commands.parse(body)
    if command:
        reply = sms_commands.dispatch(from_number, command, db)
        if reply:
            send_sms(from_number, reply, db, message_type="transactional")
    else:
        from src.services.cora_suppression import record_generic_sms_reply
        record_generic_sms_reply(
            db,
            phone=from_number,
            source_id=msg_id,
        )

    return Response(content="", media_type="application/json")


# ── Phase 2B: Deal-size capture ────────────────────────────────────────────────

class DealCaptureRequest(BaseModel):
    feed_uuid: str
    property_id: int
    deal_size_bucket: str  # 5_10k | 10_25k | 25k_plus | skip
    deal_amount: Optional[float] = None
    days_to_close: Optional[int] = None


@app.post("/api/deal-capture", status_code=201)
def deal_capture(payload: DealCaptureRequest, db: Session = Depends(get_db)):
    """Record a deal outcome reported by a subscriber.

    Stage 5: also generates the deal-win graphic and (for $10K+ deals)
    fires the annual-at-deal-win push.
    """
    from src.core.models import DealOutcome

    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == payload.feed_uuid)
    ).scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")

    valid_buckets = {"5_10k", "10_25k", "25k_plus", "skip"}
    if payload.deal_size_bucket not in valid_buckets:
        raise HTTPException(status_code=422, detail=f"deal_size_bucket must be one of {valid_buckets}")

    outcome = DealOutcome(
        subscriber_id=sub.id,
        property_id=payload.property_id,
        deal_size_bucket=payload.deal_size_bucket,
        deal_amount=payload.deal_amount,
        deal_date=date.today(),
        days_to_close=payload.days_to_close,
        pipeline_stage="closed_lost" if payload.deal_size_bucket == "skip" else "closed_won",
        county_id=sub.county_id,
        trade_vertical=sub.vertical,
    )
    db.add(outcome)
    db.flush()

    try:
        from src.services.cora_suppression import create_suppression
        create_suppression(
            db,
            subscriber_id=sub.id,
            reason="deal_lost" if payload.deal_size_bucket == "skip" else "deal_won",
            source="deal_capture",
            source_id=outcome.id,
            notes="Auto-pause triggered by deal outcome",
            cancel_reason="deal_outcome_auto_pause",
        )
    except Exception as exc:
        logger.warning("[DealCapture] cora suppression failed: %s", exc)

    graphic_url: Optional[str] = None
    annual_offered = False

    # Stage 5: generate graphic (idempotent, fails-soft)
    if payload.deal_size_bucket != "skip":
        try:
            from src.services.win_graphic import generate as gen_graphic
            path = gen_graphic(outcome.id, db)
            if path:
                graphic_url = f"/api/win-graphic/{outcome.id}"
        except Exception as exc:
            logger.warning("[DealCapture] win graphic gen failed: %s", exc)

        try:
            from src.services.win_autopsy import record_win_autopsy
            record_win_autopsy(outcome.id, db)
        except Exception as exc:
            logger.warning("[DealCapture] win autopsy failed: %s", exc)

    # Stage 5: annual-at-deal-win trigger for $10K+ deals
    is_big = (payload.deal_amount and payload.deal_amount >= 10000) \
        or payload.deal_size_bucket in ("10_25k", "25k_plus")
    if is_big:
        try:
            from src.tasks.annual_push import _push_annual_offer  # noqa: F401
            # We use the existing push helper which sends the email annual offer.
            # SMS-side push will land once Subscriber.phone column is added.
            from src.tasks.annual_push import _push_annual_offer
            if _push_annual_offer(sub, "deal_win_10k", db):
                annual_offered = True
        except Exception as exc:
            logger.warning("[DealCapture] annual push failed: %s", exc)

    try:
        from src.services.attribution_service import record_conversion_attribution
        record_conversion_attribution(
            conversion_type="deal_win_reported",
            source_table="deal_outcomes",
            source_event_id=str(outcome.id),
            subscriber_id=sub.id,
            occurred_at=datetime.now(timezone.utc),
            property_id=payload.property_id,
            deal_size_bucket=payload.deal_size_bucket,
            db=db,
        )
    except Exception:
        logger.warning("[DealCapture] Attribution recording failed sub=%s", sub.id, exc_info=True)

    return {
        "ok": True,
        "deal_id": outcome.id,
        "graphic_url": graphic_url,
        "annual_offered": annual_offered,
    }


# Stage 5: serve the generated win-graphic PNG by deal id
@app.get("/api/win-graphic/{deal_outcome_id}")
def win_graphic_endpoint(deal_outcome_id: int, db: Session = Depends(get_db)):
    """Stream the generated win graphic PNG. Generates on demand if missing."""
    from src.services.win_graphic import generate as gen_graphic, output_path
    path = output_path(deal_outcome_id)
    if not path.exists():
        path = gen_graphic(deal_outcome_id, db)
    if not path or not Path(path).exists():
        raise HTTPException(status_code=404, detail="Win graphic not available")
    return FileResponse(str(path), media_type="image/png")


# Stage 5: anonymized social proof wall — recent wins powering the landing page
@app.get("/api/proof-wall")
def proof_wall(
    limit: int = Query(50, ge=1, le=200),
    county_id: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    from src.services.win_graphic import proof_wall_payload
    items = proof_wall_payload(db, limit=limit, county_id=county_id)
    return {"items": items}


# S5: Win-Story Auto-Publisher feed — sanitised proof statements from lead-pack deliveries
@app.get("/api/proof/win-stories")
def get_win_stories(
    limit: int = Query(20, ge=1, le=50),
    county_id: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    """
    Public endpoint — returns recent is_public win_story_assets rows.
    No PII: county + deal type only. Used by the /wins proof wall page.
    Pass county_id to filter to a specific county (e.g. hillsborough).
    """
    try:
        rows = db.execute(
            text("""
                SELECT id, event_type, county_id, proof_text, created_at
                FROM win_story_assets
                WHERE is_public = true
                  AND (:county IS NULL OR county_id = :county)
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"limit": limit, "county": county_id},
        ).fetchall()
    except Exception as exc:
        logger.error("[win-stories] query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database error")

    return [
        {
            "id": r.id,
            "event_type": r.event_type,
            "county_id": r.county_id,
            "proof_text": r.proof_text,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]


# ── Stage 5: Annual lock acceptance (deal-win + Day-60 path) ─────────────────

class AnnualAcceptRequest(BaseModel):
    feed_uuid: str


@app.post("/api/annual/accept")
def annual_accept(payload: AnnualAcceptRequest, db: Session = Depends(get_db)):
    """One-tap acceptance of the annual lock offer. Switches the subscriber's
    Stripe subscription to the annual price with proration."""
    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == payload.feed_uuid)
    ).scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")
    if not sub.stripe_subscription_id:
        raise HTTPException(status_code=400, detail="Subscriber has no active subscription")

    # Pre-flight: refuse to call Stripe when the account is in a billing-broken
    # state. The user must clear it (update card / end pause) before retry.
    from src.services.stripe_service import can_switch_subscription
    ok_status, reason = can_switch_subscription(sub)
    if not ok_status:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "billing_status_blocked",
                "current_status": reason,
                "message": "Update your payment method first, then retry.",
                "billing_portal_url": "/api/portal-session",
            },
        )

    from src.tasks.annual_push import switch_to_annual
    ok = switch_to_annual(sub.id, db)
    if not ok:
        raise HTTPException(status_code=502, detail="Annual switch failed - try again or contact support")
    return {"ok": True, "subscriber_id": sub.id, "tier": "annual_lock"}


@app.get("/api/annual/accept", include_in_schema=False)
def annual_accept_get(feed_uuid: str, db: Session = Depends(get_db)):
    """GET-friendly variant so the link in the offer email can be tapped directly."""
    return annual_accept(AnnualAcceptRequest(feed_uuid=feed_uuid), db)


# ── Stage 5: Tier upgrade (AutoPilot Pro path) ───────────────────────────────

class UpgradeRequest(BaseModel):
    feed_uuid: str
    tier: str

    @field_validator("tier")
    @classmethod
    def _valid(cls, v: str) -> str:
        if v not in {"autopilot_lite", "autopilot_pro", "data_only", "partner"}:
            raise ValueError("Unsupported tier for /api/upgrade")
        return v


_UPGRADE_PRICE_NAME = {
    "autopilot_lite": "autopilot_lite",
    "autopilot_pro":  "autopilot_pro",
    "data_only":      "data_only",
    "partner":        "partner",
}


@app.post("/api/upgrade")
def upgrade(req: UpgradeRequest, db: Session = Depends(get_db)):
    """Switch a subscriber's Stripe subscription to a different tier."""
    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)
    ).scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")
    if not sub.stripe_subscription_id:
        raise HTTPException(status_code=400, detail="Subscriber has no active subscription")

    # Pre-flight: same status guard as /api/annual/accept.
    from src.services.stripe_service import can_switch_subscription, switch_subscription_plan
    ok_status, reason = can_switch_subscription(sub)
    if not ok_status:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "billing_status_blocked",
                "current_status": reason,
                "message": "Update your payment method first, then retry.",
                "billing_portal_url": "/api/portal-session",
            },
        )

    settings = get_settings()
    price_name = _UPGRADE_PRICE_NAME[req.tier]
    new_price_id = settings.active_stripe_price(price_name)
    if not new_price_id:
        raise HTTPException(status_code=503, detail=f"Stripe price not configured for {req.tier}")

    try:
        switch_subscription_plan(sub.stripe_subscription_id, new_price_id, prorate=True)
    except Exception as exc:
        logger.error("[Upgrade] switch failed sub=%d tier=%s: %s", sub.id, req.tier, exc)
        raise HTTPException(status_code=502, detail="Plan switch failed")

    sub.tier = req.tier
    db.flush()

    # Tag the GHL contact so workflows pick up the new tier (e.g. AP Pro 5-touch)
    try:
        if sub.ghl_contact_id:
            from src.services.synthflow_service import _apply_tags_to_contact
            _apply_tags_to_contact(sub.ghl_contact_id, [req.tier])
    except Exception as exc:
        logger.warning("[Upgrade] GHL tag failed for sub=%d: %s", sub.id, exc)

    return {"ok": True, "subscriber_id": sub.id, "tier": req.tier}


@app.get("/api/upgrade", include_in_schema=False)
def upgrade_get(feed_uuid: str, tier: str, db: Session = Depends(get_db)):
    """GET-friendly upgrade so the link in the upsell email can be tapped directly."""
    return upgrade(UpgradeRequest(feed_uuid=feed_uuid, tier=tier), db)


@app.get("/api/save-offer/accept", include_in_schema=False)
def save_offer_accept_get(feed_uuid: str):
    """Redirect email link to a confirmation page — prevents link prefetchers from triggering the downgrade."""
    from fastapi.responses import RedirectResponse
    from config.settings import get_settings as _gs
    base = _gs().app_base_url.rstrip("/")
    return RedirectResponse(url=f"{base}/save-offer/confirm?uuid={feed_uuid}", status_code=302)


@app.post("/api/save-offer/accept", include_in_schema=False)
def save_offer_accept_post(feed_uuid: str, db: Session = Depends(get_db)):
    """Execute the tier downgrade after the user confirms on the confirmation page."""
    return upgrade(UpgradeRequest(feed_uuid=feed_uuid, tier="data_only"), db)


# ── Phase B: ZIP Territory Map ───────────────────────────────────────────────

@app.get("/api/territory-map")
def territory_map(
    county_id: str,
    vertical: str,
    db: Session = Depends(get_db),
):
    """
    Returns all ZIP territories for a county/vertical combination with live status.
    Used by the ZIP Territory Map UI component.
    Response cached 60s in Redis.
    """
    import json
    from src.core.models import ZipTerritory, Property as _Prop, DistressScore as _DS
    from src.core.redis_client import redis_available, rget, rset
    from src.services.urgency_engine import get_active_count
    from src.utils.zip_centroids import get_county_zip_centroids, get_zip_centroid, get_county_map_config

    cache_key = f"territory_map:{county_id}:{vertical}"
    if redis_available():
        cached = rget(cache_key)
        if cached:
            return json.loads(cached)

    zip_rows = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.county_id == county_id,
            ZipTerritory.vertical == vertical,
        )
    ).scalars().all()
    territory_db = {zt.zip_code: zt for zt in zip_rows}

    # Always show all known ZIPs for this county; default to 'available' if not yet locked.
    # For non-hillsborough counties fall back to only the rows that exist in zip_territories.
    county_centroids = get_county_zip_centroids(county_id)
    known_zips = sorted(county_centroids.keys()) if county_centroids else sorted(territory_db.keys())

    # Single GROUP BY query for lead counts across all known ZIPs
    lead_counts: dict = {}
    if known_zips:
        lead_counts = dict(db.execute(
            select(_Prop.zip, func.count().label("cnt"))
            .where(_Prop.zip.in_(known_zips), _Prop.county_id == county_id)
            .group_by(_Prop.zip)
        ).all())

    now = datetime.now(timezone.utc)
    results = []
    for zip_code in known_zips:
        zt = territory_db.get(zip_code)
        status = zt.status if zt else "available"
        lead_count = lead_counts.get(zip_code, 0)

        active_viewers = 0
        try:
            active_viewers = get_active_count(zip_code)
        except Exception:
            pass

        centroid = get_zip_centroid(zip_code, county_id)
        entry: dict = {
            "zip": zip_code,
            "status": status,
            "active_viewers": active_viewers,
            "lead_count": lead_count,
            "lat": centroid[0] if centroid else None,
            "lon": centroid[1] if centroid else None,
        }
        if zt and zt.status == "grace" and zt.grace_expires_at:
            entry["grace_expires_at"] = zt.grace_expires_at.isoformat()
        entry["waitlist_count"] = len(zt.waitlist_emails) if zt and zt.waitlist_emails else 0
        results.append(entry)

    payload = {
        "county_id": county_id,
        "vertical": vertical,
        "zips": results,
        "map_config": get_county_map_config(county_id),
        "generated_at": now.isoformat(),
    }

    if redis_available():
        rset(cache_key, json.dumps(payload), ttl_seconds=60)

    return payload


class CreateAffiliateRequest(BaseModel):
    name: str
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    commission_rate: Optional[float] = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        if len(v) > 200:
            raise ValueError("name must be 200 characters or fewer")
        return v

    @field_validator("contact_email")
    @classmethod
    def _validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip().lower()
        if not v:
            return None
        if "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError("contact_email is not a valid email address")
        return v

    @field_validator("commission_rate")
    @classmethod
    def _validate_rate(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return None
        if not (0 < v <= 1):
            raise ValueError("commission_rate must be a fraction between 0 and 1 (e.g. 0.20)")
        return v


@app.post("/api/admin/affiliates", status_code=201)
def create_affiliate(
    req: CreateAffiliateRequest,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Mint an Affiliate with an opaque ref_code (the ?aff= token). Admin only.

    400/422 invalid body, 401/403 auth (get_current_admin), 409 ref_code
    collision, 500 unexpected.
    """
    from src.services.affiliate_engine import mint_affiliate
    try:
        aff = mint_affiliate(
            db,
            name=req.name,
            contact_email=req.contact_email,
            contact_phone=req.contact_phone,
            commission_rate=req.commission_rate,
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Could not allocate a unique referral code; please retry")
    except RuntimeError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Could not allocate a unique referral code; please retry")
    except SQLAlchemyError:
        db.rollback()
        logger.exception("create_affiliate: database error")
        raise HTTPException(status_code=500, detail="Failed to create affiliate")
    except Exception:
        db.rollback()
        logger.exception("create_affiliate: unexpected error")
        raise HTTPException(status_code=500, detail="Failed to create affiliate")
    return {
        "id": aff.id,
        "ref_code": aff.ref_code,
        "commission_rate": float(aff.commission_rate),
        "status": aff.status,
    }


@app.get("/api/admin/affiliates/{affiliate_id}/ledger")
def affiliate_ledger(
    affiliate_id: int,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Affiliate payout ledger: running balance + accrual/clawback lines. Admin only.

    401/403 auth, 404 unknown affiliate, 422 bad id, 500 unexpected.
    """
    if affiliate_id <= 0:
        raise HTTPException(status_code=422, detail="affiliate_id must be a positive integer")
    from src.services.affiliate_engine import get_affiliate_ledger
    try:
        exists = db.execute(
            text("SELECT 1 FROM affiliates WHERE id = :a"), {"a": affiliate_id}
        ).first()
        if exists is None:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        return get_affiliate_ledger(db, affiliate_id)
    except HTTPException:
        raise
    except SQLAlchemyError:
        logger.exception("affiliate_ledger: database error affiliate_id=%s", affiliate_id)
        raise HTTPException(status_code=500, detail="Failed to load affiliate ledger")


@app.get("/api/admin/human-close")
def list_human_close(
    status: str = "open",
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """List human close escalations. status=open|closed|all. Requires admin JWT."""
    from src.core.models import HumanCloseEscalation
    q = select(HumanCloseEscalation)
    if status == "open":
        q = q.where(HumanCloseEscalation.outcome.is_(None))
    elif status == "closed":
        q = q.where(HumanCloseEscalation.outcome.is_not(None))
    elif status != "all":
        raise HTTPException(status_code=422, detail="status must be open|closed|all")
    q = q.order_by(HumanCloseEscalation.routed_at.desc()).limit(min(limit, 200)).offset(offset)
    rows = db.execute(q).scalars().all()

    # Batch-load subscriber name/phone/email for the queue rows (single query, no N+1).
    from src.core.models import Subscriber
    from src.services.phone_utils import normalize_closer as normalize_phone
    sub_ids = {r.subscriber_id for r in rows}
    sub_lookup: dict[int, tuple] = {}
    if sub_ids:
        for sid, sname, sphone, semail in db.execute(
            select(Subscriber.id, Subscriber.name, Subscriber.phone, Subscriber.email).where(
                Subscriber.id.in_(sub_ids)
            )
        ).all():
            sub_lookup[sid] = (sname, normalize_phone(sphone), semail)

    return {
        "count": len(rows),
        "items": [
            {
                "id": r.id,
                "subscriber_id": r.subscriber_id,
                "subscriber_name": sub_lookup.get(r.subscriber_id, (None, None, None))[0],
                "subscriber_phone": sub_lookup.get(r.subscriber_id, (None, None, None))[1],
                "subscriber_email": sub_lookup.get(r.subscriber_id, (None, None, None))[2],
                "revenue_signal_score": r.revenue_signal_score,
                "interactions_count": r.interactions_count,
                "target_tier": r.target_tier,
                "target_tier_price_cents": r.target_tier_price_cents,
                "vertical": r.vertical,
                "channel": r.channel,
                "routed_at": r.routed_at.isoformat() if r.routed_at else None,
                "outcome": r.outcome,
                "closer_assigned": r.closer_assigned,
                "posted_at": r.posted_at.isoformat() if r.posted_at else None,
                "post_attempts": r.post_attempts,
            }
            for r in rows
        ],
    }


@app.post("/api/admin/human-close/{escalation_id}/outcome")
def human_close_outcome(
    escalation_id: int,
    outcome: str,
    closer_assigned: Optional[str] = None,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Record outcome for a human close escalation. Requires admin JWT."""
    from src.core.models import HumanCloseEscalation
    esc = db.get(HumanCloseEscalation, escalation_id)
    if not esc:
        raise HTTPException(status_code=404, detail="Escalation not found")
    valid = {"won", "lost", "no_response", "rescheduled"}
    if outcome not in valid:
        raise HTTPException(status_code=422, detail=f"outcome must be one of {valid}")
    esc.outcome = outcome
    esc.outcome_at = datetime.now(timezone.utc)
    if closer_assigned:
        esc.closer_assigned = closer_assigned
    if outcome in {"won", "lost"}:
        try:
            from src.services.cora_suppression import create_suppression
            create_suppression(
                db,
                subscriber_id=esc.subscriber_id,
                reason=f"deal_{outcome}",
                source="human_close",
                source_id=esc.id,
                notes="Auto-pause triggered by deal outcome",
                cancel_reason="deal_outcome_auto_pause",
                created_by=closer_assigned,
            )
        except Exception as exc:
            logger.warning("[HumanClose] cora suppression failed escalation=%s: %s", escalation_id, exc)
    db.flush()
    return {"ok": True, "escalation_id": escalation_id, "outcome": outcome}


# ── Stage 5: Referral team view + weekly leaderboard ─────────────────────────

@app.get("/api/feed/{feed_uuid}/team-view")
def team_view(feed_uuid: str, db: Session = Depends(get_db)):
    """
    Shared ZIP density view for a referral team. Returns lead density only —
    no PII or lead detail crosses team-member boundaries.
    """
    from src.core.models import ReferralTeam, DistressScore as _DS, Property as _P

    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")

    # Find an active team this subscriber is in
    team = db.execute(
        select(ReferralTeam).where(
            ReferralTeam.member_subscriber_ids.any(sub.id),
            ReferralTeam.status == "active",
        ).limit(1)
    ).scalar_one_or_none()
    if not team:
        # Check whether the subscriber had a team that was subsequently broken
        broken_team = db.execute(
            select(ReferralTeam).where(
                ReferralTeam.member_subscriber_ids.any(sub.id),
                ReferralTeam.status == "broken",
            ).order_by(ReferralTeam.broken_at.desc()).limit(1)
        ).scalar_one_or_none()
        if broken_team:
            return {
                "unlocked": False,
                "status": "broken",
                "broken_at": broken_team.broken_at.isoformat() if broken_team.broken_at else None,
                "broken_reason": broken_team.broken_reason,
                "shared_zips": [],
                "density": [],
            }
        return {"unlocked": False, "shared_zips": [], "density": []}

    zips = team.shared_zips or []
    if not zips:
        return {
            "unlocked": True,
            "team_id": team.id,
            "shared_zips": [],
            "density": [],
        }

    # Per-ZIP qualified-lead count for this team's vertical
    score_col = _DS.vertical_scores[team.vertical].as_float()
    rows = db.execute(
        select(_P.zip, func.count())
        .join(_DS, _DS.property_id == _P.id)
        .where(
            _P.zip.in_(zips),
            _P.county_id == team.county_id,
            _DS.qualified == True,   # noqa: E712
            score_col >= 40,
        )
        .group_by(_P.zip)
    ).all()
    density = [{"zip": z, "leads": int(c)} for z, c in rows]
    density.sort(key=lambda r: r["leads"], reverse=True)

    return {
        "unlocked": True,
        "team_id": team.id,
        "county_id": team.county_id,
        "vertical": team.vertical,
        "shared_zips": zips,
        "density": density,
    }


@app.get("/api/leaderboard")
def leaderboard_endpoint(
    request: Request,
    response: Response,
    county_id: Optional[str] = None,
    vertical: Optional[str] = None,
):
    """Public weekly leaderboard. Filter by county_id and/or vertical.
    Reads the latest snapshot written by `src.tasks.leaderboard`.

    Phase A.2 (2026-05-04) hardening:
      - Per-IP rate limit: 60 req/min/IP (compresses casual scraping while
        still allowing dashboards to poll every 30s without tripping).
      - subscriber_id stripped from the public response — kept only on the
        on-disk snapshot for ops. Public consumers see handle + rank + counts
        + badge.
      - Cache-Control: public, max-age=3600. Snapshot only refreshes Monday,
        so a 1-hour CDN / browser cache is safe and absorbs scraper traffic.
    """
    enforce_or_429(request, scope="leaderboard", limit=60, window_seconds=60)

    from src.tasks.leaderboard import latest_snapshot
    snap = latest_snapshot()
    response.headers["Cache-Control"] = "public, max-age=3600"

    if not snap:
        return {"as_of": None, "leaderboards": []}
    boards = snap.get("leaderboards", [])
    if county_id:
        boards = [b for b in boards if b["county_id"] == county_id]
    if vertical:
        boards = [b for b in boards if b["vertical"] == vertical]

    # Strip subscriber_id from every leaderboard row before sending — the
    # snapshot file keeps it for ops, but the public API must not.
    public_boards = []
    for b in boards:
        rows = [{k: v for k, v in row.items() if k != "subscriber_id"}
                for row in b.get("leaderboard", [])]
        public_boards.append({**b, "leaderboard": rows})

    return {"as_of": snap.get("as_of"), "leaderboards": public_boards}


# ── Phase 2B: NWS weather alert webhook ───────────────────────────────────────

@app.post("/webhooks/nws/alert")
async def nws_alert(request: Request, db: Session = Depends(get_db)):
    """Receive NWS CAP alert and activate storm packs in affected ZIPs."""
    from src.services import nws_webhook
    from src.services.webhook_log import log_webhook_event
    payload = await request.json()
    log_webhook_event(
        source="nws",
        event_type=((payload.get("properties") or payload).get("event") or "alert"),
        source_event_id=((payload.get("properties") or payload).get("id") or payload.get("id")),
        status="received",
        payload=payload,
        payload_kind="nws",
        db=db,
    )
    result = nws_webhook.process_alert(payload, db)
    return result


# ── Phase 2B: Admin DLQ review ────────────────────────────────────────────────

@app.get("/api/admin/dlq")
def admin_dlq(limit: int = 50, db: Session = Depends(get_db)):
    """Return unreviewed SMS dead-letter queue items for admin review."""
    from src.core.models import SmsDeadLetter
    rows = db.execute(
        select(SmsDeadLetter)
        .where(SmsDeadLetter.reviewed_at.is_(None))
        .order_by(SmsDeadLetter.created_at.desc())
        .limit(limit)
    ).scalars().all()
    return {
        "count": len(rows),
        "items": [
            {
                "id": r.id,
                "phone": r.phone,
                "reason": r.reason,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
    }


# ── Phase 2B: Live ZIP activity — GET /api/zip-activity ──────────────────────

@app.get("/api/zip-activity")
def zip_activity(
    response: Response,
    zip_code: str,
    vertical: Optional[str] = None,
    county_id: Optional[str] = Query(default=None),
):
    """
    Return live urgency signal for a ZIP — viewer count + recent message
    volume. Powers the FOMO indicator on SampleLeads / dashboard feed.

    When county_id is provided, uses a county-scoped Redis key so activity
    from different counties is tracked separately.
    Read-only, no auth required. Degrades cleanly when Redis is down.
    """
    if not _ZIP_RE.match(zip_code):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_zip", "message": "ZIP code must be exactly 5 digits"},
        )
    if vertical is not None and vertical not in VALID_VERTICALS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"vertical must be one of: {sorted(VALID_VERTICALS)}"},
        )
    from src.services.urgency_engine import get_active_count
    active_viewers = get_active_count(zip_code, county_id=county_id)
    response.headers["Cache-Control"] = "public, max-age=10"
    return {
        "zip_code": zip_code,
        "vertical": vertical,
        "county_id": county_id,
        "active_viewers": active_viewers,
    }


# ── Phase 2B: Lead hold status — GET /api/leads/{property_id}/hold ────────────

@app.get("/api/leads/{property_id}/hold")
def lead_hold_status(
    property_id: int,
    feed_uuid: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    """
    Return current 20-min hold reservation state for a lead.

    Public-readable (no auth) so the LeadCard FOMO banner can poll without
    leaking subscriber context to other viewers. When `feed_uuid` is supplied
    the response also flags whether the hold belongs to the requesting
    subscriber so the UI can show "currently held FOR YOU" vs
    "currently being worked".

    Degrades cleanly when Redis is unavailable: returns held=false.
    """
    from src.services.lead_hold import get_holder

    holder_id = get_holder(property_id)
    if holder_id is None:
        return {
            "property_id": property_id,
            "held": False,
            "held_by_self": False,
            "expires_at": None,
            "hold_minutes": 20,
        }

    # Compute remaining TTL from Redis if available so the UI can render a countdown.
    expires_at = None
    try:
        from src.core.redis_client import get_redis, redis_available
        if redis_available():
            ttl = get_redis().ttl(f"lead_hold:{property_id}")
            if ttl and ttl > 0:
                from datetime import datetime, timedelta, timezone
                expires_at = (datetime.now(timezone.utc) + timedelta(seconds=int(ttl))).isoformat()
    except Exception:
        expires_at = None

    held_by_self = False
    if feed_uuid:
        sub_id = None
        try:
            from src.core.redis_client import get_redis, redis_available
            if redis_available():
                cached = get_redis().get(f"fa:sub_uuid:{feed_uuid}")
                if cached:
                    sub_id = int(cached)
        except Exception:
            pass

        if sub_id is None:
            row = db.execute(
                select(Subscriber.id).where(Subscriber.event_feed_uuid == feed_uuid)
            ).scalar_one_or_none()
            if row is not None:
                sub_id = int(row)
                try:
                    from src.core.redis_client import get_redis, redis_available
                    if redis_available():
                        get_redis().setex(f"fa:sub_uuid:{feed_uuid}", 3600, str(sub_id))
                except Exception:
                    pass

        held_by_self = sub_id is not None and sub_id == int(holder_id)

    return {
        "property_id": property_id,
        "held": True,
        "held_by_self": held_by_self,
        "expires_at": expires_at,
        "hold_minutes": 20,
    }


# ── Phase 2B: Proof Moment — GET /api/proof-leads ────────────────────────────

@app.get("/api/proof-leads")
def proof_leads(
    request: Request,
    vertical: str = "roofing",
    county_id: str = "hillsborough",
    feed_uuid: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    """
    Return 1 fully revealed + 2 blurred leads for the signup proof moment.
    Requires no auth — used immediately after free account creation.
    """
    from src.services.proof_moment import get_proof_leads, ip_to_slot
    from src.services.business_events import log_business_event
    if vertical not in VALID_VERTICALS:
        raise HTTPException(status_code=400, detail=f"Invalid vertical. Must be one of: {sorted(VALID_VERTICALS)}")
    xff = request.headers.get("X-Forwarded-For")
    client_ip = xff.split(",")[0].strip() if xff else (request.client.host if request.client else "0.0.0.0")
    slot = ip_to_slot(client_ip)
    result = get_proof_leads(vertical=vertical, county_id=county_id, db=db, feed_uuid=feed_uuid, ip_slot=slot)
    if feed_uuid:
        sub = db.query(Subscriber).filter_by(event_feed_uuid=feed_uuid).first()
        log_business_event(
            "PROOF_MOMENT_VIEWED",
            subscriber_id=sub.id if sub else None,
            payload={"vertical": vertical, "county_id": county_id},
            db=db,
        )
    return result


# ── Phase 2B: Free signup — POST /api/free-signup ────────────────────────────

class FreeSignupRequest(BaseModel):
    email: str
    vertical: str = "roofing"
    county_id: str = "hillsborough"
    name: Optional[str] = None
    referral_code: Optional[str] = None
    # Optional phone + TCPA consent for SMS features. When provided AND
    # sms_consent=True, signup_engine inserts an SmsOptIn row so Cora can
    # send marketing SMS (lead alerts, accelerated wallet push, FOMO).
    phone: Optional[str] = None
    sms_consent: bool = False
    # fa017 signup-source attribution. signup_source is validated against
    # the allow-list in signup_engine.ALLOWED_SIGNUP_SOURCES; anything else
    # falls back to 'unknown'. utm_*/campaign_id/attribution_token are free
    # text and stored as-is (truncated to schema lengths).
    signup_source: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    campaign_id: Optional[str] = None
    attribution_token: Optional[str] = None
    # fa081: affiliate ?aff= token, captured client-side and forwarded here.
    # Distinct from referral_code (the peer credit loop).
    affiliate_ref: Optional[str] = None
    # Phase 2B: caller hint about what the user is about to do. Suppresses the
    # welcome email when the user is mid-purchase ('upgrade' = paid checkout,
    # 'unlock' = $4 lead unlock). Welcome fires from the relevant payment
    # webhook instead, so abandoned-cart users never get a misleading email.
    intent: Optional[str] = None
    consent_acceptance: Optional[ConsentAcceptanceRequest] = None

    @field_validator("email")
    @classmethod
    def _validate_email(cls, v: str) -> str:
        v = (v or "").strip().lower()
        if not v or "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError("A valid email is required")
        return v


@app.post("/api/free-signup", status_code=201)
def free_signup(req: FreeSignupRequest, request: Request, db: Session = Depends(get_db)):
    """
    Create (or re-use) a free-tier Subscriber keyed by email.

    Powers the landing-page unlock flow: landing visitor enters email →
    this endpoint creates the free Subscriber + real Stripe customer →
    returns the feed_uuid the frontend needs for /api/payment-intent.

    Idempotent on email — re-visiting with the same email returns the
    existing subscriber's feed_uuid without creating duplicates.
    """
    if req.vertical not in VALID_VERTICALS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"Must be one of: {sorted(VALID_VERTICALS)}"},
        )

    from src.services.signup_engine import create_free_account_by_email
    from src.services.affiliate_engine import AFFILIATE_COOKIE_NAME

    # Defer the welcome email when the user is mid-purchase. The corresponding
    # payment webhook handler is responsible for sending the welcome on success.
    defer_welcome = req.intent in ("upgrade", "unlock")

    # Affiliate attribution: the ?aff= token is captured client-side and sent in
    # the request body (primary). The cookie set by the tracking middleware is a
    # fallback for any flow that reaches FastAPI directly.
    affiliate_ref = req.affiliate_ref or request.cookies.get(AFFILIATE_COOKIE_NAME)

    # Derive SMS consent from structured consent_acceptance when present.
    tcpa_accepted = bool(req.consent_acceptance and req.consent_acceptance.tcpa_accepted)
    sub = create_free_account_by_email(
        email=req.email,
        db=db,
        vertical=req.vertical,
        county_id=req.county_id,
        name=req.name,
        referral_code=req.referral_code,
        phone=req.phone,
        sms_consent=req.sms_consent or tcpa_accepted,
        signup_source=req.signup_source,
        utm_source=req.utm_source,
        utm_medium=req.utm_medium,
        utm_campaign=req.utm_campaign,
        campaign_id=req.campaign_id,
        attribution_token=req.attribution_token,
        affiliate_ref=affiliate_ref,
        send_welcome=not defer_welcome,
    )

    if req.consent_acceptance and req.consent_acceptance.terms_accepted:
        try:
            from datetime import datetime

            def _parse_iso_fs(s):
                if not s:
                    return None
                try:
                    return datetime.fromisoformat(s.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    return None

            ca = ConsentAcceptance(
                email=req.email,
                phone=sub.phone,
                subscriber_id=sub.id,
                terms_version=req.consent_acceptance.terms_version or "2026.06",
                privacy_version=req.consent_acceptance.privacy_version or "2026.06",
                accepted_at=datetime.now(timezone.utc),
                source_flow="free_signup",
                user_agent=req.consent_acceptance.user_agent,
                modal_opened_at=_parse_iso_fs(req.consent_acceptance.modal_opened_at),
                modal_scrolled_to_end_at=_parse_iso_fs(req.consent_acceptance.modal_scrolled_to_end_at),
                accepted_text_hash=req.consent_acceptance.accepted_text_hash or "",
                tcpa_consent_text=req.consent_acceptance.tcpa_consent_text if tcpa_accepted else None,
                tcpa_consent_version=req.consent_acceptance.tcpa_consent_version if tcpa_accepted else None,
                tcpa_checked_at=datetime.now(timezone.utc) if tcpa_accepted else None,
                consent_scope="marketing" if tcpa_accepted else None,
                not_condition_of_purchase_ack=tcpa_accepted or None,
            )
            db.add(ca)
            db.commit()
        except Exception:
            logger.warning("ConsentAcceptance write failed in free_signup (non-fatal):", exc_info=True)

    return {
        "subscriber_id": sub.id,
        "feed_uuid": sub.event_feed_uuid,
        "tier": sub.tier,
        "status": sub.status,
        "email": sub.email,
        "phone": sub.phone,
        "vertical": sub.vertical,
        "county_id": sub.county_id,
        "signup_source": sub.signup_source,
    }


# ── fa017: Landing token resolver ───────────────────────────────────────────
# Maps a signed HMAC token from a missed-call / DBPR / Cora SMS landing link
# back to the Subscriber's feed_uuid so the frontend can navigate directly to
# /dashboard/<uuid> without forcing a re-signup. Best-effort: invalid /
# expired tokens return 410 Gone and the frontend silently falls back to the
# normal landing-page signup flow.

class ResolveTokenRequest(BaseModel):
    token: str = Field(..., min_length=10, max_length=2000)


@app.post("/api/landing/resolve-token", status_code=200)
def resolve_landing_token(req: ResolveTokenRequest, db: Session = Depends(get_db)):
    from src.services.signed_links import decode_landing_token

    payload = decode_landing_token(req.token)
    if not payload:
        raise HTTPException(
            status_code=410,
            detail={"error": "invalid_or_expired_token"},
        )

    sub_id = payload.get("sub_id")
    if not isinstance(sub_id, int):
        raise HTTPException(status_code=410, detail={"error": "malformed_token"})

    sub = db.get(Subscriber, sub_id)
    if sub is None:
        raise HTTPException(status_code=410, detail={"error": "subscriber_not_found"})

    from src.services.business_events import log_business_event
    log_business_event(
        "TOKEN_RESOLVED",
        subscriber_id=sub.id,
        payload={"signup_source": sub.signup_source, "source": payload.get("source")},
        db=db,
    )

    return {
        "feed_uuid": sub.event_feed_uuid,
        "subscriber_id": sub.id,
        "signup_source": sub.signup_source,
    }


# ── fa017: Business event log ───────────────────────────────────────────────
# Frontend-callable audit endpoint. Captures landing_page_viewed / signup_started /
# lead_unlock_clicked / payment_started events that only the FE can observe.
# Returns 204 always — failures swallowed by the business_events helper so this
# can never block the user's flow.

class BusinessEventRequest(BaseModel):
    event_type: str = Field(..., max_length=80)
    feed_uuid: Optional[str] = None
    payload: Optional[dict] = None


@app.post("/api/business-event", status_code=204)
def post_business_event(req: BusinessEventRequest, db: Session = Depends(get_db)):
    from src.services.business_events import log_business_event

    subscriber_id: Optional[int] = None
    if req.feed_uuid:
        sub = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)
        ).scalar_one_or_none()
        if sub is not None:
            subscriber_id = sub.id

    log_business_event(
        event_type=req.event_type.upper(),
        subscriber_id=subscriber_id,
        payload=req.payload or None,
        source="frontend",
        db=db,
    )
    return Response(status_code=204)


# ── Phase 2B: Monetization Wall ───────────────────────────────────────────────

class WallSessionRequest(BaseModel):
    subscriber_id: int
    session_id: str
    vertical: str = "roofing"
    county_id: str = "hillsborough"


@app.post("/api/wall/session", status_code=201)
def create_wall_session(req: WallSessionRequest, db: Session = Depends(get_db)):
    """Create a monetization wall session for a new subscriber."""
    from src.services.monetization_wall import create_session, get_roi_frame
    state = create_session(req.subscriber_id, req.session_id)
    roi = get_roi_frame(req.vertical, req.county_id, db)
    return {"session": state, "roi_frame": roi}


@app.get("/api/wall/{session_id}")
def get_wall_session(session_id: str):
    """Poll wall session state (converted flag + countdown expiry)."""
    from src.services.monetization_wall import get_session_state
    state = get_session_state(session_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    return state


# ── Bundle Checkout — POST /api/bundle/checkout ───────────────────────────────

class BundleCheckoutRequest(BaseModel):
    feed_uuid: str
    bundle_type: str
    zip_code: Optional[str] = None
    vertical: Optional[str] = None
    ab_variant: Optional[str] = None
    attribution: Optional[dict] = None  # Meta Ads attribution (utm_*, campaign_id, fbclid, ...)


@app.post("/api/bundle/checkout", status_code=201)
def bundle_checkout(req: BundleCheckoutRequest, request: Request, db: Session = Depends(get_db)):
    """Create a Stripe PaymentIntent for a bundle purchase.
    Returns { client_secret, publishable_key, amount, currency, bundle_type }.
    """
    from src.services.bundle_engine import create_payment_intent, is_available
    _s = get_settings()

    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)
    ).scalar_one_or_none()
    if not sub or sub.status not in ("active", "grace"):
        raise HTTPException(status_code=403, detail="Active subscription required")

    if not is_available(req.bundle_type, sub.id, db):
        raise HTTPException(
            status_code=422,
            detail={"error": "bundle_unavailable", "message": "This bundle is not available right now."},
        )

    try:
        result = create_payment_intent(
            bundle_type=req.bundle_type,
            subscriber_id=sub.id,
            zip_code=req.zip_code or "",
            vertical=req.vertical or "",
            db=db,
            extra_metadata=_attribution_stripe_metadata(request, req.attribution),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    return {
        "client_secret": result["client_secret"],
        "publishable_key": _s.active_stripe_publishable_key,
        "amount": result["amount"],
        "currency": "usd",
        "bundle_type": req.bundle_type,
    }


# ── Stage 5+: Wallet Topup — POST /api/wallet/topup ───────────────────────────

# Whitelisted topup amounts (cents) → credits granted on webhook fulfillment.
# Aligns with wallet tier pricing so a "20 credits for $25" topup matches the
# implicit per-credit price of starter wallet ($49/20cr ≈ $2.45/cr).
WALLET_TOPUP_PACKAGES = {
    2500:  10,    # $25  → 10 credits
    5000:  22,    # $50  → 22 credits (10% bonus)
    10000: 48,    # $100 → 48 credits (20% bonus)
}


class WalletTopupRequest(BaseModel):
    feed_uuid: str
    amount_cents: int = Field(..., description="Must match a key in WALLET_TOPUP_PACKAGES")


@app.post("/api/wallet/topup", status_code=201)
def wallet_topup_endpoint(
    req: WalletTopupRequest, db: Session = Depends(get_db)
):
    """One-tap wallet top-up. Returns Stripe PaymentIntent client_secret for
    the Payment Sheet. Webhook (_on_wallet_topup_payment) credits the wallet
    on payment_intent.succeeded.

    Amount must match a whitelisted package — prevents arbitrary-amount
    submissions from the client.
    """
    if req.amount_cents not in WALLET_TOPUP_PACKAGES:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_topup_amount",
                "allowed_amounts_cents": sorted(WALLET_TOPUP_PACKAGES.keys()),
            },
        )

    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)
    ).scalar_one_or_none()
    if sub is None:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")
    if sub.status == "disputed":
        raise HTTPException(
            status_code=403,
            detail="Account on hold for review. Email support@forcedaction.io.",
        )

    credits = WALLET_TOPUP_PACKAGES[req.amount_cents]
    from src.services.payment_sheet import create_payment_intent as _create_pi
    try:
        result = _create_pi(
            subscriber_id=sub.id,
            amount_cents=req.amount_cents,
            description=f"Wallet top-up — {credits} credits",
            save_card=True,
            db=db,
            metadata={
                "product": "wallet_topup",
                "amount_cents": str(req.amount_cents),
                "credits": str(credits),
            },
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {**result, "credits": credits}


# ── fa016: Accelerated Wallet Push — accept / decline ──────────────────────

class AcceleratedWalletOfferRequest(BaseModel):
    offer_id: int = Field(..., gt=0)


@app.post("/api/wallet/accept-accelerated-offer/{feed_uuid}", status_code=200)
def accept_accelerated_wallet_offer(
    feed_uuid: str,
    req: AcceleratedWalletOfferRequest,
    db: Session = Depends(get_db),
):
    """Accept an Accelerated Wallet Push offer (in-app modal or `?wallet_offer=accept`).
    Creates a wallet Subscription against the saved card off-session.
    Activation happens asynchronously via the invoice.payment_succeeded webhook.
    Returns 409 if the offer is not in 'offered' status (already accepted, declined, etc).
    """
    from config.settings import settings as _settings
    if not getattr(_settings, "accelerated_wallet_push_enabled", False):
        raise HTTPException(status_code=404, detail={"error": "feature_disabled"})

    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if sub is None:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")
    if not sub.has_saved_card or not sub.stripe_payment_method_id:
        raise HTTPException(status_code=409, detail={"error": "no_saved_card"})

    from src.core.models import WalletPushOffer
    offer = db.get(WalletPushOffer, req.offer_id)
    if offer is None or offer.subscriber_id != sub.id:
        raise HTTPException(status_code=404, detail={"error": "offer_not_found"})
    if offer.status != "offered":
        raise HTTPException(
            status_code=409,
            detail={"error": "offer_not_open", "status": offer.status},
        )

    from src.services import wallet_engine
    try:
        result = wallet_engine.activate_via_saved_card(
            subscriber_id=sub.id, tier=offer.tier, db=db, offer_id=offer.id
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail={"error": str(exc)})
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail={"error": str(exc)})
    except Exception as exc:
        logger.error("activate_via_saved_card failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error"})

    from datetime import datetime, timezone
    offer.accepted_at = datetime.now(timezone.utc)
    if result.get("subscription_id"):
        offer.stripe_subscription_id = result["subscription_id"]
    if result.get("status") == "failed":
        offer.status = "failed"
    else:
        offer.status = "accepted"
    db.flush()

    # Clear pending offer so subsequent SMS YES/WALLET aren't double-routed
    try:
        from src.core.redis_client import redis_available, rdelete
        if redis_available():
            rdelete(f"fa:pending_offer:{sub.id}")
    except Exception:
        pass

    return {
        "offer_id": offer.id,
        "status": offer.status,
        "subscription_id": result.get("subscription_id"),
        "stripe_status": result.get("status"),
        "requires_action": bool(result.get("requires_action")),
        "client_secret": result.get("client_secret"),
    }


@app.post("/api/wallet/decline-accelerated-offer/{feed_uuid}", status_code=200)
def decline_accelerated_wallet_offer(
    feed_uuid: str,
    req: AcceleratedWalletOfferRequest,
    db: Session = Depends(get_db),
):
    """Decline an Accelerated Wallet Push offer. Sets wallet_opt_out so the
    subscriber won't receive another push (until cleared manually)."""
    from config.settings import settings as _settings
    if not getattr(_settings, "accelerated_wallet_push_enabled", False):
        raise HTTPException(status_code=404, detail={"error": "feature_disabled"})

    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if sub is None:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")

    from src.core.models import WalletPushOffer
    offer = db.get(WalletPushOffer, req.offer_id)
    if offer is None or offer.subscriber_id != sub.id:
        raise HTTPException(status_code=404, detail={"error": "offer_not_found"})

    from datetime import datetime, timezone
    if offer.status == "offered":
        offer.status = "declined"
        offer.declined_at = datetime.now(timezone.utc)
    sub.wallet_opt_out = True
    db.flush()

    try:
        from src.core.redis_client import redis_available, rdelete
        if redis_available():
            rdelete(f"fa:pending_offer:{sub.id}")
    except Exception:
        pass

    try:
        from src.services.business_events import log_business_event
        log_business_event(
            "WALLET_DECLINED", subscriber_id=sub.id,
            payload={"offer_id": offer.id}, db=db,
        )
    except Exception:
        pass

    return {"offer_id": offer.id, "status": offer.status, "wallet_opt_out": True}


# ── Phase 2B: Payment Sheet — POST /api/payment-intent ───────────────────────

class PaymentIntentRequest(BaseModel):
    feed_uuid: str
    amount_cents: int = Field(..., gt=0, le=100000)  # max $1,000
    description: str
    save_card: bool = False
    metadata: Optional[dict] = None
    attribution: Optional[dict] = None  # Meta Ads attribution (utm_*, campaign_id, fbclid, ...)


@app.post("/api/payment-intent", status_code=201)
def create_payment_intent_endpoint(
    req: PaymentIntentRequest, request: Request, db: Session = Depends(get_db)
):
    """Create a Stripe PaymentIntent for the Payment Sheet SDK."""
    from src.services.payment_sheet import create_payment_intent

    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)
    ).scalar_one_or_none()
    if sub is None:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")

    # Acquire a 20-min lead hold for lead_unlock purchases to prevent double-selling
    if req.metadata and req.metadata.get("product") == "lead_unlock":
        try:
            property_id = int(req.metadata["property_id"])
            from src.services.lead_hold import hold as acquire_hold
            hold_result = acquire_hold(property_id, sub.id)
            if not hold_result.get("held"):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "lead_held",
                        "message": "This lead is currently being purchased by another subscriber. Try again in a few minutes.",
                    },
                )
        except HTTPException:
            raise
        except Exception as exc:
            logger.warning("[PaymentIntent] lead hold check failed (non-blocking): %s", exc)

    # Merge buyer attribution / Meta CAPI context; existing product metadata wins.
    pi_metadata = {**_attribution_stripe_metadata(request, req.attribution), **(req.metadata or {})}

    try:
        result = create_payment_intent(
            subscriber_id=sub.id,
            amount_cents=req.amount_cents,
            description=req.description,
            save_card=req.save_card,
            db=db,
            metadata=pi_metadata,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    return result


# ── Stage 5: Premium credit SKUs ─────────────────────────────────────────────

class PremiumPurchaseRequest(BaseModel):
    feed_uuid: str
    sku: str = Field(..., description="report | brief | transfer | byol")
    payment_mode: str = Field(..., description="credits | card")
    property_id: Optional[int] = None
    target_address: Optional[str] = Field(default=None, max_length=255)
    attribution: Optional[dict] = None  # Meta Ads attribution (utm_*, campaign_id, fbclid, ...)

    @field_validator("sku")
    @classmethod
    def _valid_sku(cls, v: str) -> str:
        if v not in {"report", "brief", "transfer", "byol"}:
            raise ValueError("sku must be one of report|brief|transfer|byol")
        return v

    @field_validator("payment_mode")
    @classmethod
    def _valid_mode(cls, v: str) -> str:
        if v not in {"credits", "card"}:
            raise ValueError("payment_mode must be credits or card")
        return v


@app.post("/api/premium/purchase", status_code=201)
def premium_purchase_endpoint(
    req: PremiumPurchaseRequest, request: Request, db: Session = Depends(get_db)
):
    """
    Stage 5 — Premium credit SKU purchase.

    `payment_mode='credits'` debits the wallet immediately and runs fulfillment
    in-band (returns 402 if balance insufficient with a topup deep link).

    `payment_mode='card'` creates a Stripe PaymentIntent for the Payment Sheet;
    the row is persisted on payment_intent.succeeded webhook.
    """
    from config.revenue_ladder import PREMIUM_CREDITS

    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)
    ).scalar_one_or_none()
    if sub is None:
        raise HTTPException(status_code=403, detail="Invalid feed_uuid")

    # Block subscribers flagged after repeated disputes (fa004, 2026-05-04)
    if sub.status == "disputed":
        raise HTTPException(
            status_code=403,
            detail="Account on hold for review. Email support@forcedaction.io.",
        )

    cfg = PREMIUM_CREDITS[req.sku]

    # SKU-specific argument validation
    if req.sku in ("report", "brief", "transfer") and not req.property_id:
        raise HTTPException(status_code=400, detail=f"{req.sku} requires property_id")
    if req.sku == "byol" and not req.target_address:
        raise HTTPException(status_code=400, detail="byol requires target_address")

    # Per-subscriber rate cap on the highest-ticket SKU. Prevents burst-and-
    # dispute attacks on Transfer ($65/26cr).
    if req.sku == "transfer":
        from src.core.models import PremiumPurchase
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        recent_transfers = db.execute(
            select(func.count()).select_from(PremiumPurchase).where(
                PremiumPurchase.subscriber_id == sub.id,
                PremiumPurchase.sku == "transfer",
                PremiumPurchase.purchased_at >= cutoff,
                # Don't count refunded/disputed against the cap (those are losses, not abuse)
                PremiumPurchase.status.notin_(["refunded", "disputed", "failed"]),
            )
        ).scalar() or 0
        if recent_transfers >= 3:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "transfer_daily_cap_reached",
                    "message": "Maximum 3 skip-trace transfers per 24 hours.",
                    "retry_after_hours": 24,
                },
            )

    if req.payment_mode == "credits":
        from src.services import wallet_engine
        from src.services.premium_engine import record_credit_purchase, fulfill
        ok = wallet_engine.debit(sub.id, action=req.sku, db=db, description=f"premium_{req.sku}")
        if not ok:
            balance = wallet_engine.get_balance(sub.id, db)
            return JSONResponse(
                status_code=402,
                content={
                    "error": "insufficient_credits",
                    "balance": balance,
                    "required": cfg["credits_cost"],
                    "topup_url": f"/dashboard/{sub.event_feed_uuid}?wallet=topup",
                },
            )
        purchase = record_credit_purchase(
            subscriber_id=sub.id,
            sku=req.sku,
            db=db,
            property_id=req.property_id,
            target_address=req.target_address,
        )
        try:
            fulfill(purchase.id, db)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {
            "purchase_id": purchase.id,
            "sku": req.sku,
            "paid_via": "credits",
            "credits_spent": cfg["credits_cost"],
            "status": purchase.status,
            "output_ref": purchase.output_ref,
        }

    # payment_mode == "card"
    settings = get_settings()
    price_id = settings.active_stripe_price(f"premium_{req.sku}")
    if not price_id:
        raise HTTPException(
            status_code=503,
            detail=f"Stripe price not configured for premium_{req.sku}",
        )

    from src.services.payment_sheet import create_payment_intent as _create_pi
    try:
        result = _create_pi(
            subscriber_id=sub.id,
            amount_cents=cfg["retail_price_cents"],
            description=f"Premium {cfg['label']}",
            # Match the $4 lead-unlock UX: save card by default so premium-only
            # buyers also enter the accelerated-wallet-push funnel. The
            # PremiumCreditsModal already pre-checks "save card" on the
            # PaymentSheet UI; this aligns the server-side flag with that
            # default. The user can still uncheck on the Stripe sheet.
            save_card=True,
            db=db,
            metadata={
                **_attribution_stripe_metadata(request, req.attribution),
                "product": "premium",
                "sku": req.sku,
                "property_id": str(req.property_id) if req.property_id else "",
                "target_address": req.target_address or "",
            },
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {
        "client_secret": result["client_secret"],
        "payment_intent_id": result["payment_intent_id"],
        "amount": result["amount"],
        "publishable_key": result["publishable_key"],
        "sku": req.sku,
        "paid_via": "card",
    }


# ── Phase 2B: Missed-Call Voice Webhook ──────────────────────────────────────

@app.post("/webhooks/telnyx/voice", include_in_schema=False)
async def telnyx_voice_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Telnyx Programmable Voice webhook (replaces /webhooks/twilio/voice).

    Verifies the Ed25519 signature, extracts the caller phone from the
    Telnyx call.initiated event envelope, then routes through the
    existing signup_engine.handle_missed_call() — auto-creates a free
    account and sends a welcome SMS.

    Returns 200 with an empty JSON body. Telnyx Call Control commands
    (hang up, redirect, etc.) flow through the separate REST API, not
    the webhook reply, so no TeXML response is needed here.
    """
    from src.services.signup_engine import handle_missed_call
    from src.services.telnyx_signature import (
        SIGNATURE_HEADER, TIMESTAMP_HEADER, verify as verify_telnyx_signature,
    )
    from src.services.webhook_log import log_webhook_event

    raw_body = await request.body()
    settings_obj = get_settings()

    if not verify_telnyx_signature(
        body=raw_body,
        signature_b64=request.headers.get(SIGNATURE_HEADER),
        timestamp=request.headers.get(TIMESTAMP_HEADER),
        public_key_b64=settings_obj.telnyx_public_key,
    ):
        log_webhook_event(
            source="telnyx_voice",
            event_type="call_initiated",
            status="failed",
            status_detail="signature_verification_failed",
        )
        raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        envelope = json.loads(raw_body.decode("utf-8") or "{}")
    except json.JSONDecodeError as exc:
        logger.warning("Telnyx voice body not JSON-decodable: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    data = envelope.get("data") or {}
    event_type = data.get("event_type", "")
    payload = data.get("payload") or {}
    from_number = (payload.get("from") or {}).get("phone_number", "")
    call_id = payload.get("call_control_id") or payload.get("call_leg_id")

    log_webhook_event(
        source="telnyx_voice",
        event_type=event_type or "call_initiated",
        source_event_id=call_id,
        status="received",
        payload=envelope,
        payload_kind="telnyx",
    )

    if event_type != "call.initiated":
        # We only act on the inbound-call-start event. Other voice events
        # (answered, hangup, etc.) share this URL but don't need handler
        # routing for the signup flow.
        return Response(content="", media_type="application/json")

    if not from_number:
        logger.warning("[Voice] Inbound call with no From number")
        return Response(content="", media_type="application/json")

    handle_missed_call(from_number=from_number, db=db)
    return Response(content="", media_type="application/json")


# ── Phase 2B Frontend: Pause / Resume / Partner / AP-Lite endpoints ─────────

class PauseRequest(BaseModel):
    feed_uuid: str
    days: int = 60

class ResumeRequest(BaseModel):
    feed_uuid: str

class PartnerCheckoutRequest(BaseModel):
    feed_uuid: str
    zip_codes: List[str]
    vertical: str


@app.post("/api/pause-subscription")
def pause_subscription_endpoint(req: PauseRequest, db: Session = Depends(get_db)):
    sub = db.execute(select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)).scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    from src.services.pause_subscription import pause_subscriber
    ok = pause_subscriber(db, sub.id, days=req.days)
    if not ok:
        raise HTTPException(status_code=409, detail={"error": "already_paused", "message": "Subscription already paused or invalid."})
    db.refresh(sub)
    return {"ok": True, "paused_at": sub.paused_at.isoformat() if sub.paused_at else None, "resume_at": sub.pause_resume_at.isoformat() if sub.pause_resume_at else None}


@app.post("/api/resume-subscription")
def resume_subscription_endpoint(req: ResumeRequest, db: Session = Depends(get_db)):
    sub = db.execute(select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)).scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    from src.services.pause_subscription import resume_subscriber
    ok = resume_subscriber(db, sub.id)
    if not ok:
        raise HTTPException(status_code=409, detail={"error": "not_paused", "message": "Subscription is not paused."})
    return {"ok": True, "resumed_at": datetime.now(timezone.utc).isoformat()}


@app.get("/api/upgrade/partner/eligibility")
def partner_eligibility(feed_uuid: str, db: Session = Depends(get_db)):
    sub = db.execute(select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)).scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    from src.services.partner_tier import is_eligible
    eligible, reason = is_eligible(sub)
    return {
        "eligible": eligible,
        "reason": reason,
        "current_tier": sub.tier,
        "county_id": sub.county_id or "fl_hillsborough",
        "vertical": sub.vertical or "roofing",
    }


@app.post("/api/upgrade/partner")
def partner_checkout(req: PartnerCheckoutRequest, db: Session = Depends(get_db)):
    sub = db.execute(select(Subscriber).where(Subscriber.event_feed_uuid == req.feed_uuid)).scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    from src.services.partner_tier import is_eligible, validate_zip_selection
    eligible, reason = is_eligible(sub)
    if not eligible:
        raise HTTPException(status_code=403, detail={"error": "not_eligible", "message": reason})
    zip_check = validate_zip_selection(db, req.zip_codes, req.vertical, sub.county_id or "fl_hillsborough")
    if not zip_check["ok"]:
        raise HTTPException(status_code=409, detail={"error": zip_check.get("reason"), "message": zip_check.get("reason"), "zips": zip_check.get("zips", [])})
    from src.services.stripe_service import create_subscription_checkout
    from config.settings import get_settings
    _s = get_settings()
    base = _s.app_base_url.rstrip("/")
    try:
        result = create_subscription_checkout(
            db=db,
            tier="partner",
            vertical=req.vertical,
            county_id=sub.county_id or "fl_hillsborough",
            zip_codes=req.zip_codes,
            success_url=f"{base}/success?tier=partner&zips={','.join(req.zip_codes)}",
            cancel_url=f"{base}/dashboard/{sub.event_feed_uuid}/partner",
            customer_email=sub.email,
        )
        return {"checkout_url": result["url"]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"error": "checkout_failed", "message": str(exc)})


@app.post("/api/upgrade/ap-lite")
def ap_lite_upgrade(req: UpgradeRequest, db: Session = Depends(get_db)):
    req.tier = "autopilot_lite"
    return upgrade(req, db)


# ---------------------------------------------------------------------------
# Referral Core Loop endpoints
# ---------------------------------------------------------------------------

class ClaimBonusZipRequest(BaseModel):
    zip_code: str

    @field_validator("zip_code")
    @classmethod
    def validate_zip(cls, v: str) -> str:
        if not _ZIP_RE.match(v):
            raise ValueError("ZIP code must be exactly 5 digits")
        return v


@app.get("/api/referral/status/{feed_uuid}")
def referral_status(feed_uuid: str, db: Session = Depends(get_db)):
    """
    Returns the referrer's referral program status.
    Authenticated by event_feed_uuid (same pattern as the lead feed).
    """
    subscriber = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if not subscriber:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Feed not found"})

    from src.core.models import ReferralEvent, ReferralMilestoneAward
    from config.settings import get_settings

    confirmed_count = len(db.execute(
        select(ReferralEvent).where(
            ReferralEvent.referrer_subscriber_id == subscriber.id,
            ReferralEvent.status.in_(("confirmed", "rewarded")),
        )
    ).scalars().all())

    milestones_awarded = [
        {"milestone": row.milestone, "awarded_at": row.awarded_at.isoformat()}
        for row in db.execute(
            select(ReferralMilestoneAward).where(
                ReferralMilestoneAward.referrer_subscriber_id == subscriber.id
            )
        ).scalars().all()
    ]
    awarded_names = {m["milestone"] for m in milestones_awarded}

    next_milestone = None
    if "free_month_3" not in awarded_names and confirmed_count < 3:
        next_milestone = {"milestone": "free_month_3", "threshold": 3, "remaining": 3 - confirmed_count}
    elif "lock_slot_5" not in awarded_names and confirmed_count < 5:
        next_milestone = {"milestone": "lock_slot_5", "threshold": 5, "remaining": 5 - confirmed_count}

    settings = get_settings()
    # `app_base_url` is the canonical setting; `base_url` was the older
    # name and is no longer defined, so the getattr fallback would always
    # produce a relative URL ("/share/REFXXXX"). Frontend now composes the
    # absolute URL from window.location.origin when the backend value isn't
    # absolute, so this still works either way.
    base_url = (getattr(settings, "app_base_url", "") or "").rstrip("/")
    share_url = f"{base_url}/share/{subscriber.referral_code}" if subscriber.referral_code else None

    return {
        "confirmed_count": confirmed_count,
        "milestones_awarded": milestones_awarded,
        "next_milestone": next_milestone,
        "bonus_zip_slots": subscriber.bonus_zip_slots,
        "share_url": share_url,
    }


@app.post("/api/referral/claim-bonus-zip/{feed_uuid}")
def claim_bonus_zip(feed_uuid: str, body: ClaimBonusZipRequest, db: Session = Depends(get_db)):
    """
    Redeem one bonus ZIP lock slot granted by the 5-referral milestone.
    Authenticated by event_feed_uuid.
    """
    subscriber = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if not subscriber:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Feed not found"})

    if subscriber.bonus_zip_slots <= 0:
        raise HTTPException(status_code=409, detail={
            "error": "no_bonus_slots",
            "message": "No bonus ZIP slots available. Refer 5 paying users to earn one.",
        })

    # Validate ZIP is within the subscriber's county.
    # Primary: 3-digit prefix check against county config (fast, no DB hit).
    # Fallback: if zip_prefixes is not configured for this county, verify the
    #           ZIP exists in the properties table for that county instead.
    from src.utils.county_config import get_county, is_zip_in_county
    try:
        county_cfg = get_county(subscriber.county_id)
        zip_prefixes = county_cfg.get("zip_prefixes") or []
        if zip_prefixes:
            in_county = is_zip_in_county(subscriber.county_id, body.zip_code)
        else:
            # zip_prefixes not configured — fall back to properties table
            in_county = db.execute(
                text("""
                    SELECT 1 FROM properties
                    WHERE zip = :zip AND county_id = :county
                    LIMIT 1
                """),
                {"zip": body.zip_code, "county": subscriber.county_id},
            ).first() is not None
    except KeyError:
        in_county = True  # unknown county_id — skip strict check rather than 500

    if not in_county:
        raise HTTPException(status_code=400, detail={
            "error": "zip_out_of_county",
            "message": f"ZIP {body.zip_code} is not in your county ({subscriber.county_id}).",
        })

    # Unique constraint is on (zip_code, vertical, county_id) regardless of
    # status — fetch any existing row, not just locked ones.
    existing = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code == body.zip_code,
            ZipTerritory.vertical == subscriber.vertical,
            ZipTerritory.county_id == subscriber.county_id,
        )
    ).scalar_one_or_none()

    if existing and existing.subscriber_id == subscriber.id:
        raise HTTPException(status_code=400, detail={
            "error": "zip_already_owned",
            "message": f"ZIP {body.zip_code} is already in your territory.",
        })
    if existing and existing.status == "locked":
        raise HTTPException(status_code=400, detail={
            "error": "zip_already_locked",
            "message": f"ZIP {body.zip_code} is already locked by another subscriber.",
        })

    # Grant the bonus ZIP — either reclaim the existing (non-locked) row or
    # insert a fresh one.
    from sqlalchemy import update as _update
    now = datetime.now(timezone.utc)
    if existing:
        existing.subscriber_id = subscriber.id
        existing.status = "locked"
        existing.locked_at = now
        existing.grace_expires_at = None
        existing.updated_at = now
    else:
        db.add(ZipTerritory(
            zip_code=body.zip_code,
            vertical=subscriber.vertical,
            county_id=subscriber.county_id,
            subscriber_id=subscriber.id,
            status="locked",
            locked_at=now,
        ))
    db.execute(
        _update(Subscriber)
        .where(Subscriber.id == subscriber.id)
        .values(bonus_zip_slots=Subscriber.bonus_zip_slots - 1)
    )
    db.flush()

    return {
        "ok": True,
        "zip_code": body.zip_code,
        "bonus_zip_slots_remaining": subscriber.bonus_zip_slots - 1,
    }


@app.get("/share/{referral_code}", include_in_schema=False)
def referral_share_page(referral_code: str, db: Session = Depends(get_db)):
    """
    Public referral landing page. Looks up the referrer's vertical and
    renders current weekly forward-pack copy with a signup CTA.
    """
    from src.services.forward_pack_renderer import get_current_copy
    from fastapi.responses import HTMLResponse

    referrer = db.execute(
        select(Subscriber).where(Subscriber.referral_code == referral_code)
    ).scalar_one_or_none()
    if not referrer:
        raise HTTPException(status_code=404, detail="Referral link not found")

    copy_body = get_current_copy(referrer.vertical, db)
    _settings = get_settings()
    base_url = getattr(_settings, "base_url", "")
    signup_url = f"{base_url}/?ref={referral_code}"

    html = f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Join Forced Action</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:sans-serif;max-width:640px;margin:40px auto;padding:0 20px;line-height:1.6}}
h1{{font-size:1.5rem}}a.cta{{display:inline-block;margin-top:24px;padding:12px 28px;background:#1a56db;color:#fff;border-radius:6px;text-decoration:none;font-weight:600}}</style>
</head>
<body>
<h1>You've been invited</h1>
<p>{copy_body or "Join the platform that finds distressed properties before anyone else."}</p>
<a href="{signup_url}" class="cta">Get started free →</a>
</body>
</html>"""
    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# SPA catch-all — must be LAST so it never shadows /api/* or /webhooks/*
# Handles any client-side route (e.g. /dashboard/:uuid/settings, /proof-wall)
# that the browser requests directly on reload or deep-link.
# ---------------------------------------------------------------------------

