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

import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests as _requests
import stripe
from fastapi import FastAPI, Header, HTTPException, Request, Depends, Query
from fastapi.exception_handlers import http_exception_handler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator, EmailStr
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_, desc, func, cast, text, Date

from src.core.database import get_db_context
from src.core.models import FoundingSubscriberCount, ZipTerritory, Subscriber, Property, DistressScore, Incident, LeadPackPurchase, ScraperRunStats, EnrichedContact
from src.services.stripe_webhooks import handle_webhook
from src.services.stripe_service import get_price_id_for_checkout
from config.settings import get_settings
from config.scoring import VERTICAL_WEIGHTS

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"
REACT_DIST = Path(__file__).parent.parent.parent.parent / "Forced-action-ui" / "dist"

VALID_TIERS = {"starter", "pro", "dominator"}
VALID_VERTICALS = set(VERTICAL_WEIGHTS.keys())

app = FastAPI(title="Forced Action API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

from src.api.admin_router import router as admin_router  # noqa: E402
app.include_router(admin_router)

# Mount React build assets (JS/CSS chunks) if the dist directory exists
if REACT_DIST.is_dir() and (REACT_DIST / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=str(REACT_DIST / "assets")), name="react-assets")


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
# DB dependency
# ---------------------------------------------------------------------------

def get_db():
    with get_db_context() as db:
        yield db


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
    if not settings.idi_api_key:
        missing_optional.append("IDI_API_KEY")
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
        status_code=200 if overall == "ok" else 503,
        content={
            "status": overall,
            "checks": checks,
            "checked_at": checked_at,
        },
    )


# ---------------------------------------------------------------------------
# GET / — Landing page (React SPA or static HTML fallback)
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def landing_page():
    react_index = REACT_DIST / "index.html"
    if react_index.is_file():
        return FileResponse(str(react_index))
    return FileResponse(str(STATIC_DIR / "index.html"))


# ---------------------------------------------------------------------------
# POST /api/checkout — Create Stripe checkout session
# ---------------------------------------------------------------------------

class CheckoutRequest(BaseModel):
    tier: str        # starter | pro | dominator
    vertical: str    # roofing | remediation | investor
    county_id: str   # hillsborough
    zip_codes: list[str] = []  # ZIP territories to lock on purchase
    email: str       # collected before checkout — used to block duplicate subscriptions

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
def create_checkout(payload: CheckoutRequest, db: Session = Depends(get_db)):

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

    # Block duplicate subscriptions — check before creating Stripe session so the
    # user is never charged twice.  Email is normalised by the request validator.
    try:
        existing_sub = db.execute(
            select(Subscriber).where(
                Subscriber.email == payload.email,
                Subscriber.vertical == payload.vertical,
                Subscriber.county_id == payload.county_id,
                Subscriber.status.in_(["active", "grace"]),
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

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            ui_mode="embedded",
            customer_email=payload.email,   # pre-fills email in Stripe form
            line_items=[{"price": price_id, "quantity": 1}],
            metadata={
                "tier": payload.tier,
                "vertical": payload.vertical,
                "county_id": payload.county_id,
                "is_founding": str(is_founding),
                "founding_price_id": price_id if is_founding else "",
                "zip_codes": ",".join(payload.zip_codes),
            },
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


# GET /success — Post-checkout confirmation page (React SPA)
# ---------------------------------------------------------------------------

@app.get("/success", include_in_schema=False)
def success_page():
    react_index = REACT_DIST / "index.html"
    if react_index.is_file():
        return FileResponse(str(react_index))
    return FileResponse(str(STATIC_DIR / "success.html"))


# ---------------------------------------------------------------------------
# GET /dashboard/{feed_uuid} — Subscriber dashboard (React SPA)
# ---------------------------------------------------------------------------

@app.get("/dashboard/{feed_uuid}", include_in_schema=False)
def dashboard_page(feed_uuid: str):
    react_index = REACT_DIST / "index.html"
    if react_index.is_file():
        return FileResponse(str(react_index))
    return FileResponse(str(STATIC_DIR / "dashboard.html"))


# ---------------------------------------------------------------------------
# GET /email-previews — Email template previews (React SPA)
# ---------------------------------------------------------------------------

@app.get("/email-previews", include_in_schema=False)
def email_previews_page():
    react_index = REACT_DIST / "index.html"
    if react_index.is_file():
        return FileResponse(str(react_index))
    return FileResponse(str(STATIC_DIR / "email-previews.html"))


@app.get("/admin", include_in_schema=False)
def admin_page():
    react_index = REACT_DIST / "index.html"
    if react_index.is_file():
        return FileResponse(str(react_index))
    raise HTTPException(status_code=503, detail="Admin UI not built — run npm run build in Forced-action-ui/")


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

_ZIP_RE = re.compile(r"^\d{5}$")
_FLORIDA_PREFIXES = ("33", "34")


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
            select(ZipTerritory.zip_code, ZipTerritory.status).where(
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

    # Gold+ lead counts per ZIP (latest scores)
    try:
        lead_rows = db.execute(
            text("""
                SELECT p.zip, COUNT(*) AS lead_count
                FROM properties p
                JOIN distress_scores ds ON ds.property_id = p.id
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
        })

    return {
        "vertical": vertical,
        "county_id": county_id,
        "zips": result,
    }


# ---------------------------------------------------------------------------
# GET /api/feed/{uuid}
# ---------------------------------------------------------------------------

_VALID_SORTS = {"score_desc", "newest", "value_desc"}


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
):
    """
    Subscriber-facing Event Feed.
    Authenticated by event_feed_uuid in the URL.
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

    if subscriber.status not in ("active", "grace"):
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
        return {
            "feed_uuid": feed_uuid,
            "subscriber": {"tier": subscriber.tier, "vertical": subscriber.vertical},
            "total": 0,
            "page": page,
            "page_size": page_size,
            "pages": 0,
            "leads": [],
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

    # Sort order
    _sort = sort if sort in _VALID_SORTS else "score_desc"
    if _sort == "newest":
        order_col = desc(DistressScore.score_date)
    elif _sort == "value_desc":
        order_col = desc(DistressScore.final_cds_score)  # CDS as value proxy until job estimator is query-able
    else:
        order_col = desc(score_col)

    base_query = (
        select(Property, DistressScore)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .where(and_(*filters))
        .order_by(order_col)
    )

    # 4. Total count
    try:
        count_q = select(func.count()).select_from(base_query.subquery())
        total = db.execute(count_q).scalar()

        # 5. Paginate
        offset = (page - 1) * page_size
        rows = db.execute(base_query.offset(offset).limit(page_size)).all()
    except OperationalError:
        logger.error("DB error fetching leads for feed", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    # 6. Fetch incidents for returned properties in one query
    property_ids = [prop.id for prop, _ in rows]

    try:
        incidents_raw = db.execute(
            select(Incident).where(Incident.property_id.in_(property_ids))
        ).scalars().all()
    except OperationalError:
        logger.error("DB error fetching incidents for feed", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    incidents_by_prop: dict = {}
    for inc in incidents_raw:
        incidents_by_prop.setdefault(inc.property_id, []).append({
            "type": inc.incident_type,
            "date": inc.incident_date.isoformat() if inc.incident_date else None,
        })

    # 7. Build response
    leads = []
    for prop, score in rows:
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
            "lead_tier": score.lead_tier,
            "urgency": score.urgency_level,
            "distress_types": score.distress_types,
            "est_job_value": _estimate_lead_job_value(prop, score),
            "incidents": incidents_by_prop.get(prop.id, []),
        })

    return {
        "feed_uuid": feed_uuid,
        "subscriber": {
            "tier": subscriber.tier,
            "vertical": subscriber.vertical,
            "county_id": subscriber.county_id,
            "locked_zips": list(locked_zips),
            "founding_member": subscriber.founding_member,
            "status": subscriber.status,
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
def feed_stats(feed_uuid: str, db: Session = Depends(get_db)):
    """Aggregate stats for the subscriber's feed: totals, new today, tier breakdown."""
    from datetime import date, timezone, datetime as _dt

    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
        ).scalar_one_or_none()
    except OperationalError:
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if not subscriber:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": "Feed not found"})

    if subscriber.status not in ("active", "grace"):
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

        total = db.execute(select(func.count()).select_from(base_q.subquery())).scalar()

        today_start = _dt.combine(date.today(), _dt.min.time())
        new_today = db.execute(
            select(func.count(func.distinct(DistressScore.property_id))).select_from(
                base_q.where(DistressScore.score_date >= today_start).subquery()
            )
        ).scalar()

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
        from src.services.stripe_webhooks import _send_welcome_email
        _send_welcome_email(subscriber)
    except Exception:
        logger.error("Failed to resend confirmation for feed %s", payload.feed_uuid, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "send_failed", "message": "Failed to send email"})

    return {"ok": True}


def _estimate_lead_job_value(prop, score) -> dict:
    """Compute job value estimate for a feed lead."""
    try:
        from src.services.job_estimator import estimate_job_value
        distress_types = score.distress_types or []
        return estimate_job_value(prop, distress_types)
    except Exception:
        return {"low": 0, "high": 0, "display": "N/A", "method": "error"}


# ---------------------------------------------------------------------------
# POST /api/waitlist — Add email to ZIP waitlist
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class WaitlistRequest(BaseModel):
    zip_code: str
    vertical: str
    county_id: str = "hillsborough"
    name: str
    email: str

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

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        return v


@app.post("/api/waitlist", status_code=201)
def join_waitlist(payload: WaitlistRequest, db: Session = Depends(get_db)):
    """
    Add an email to the waitlist for a taken/grace ZIP territory.
    Appended to ZipTerritory.waitlist_emails array.
    """
    try:
        territory = db.execute(
            select(ZipTerritory).where(
                ZipTerritory.zip_code == payload.zip_code,
                ZipTerritory.vertical == payload.vertical,
                ZipTerritory.county_id == payload.county_id,
            )
        ).scalar_one_or_none()
    except OperationalError:
        logger.error("DB error fetching territory in waitlist", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    if territory is None:
        territory = ZipTerritory(
            zip_code=payload.zip_code,
            vertical=payload.vertical,
            county_id=payload.county_id,
            status="available",
            waitlist_emails=[payload.email],
        )
        db.add(territory)
    else:
        existing = list(territory.waitlist_emails or [])
        if payload.email in existing:
            return {"status": "already_registered", "zip_code": payload.zip_code, "email": payload.email}
        existing.append(payload.email)
        territory.waitlist_emails = existing

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        logger.warning("Integrity error on waitlist insert for %s / %s", payload.zip_code, payload.email)
        return {"status": "already_registered", "zip_code": payload.zip_code, "email": payload.email}
    except OperationalError:
        db.rollback()
        logger.error("DB error committing waitlist entry", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    return {"status": "added", "zip_code": payload.zip_code, "email": payload.email}


# ---------------------------------------------------------------------------
# GET /api/sample-leads — 3 top-scored properties for a ZIP (phone blurred)
# ---------------------------------------------------------------------------

@app.get("/api/sample-leads")
def sample_leads(
    zip_code: str,
    vertical: str = "roofing",
    county_id: str = "hillsborough",
    db: Session = Depends(get_db),
):
    """
    Returns up to 3 real top-scored properties from a ZIP, with phone blurred.
    Used by the landing page to show sample leads after ZIP check.
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

    try:
        rows = db.execute(
            select(Property, DistressScore)
            .join(DistressScore, DistressScore.property_id == Property.id)
            .where(
                and_(
                    Property.zip == zip_code,
                    Property.county_id == county_id,
                    DistressScore.qualified == True,
                )
            )
            .order_by(desc(score_col))
            .limit(3)
        ).all()
    except OperationalError:
        logger.error("DB error fetching sample leads", exc_info=True)
        raise HTTPException(status_code=503, detail={"error": "service_unavailable", "message": "Database temporarily unavailable"})

    leads = []
    for prop, score in rows:
        try:
            inc = db.execute(
                select(Incident)
                .where(Incident.property_id == prop.id)
                .order_by(desc(Incident.incident_date))
                .limit(1)
            ).scalar_one_or_none()
        except OperationalError:
            inc = None  # non-fatal — degrade gracefully

        leads.append({
            "address": prop.address,
            "city": prop.city,
            "zip": prop.zip,
            "year_built": prop.year_built,
            "sq_ft": prop.sq_ft,
            "cds_score": float(score.final_cds_score) if score.final_cds_score else None,
            "vertical_score": score.vertical_scores.get(vertical) if score.vertical_scores else None,
            "lead_tier": score.lead_tier,
            "distress_types": score.distress_types,
            "latest_incident": inc.incident_type if inc else None,
            "latest_incident_date": inc.incident_date.isoformat() if inc and inc.incident_date else None,
            "phone": "•••-•••-••••",  # blurred
        })

    return {"zip_code": zip_code, "vertical": vertical, "leads": leads}


# ---------------------------------------------------------------------------
# POST /api/lead-pack/checkout — Create PaymentIntent for a lead pack purchase
# ---------------------------------------------------------------------------

class LeadPackCheckoutRequest(BaseModel):
    feed_uuid: str
    zip_code: str
    vertical: str
    county_id: str = "hillsborough"


@app.post("/api/lead-pack/checkout")
def lead_pack_checkout(payload: LeadPackCheckoutRequest, db: Session = Depends(get_db)):
    """
    Create a Stripe PaymentIntent for a $99 lead pack.
    Returns { client_secret, publishable_key, amount, currency }.
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

    try:
        intent = stripe.PaymentIntent.create(
            amount=amount,
            currency=currency,
            metadata={
                "product":   "lead_pack",
                "feed_uuid": payload.feed_uuid,
                "zip_code":  payload.zip_code,
                "vertical":  payload.vertical,
                "county_id": payload.county_id,
            },
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
            "lead_tier": score.lead_tier,
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
    Flexible model — Synthflow and Finetuner.ai use slightly different field
    names. All outcome-critical fields are optional with sensible defaults so
    the handler degrades gracefully if a field is missing.
    """
    # Call identity
    call_id: Optional[str] = None

    # Prospect phone — various field names across providers
    to: Optional[str] = None
    prospect_phone: Optional[str] = None
    phone_number: Optional[str] = None
    phone: Optional[str] = None
    caller_phone: Optional[str] = None
    contact_phone: Optional[str] = None

    # Outcome — agent sets this via a variable during the call
    outcome: Optional[str] = None          # sample_requested | demo_requested | not_interested | voicemail | no_answer | completed
    call_status: Optional[str] = None      # Synthflow native: completed | no_answer | voicemail | failed

    # Variables captured by the agent during the call
    zip_code: Optional[str] = None
    vertical: Optional[str] = "roofing"
    prospect_name: Optional[str] = None
    notes: Optional[str] = None

    # Synthflow also sends these at top level
    duration: Optional[int] = None
    recording_url: Optional[str] = None

    @property
    def resolved_phone(self) -> Optional[str]:
        return self.prospect_phone or self.phone_number or self.to or self.phone or self.caller_phone or self.contact_phone

    @property
    def resolved_outcome(self) -> str:
        """Map Synthflow native call_status to our outcome taxonomy if needed."""
        if self.outcome:
            return self.outcome
        status_map = {
            "no_answer": "no_answer",
            "voicemail": "voicemail",
            "failed":    "no_answer",
            "completed": "completed",
        }
        return status_map.get(self.call_status or "", "completed")


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
async def synthflow_webhook(payload: SynthflowWebhookPayload):
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
    phone = payload.resolved_phone
    if not phone:
        logger.warning("[Synthflow webhook] received payload with no phone number")
        return {"status": "ignored", "reason": "no phone"}

    from src.services.synthflow_service import process_call_outcome
    try:
        result = process_call_outcome(
            prospect_phone=phone,
            outcome=payload.resolved_outcome,
            vertical=payload.vertical or "roofing",
            zip_code=payload.zip_code or "",
            prospect_name=payload.prospect_name or "",
            notes=payload.notes or "",
        )
    except Exception:
        logger.error("[Synthflow webhook] processing error for %s", phone, exc_info=True)
        # Always return 200 to Synthflow — retries on non-200 flood the queue
        return {"status": "error", "reason": "internal"}

    logger.info(
        "[Synthflow webhook] call_id=%s phone=%s outcome=%s contact=%s tags=%s",
        payload.call_id, phone, payload.resolved_outcome,
        result.get("contact_id"), result.get("tags_applied"),
    )
    return {"status": "ok", **result}


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
