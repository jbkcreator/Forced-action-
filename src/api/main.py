"""
Forced Action — FastAPI application.

Endpoints:
    POST /webhooks/stripe          — Stripe event receiver
    GET  /api/founding-spots       — Founding countdown for landing page
    GET  /api/zip-check            — ZIP availability checker for landing page
    POST /api/checkout             — Create Stripe checkout session
    GET  /api/feed/{uuid}          — Event Feed for subscribers (paginated leads)
    GET  /                         — Landing page
"""

import logging
import re
from pathlib import Path
from typing import Optional

import stripe
from fastapi import FastAPI, Header, HTTPException, Request, Depends, Query
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator, EmailStr
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, desc, func

from src.core.database import get_db_context
from src.core.models import FoundingSubscriberCount, ZipTerritory, Subscriber, Property, DistressScore, Incident
from src.services.stripe_webhooks import handle_webhook
from src.services.stripe_service import get_price_id_for_checkout
from config.settings import get_settings

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"

VALID_TIERS = {"starter", "pro", "dominator"}
VALID_VERTICALS = {"roofing", "remediation", "investor"}

app = FastAPI(title="Forced Action API", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


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
# GET / — Landing page
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def landing_page():
    return FileResponse(str(STATIC_DIR / "index.html"))


# ---------------------------------------------------------------------------
# POST /api/checkout — Create Stripe checkout session
# ---------------------------------------------------------------------------

class CheckoutRequest(BaseModel):
    tier: str        # starter | pro | dominator
    vertical: str    # roofing | remediation | investor
    county_id: str   # hillsborough

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


@app.post("/api/checkout")
def create_checkout(payload: CheckoutRequest, db: Session = Depends(get_db)):
    _s = get_settings()
    stripe.api_key = _s.stripe_secret_key.get_secret_value()

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

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            ui_mode="embedded",
            line_items=[{"price": price_id, "quantity": 1}],
            metadata={
                "tier": payload.tier,
                "vertical": payload.vertical,
                "county_id": payload.county_id,
                "is_founding": str(is_founding),
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
# GET /success — Post-checkout confirmation page
# ---------------------------------------------------------------------------

@app.get("/success", include_in_schema=False)
def success_page():
    return FileResponse(str(STATIC_DIR / "success.html"))


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
        except Exception:
            logger.error("Unhandled webhook handler error", exc_info=True)
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

    FOUNDING_CAP = 10
    TIERS = ["starter", "pro", "dominator"]
    TOTAL_CAP = FOUNDING_CAP * len(TIERS)  # 30

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

    FOUNDING_CAP = 10

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
# GET /api/feed/{uuid}
# ---------------------------------------------------------------------------

@app.get("/api/feed/{feed_uuid}")
def event_feed(
    feed_uuid: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    min_score: Optional[float] = Query(default=None, ge=0.0, le=100.0),
    incident_type: Optional[str] = Query(default=None),
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

    base_query = (
        select(Property, DistressScore)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .where(and_(*filters))
        .order_by(desc(score_col))
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
        incident_filter = [Incident.property_id.in_(property_ids)]
        if incident_type:
            incident_filter.append(Incident.incident_type == incident_type)

        incidents_raw = db.execute(
            select(Incident).where(and_(*incident_filter))
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
            "incidents": incidents_by_prop.get(prop.id, []),
        })

    return {
        "feed_uuid": feed_uuid,
        "subscriber": {
            "tier": subscriber.tier,
            "vertical": subscriber.vertical,
            "county_id": subscriber.county_id,
            "locked_zips": list(locked_zips),
        },
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": -(-total // page_size),  # ceiling division
        "leads": leads,
    }


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
