"""
Stage 12 — Bankruptcy Filing Alert API.

Endpoints:
  POST /api/bankruptcy-alerts/checkout            — create Stripe checkout session (public)
  GET  /api/bankruptcy-alerts/options             — price + jurisdiction/chapter choices (public)
  GET  /api/bankruptcy-alerts/status/{token}      — subscriber's own status (access_token auth)
  PATCH /api/bankruptcy-alerts/preferences/{token} — update channels/filters (access_token auth)
  GET  /api/bankruptcy-alerts/alerts/status       — ops dashboard: recent alerts + counts (admin JWT)

Stripe webhook events for this product are delivered to the shared
/webhooks/stripe endpoint and routed by product (resolve_handler), verified
with the common active_stripe_webhook_secret — no dedicated webhook here.

Auth model:
  - checkout / options: open (payment is the gate)
  - status/preferences: per-subscriber access_token (uuid) — same pattern as feed uuid
  - alerts/status: admin JWT (Depends(get_current_admin))
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.bankruptcy_alert_config import (
    DEFAULT_CHAPTERS,
    DEFAULT_JURISDICTIONS,
    JURISDICTIONS,
    PRICE_MONTHLY_CENTS,
    RELEVANT_CHAPTERS,
)
from config.settings import get_settings
from src.api.admin_router import get_current_admin
from src.api.deps import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/bankruptcy-alerts", tags=["bankruptcy-alerts"])

# NOTE: No dedicated webhook endpoint. Bankruptcy Stripe events are delivered to
# the shared /webhooks/stripe endpoint and routed by product via
# bankruptcy_alert.subscription.resolve_handler (verified with the common
# active_stripe_webhook_secret).


# ── Request models ──────────────────────────────────────────────────────────────

class CheckoutRequest(BaseModel):
    email: Optional[str] = None
    jurisdictions: Optional[list[str]] = Field(default=None)
    chapters: Optional[list[str]] = Field(default=None)
    with_trial: bool = True
    success_url: Optional[str] = None
    cancel_url: Optional[str] = None

    @field_validator("email")
    @classmethod
    def _validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().lower()
        if "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError("invalid email")
        return v


class PreferencesRequest(BaseModel):
    channel_email: Optional[bool] = None
    channel_sms: Optional[bool] = None
    jurisdictions: Optional[list[str]] = None
    chapters: Optional[list[str]] = None


def _validate_filters(jurisdictions: Optional[list[str]], chapters: Optional[list[str]]) -> None:
    if jurisdictions:
        bad = [j for j in jurisdictions if j not in JURISDICTIONS]
        if bad:
            raise HTTPException(status_code=422, detail=f"Unknown jurisdiction(s): {bad}")
    if chapters:
        bad = [c for c in chapters if c not in RELEVANT_CHAPTERS]
        if bad:
            raise HTTPException(status_code=422, detail=f"Unsupported chapter(s): {bad}")


# ── Checkout ────────────────────────────────────────────────────────────────────

@router.post("/checkout")
def create_checkout_endpoint(body: CheckoutRequest):
    """Create a Stripe checkout session for the $297/mo product."""
    from src.services.bankruptcy_alert.subscription import create_checkout

    _validate_filters(body.jurisdictions, body.chapters)
    settings = get_settings()
    base = settings.app_base_url.rstrip("/")
    success_url = body.success_url or f"{base}/bankruptcy-alerts/success?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = body.cancel_url or f"{base}/bankruptcy-alerts"

    try:
        result = create_checkout(
            success_url=success_url,
            cancel_url=cancel_url,
            customer_email=body.email,
            jurisdictions=body.jurisdictions,
            chapters=body.chapters,
            with_trial=body.with_trial,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error("[bk-api] checkout failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="Checkout creation failed")

    return JSONResponse(result)


@router.get("/options")
def list_options():
    """Public: available jurisdictions, chapters, and price for the signup form."""
    return {
        "price_cents": PRICE_MONTHLY_CENTS,
        "price_display": f"${PRICE_MONTHLY_CENTS // 100}/mo",
        "jurisdictions": [
            {"id": jid, "label": cfg["label"]} for jid, cfg in JURISDICTIONS.items()
        ],
        "chapters": list(RELEVANT_CHAPTERS),
        "default_jurisdictions": DEFAULT_JURISDICTIONS,
        "default_chapters": DEFAULT_CHAPTERS,
    }




# Webhook: bankruptcy Stripe events arrive at the shared /webhooks/stripe
# endpoint (src/services/stripe_webhooks.py) and are routed by product via
# bankruptcy_alert.subscription.resolve_handler. No dedicated endpoint here.


# ── Subscriber self-service (access_token auth) ──────────────────────────────────

def _load_subscription(db: Session, token: str):
    row = db.execute(sa_text("""
        SELECT * FROM bankruptcy_alert_subscriptions WHERE access_token = :t LIMIT 1
    """), {"t": token}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return row


@router.get("/status/{token}")
def subscriber_status(token: str, db: Session = Depends(get_db)):
    """Subscriber's own status — access_token acts as the auth token."""
    sub = _load_subscription(db, token)
    # Count alerts delivered to this subscriber.
    cnt = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM bankruptcy_filing_alerts
        WHERE subscription_id = :id AND status = 'sent'
    """), {"id": sub.id}).first()
    return {
        "email": sub.email,
        "status": sub.status,
        "jurisdictions": sub.jurisdictions or DEFAULT_JURISDICTIONS,
        "chapters": sub.chapters or DEFAULT_CHAPTERS,
        "channel_email": sub.channel_email,
        "channel_sms": sub.channel_sms,
        "trial_ends_at": sub.trial_ends_at.isoformat() if sub.trial_ends_at else None,
        "alerts_delivered": int(cnt.c) if cnt else 0,
    }


@router.patch("/preferences/{token}")
def update_preferences(token: str, body: PreferencesRequest, db: Session = Depends(get_db)):
    """Update channels / filters. access_token auth."""
    sub = _load_subscription(db, token)
    _validate_filters(body.jurisdictions, body.chapters)

    import json as _json
    sets = []
    params: dict = {"id": sub.id}
    if body.channel_email is not None:
        sets.append("channel_email = :channel_email")
        params["channel_email"] = body.channel_email
    if body.channel_sms is not None:
        if body.channel_sms and not sub.phone:
            raise HTTPException(status_code=422, detail="No phone on file for SMS alerts")
        sets.append("channel_sms = :channel_sms")
        params["channel_sms"] = body.channel_sms
    if body.jurisdictions is not None:
        sets.append("jurisdictions = CAST(:jurisdictions AS jsonb)")
        params["jurisdictions"] = _json.dumps(body.jurisdictions)
    if body.chapters is not None:
        sets.append("chapters = CAST(:chapters AS jsonb)")
        params["chapters"] = _json.dumps(body.chapters)

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    sets.append("updated_at = NOW()")
    db.execute(sa_text(f"""
        UPDATE bankruptcy_alert_subscriptions SET {", ".join(sets)} WHERE id = :id
    """), params)
    db.commit()
    return {"status": "updated"}


# ── Ops dashboard (admin JWT) ─────────────────────────────────────────────────────

@router.get("/alerts/status")
def alerts_status(
    limit: int = 50,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Recent alerts + subscriber counts. Admin JWT required."""
    from src.services.bankruptcy_alert.alerts import recent_alerts, status_summary
    return {
        "summary": status_summary(db),
        "recent_alerts": recent_alerts(db, limit=min(limit, 200)),
    }
