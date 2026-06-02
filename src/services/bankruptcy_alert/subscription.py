"""
Stage 12 — Bankruptcy Filing Alert subscription lifecycle.

Standalone $297/mo product with its own Stripe checkout + webhook endpoint
(dedicated secret BANKRUPTCY_ALERT_STRIPE_WEBHOOK_SECRET), following the
white-label decoupled-webhook precedent. Keeps the new product fully separate
from the complex property-subscriber checkout flow in stripe_webhooks.py.

Lifecycle (webhook events):
  checkout.session.completed     → create row (status active/trialing), access_token
  customer.subscription.updated  → sync status + trial_ends_at
  customer.subscription.deleted  → status=canceled, canceled_at
  invoice.payment_failed         → status=past_due
  invoice.payment_succeeded      → status=active

Idempotency: shared stripe_webhook_events table (unique event_id). Row writes
are idempotent at the column level (stripe_subscription_id unique).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import stripe
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.bankruptcy_alert_config import (
    DEFAULT_CHAPTERS,
    DEFAULT_JURISDICTIONS,
    PRICE_MONTHLY_CENTS,
    PRODUCT_NAME,
    TRIAL_DAYS,
)
from config.settings import get_settings

logger = logging.getLogger(__name__)


# ── Stripe init ─────────────────────────────────────────────────────────────────

def _init_stripe() -> bool:
    settings = get_settings()
    key = settings.active_stripe_secret_key
    if not key:
        logger.debug("[bk-sub] Stripe not configured")
        return False
    stripe.api_key = key.get_secret_value()
    return True


def _attr(obj, key, default=None):
    """Read key from a Stripe object or plain dict (sandbox path)."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        v = obj.get(key, default)
    else:
        v = getattr(obj, key, default)
    return v if v is not None else default


# ── Checkout creation ───────────────────────────────────────────────────────────

def create_checkout(
    *,
    success_url: str,
    cancel_url: str,
    customer_email: Optional[str] = None,
    jurisdictions: Optional[list[str]] = None,
    chapters: Optional[list[str]] = None,
    with_trial: bool = True,
) -> dict:
    """Create a Stripe Checkout Session for the $297/mo bankruptcy alert product.

    Returns {session_id, url}. Raises RuntimeError if not configured, ValueError
    if the price isn't set, stripe.error.StripeError on API failure.
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    settings = get_settings()
    price_id = settings.active_stripe_price("bankruptcy_alerts")
    if not price_id:
        raise ValueError("STRIPE_PRICE_BANKRUPTCY_ALERTS not configured")

    juris = jurisdictions or DEFAULT_JURISDICTIONS
    chs = chapters or DEFAULT_CHAPTERS

    subscription_data: dict = {
        "metadata": {"product": "bankruptcy_alerts"},
    }
    if with_trial and TRIAL_DAYS > 0:
        subscription_data["trial_period_days"] = TRIAL_DAYS

    session = stripe.checkout.Session.create(
        mode="subscription",
        payment_method_types=["card"],
        customer_email=customer_email,
        phone_number_collection={"enabled": True},
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "product": "bankruptcy_alerts",
            "jurisdictions": ",".join(juris),
            "chapters": ",".join(chs),
        },
        subscription_data=subscription_data,
    )
    logger.info("[bk-sub] checkout session created: %s", session.id)
    return {"session_id": session.id, "url": session.url, "price_cents": PRICE_MONTHLY_CENTS}


# ── Row helpers (raw SQL) ────────────────────────────────────────────────────────

def _find_by_customer(db: Session, customer_id: str):
    return db.execute(sa_text("""
        SELECT * FROM bankruptcy_alert_subscriptions
        WHERE stripe_customer_id = :cid LIMIT 1
    """), {"cid": customer_id}).first()


def _find_by_subscription(db: Session, subscription_id: str):
    return db.execute(sa_text("""
        SELECT * FROM bankruptcy_alert_subscriptions
        WHERE stripe_subscription_id = :sid LIMIT 1
    """), {"sid": subscription_id}).first()


def _set_status(db: Session, subscription_id: str, status: str, *, canceled: bool = False) -> bool:
    """Update status by stripe_subscription_id. Returns True if a row was updated."""
    result = db.execute(sa_text(f"""
        UPDATE bankruptcy_alert_subscriptions
        SET status = :status,
            updated_at = NOW()
            {", canceled_at = NOW()" if canceled else ""}
        WHERE stripe_subscription_id = :sid
    """), {"status": status, "sid": subscription_id})
    return result.rowcount > 0


# ── Webhook event handlers ───────────────────────────────────────────────────────

def _on_checkout_completed(session: dict, db: Session) -> None:
    """Create the subscription row on successful checkout."""
    meta = _attr(session, "metadata", {}) or {}
    if _attr(meta, "product") != "bankruptcy_alerts":
        logger.debug("[bk-sub] checkout not a bankruptcy_alerts product — ignoring")
        return

    customer_id = _attr(session, "customer")
    subscription_id = _attr(session, "subscription")
    details = _attr(session, "customer_details", {}) or {}
    email = (_attr(details, "email") or _attr(session, "customer_email") or "").lower().strip()
    phone = _attr(details, "phone")
    name = _attr(details, "name")

    if not email:
        logger.warning("[bk-sub] checkout %s has no email — skipping", _attr(session, "id"))
        return

    # Parse filters from metadata.
    juris_raw = _attr(meta, "jurisdictions", "")
    ch_raw = _attr(meta, "chapters", "")
    jurisdictions = [j.strip() for j in juris_raw.split(",") if j.strip()] or DEFAULT_JURISDICTIONS
    chapters = [c.strip() for c in ch_raw.split(",") if c.strip()] or DEFAULT_CHAPTERS

    # Normalize phone via the project helper (E.164). channel_sms on only if phone valid.
    normalized_phone = None
    if phone:
        try:
            from src.services.phone_utils import normalize as normalize_phone
            normalized_phone = normalize_phone(phone)
        except Exception:
            normalized_phone = None

    # Trial vs active.
    status = "trialing" if TRIAL_DAYS > 0 else "active"
    trial_ends_at = (
        datetime.now(timezone.utc) + timedelta(days=TRIAL_DAYS) if TRIAL_DAYS > 0 else None
    )

    existing = _find_by_customer(db, customer_id) if customer_id else None
    if existing:
        # Returning customer — relink subscription, reactivate.
        db.execute(sa_text("""
            UPDATE bankruptcy_alert_subscriptions
            SET stripe_subscription_id = :sid,
                status = :status,
                trial_ends_at = :trial_ends_at,
                canceled_at = NULL,
                updated_at = NOW()
            WHERE id = :id
        """), {
            "sid": subscription_id, "status": status,
            "trial_ends_at": trial_ends_at, "id": existing.id,
        })
        logger.info("[bk-sub] reactivated subscription id=%s", existing.id)
        return

    access_token = str(uuid.uuid4())
    db.execute(sa_text("""
        INSERT INTO bankruptcy_alert_subscriptions
            (email, phone, name, stripe_customer_id, stripe_subscription_id,
             status, jurisdictions, chapters, channel_email, channel_sms,
             trial_ends_at, access_token, created_at, updated_at)
        VALUES
            (:email, :phone, :name, :cid, :sid,
             :status, CAST(:jurisdictions AS jsonb), CAST(:chapters AS jsonb),
             true, :channel_sms, :trial_ends_at, :access_token, NOW(), NOW())
        ON CONFLICT (stripe_subscription_id) DO NOTHING
    """), {
        "email": email,
        "phone": normalized_phone,
        "name": name,
        "cid": customer_id,
        "sid": subscription_id,
        "status": status,
        "jurisdictions": json.dumps(jurisdictions),
        "chapters": json.dumps(chapters),
        "channel_sms": bool(normalized_phone),
        "trial_ends_at": trial_ends_at,
        "access_token": access_token,
    })
    logger.info("[bk-sub] created subscription email=%s status=%s", email, status)


def _on_subscription_updated(sub: dict, db: Session) -> None:
    subscription_id = _attr(sub, "id")
    stripe_status = _attr(sub, "status", "")
    # Map Stripe status → our enum.
    mapping = {
        "trialing": "trialing",
        "active": "active",
        "past_due": "past_due",
        "unpaid": "past_due",
        "canceled": "canceled",
        "incomplete_expired": "canceled",
    }
    our_status = mapping.get(stripe_status)
    if not our_status:
        return
    _set_status(db, subscription_id, our_status, canceled=(our_status == "canceled"))
    logger.info("[bk-sub] subscription %s → %s", subscription_id, our_status)


def _on_subscription_deleted(sub: dict, db: Session) -> None:
    subscription_id = _attr(sub, "id")
    _set_status(db, subscription_id, "canceled", canceled=True)
    logger.info("[bk-sub] subscription %s canceled", subscription_id)


def _on_payment_failed(invoice: dict, db: Session) -> None:
    subscription_id = _attr(invoice, "subscription")
    if subscription_id:
        _set_status(db, subscription_id, "past_due")
        logger.info("[bk-sub] subscription %s past_due (payment failed)", subscription_id)


def _on_payment_succeeded(invoice: dict, db: Session) -> None:
    subscription_id = _attr(invoice, "subscription")
    if subscription_id:
        # Only flip to active if currently past_due/trialing (don't clobber canceled).
        db.execute(sa_text("""
            UPDATE bankruptcy_alert_subscriptions
            SET status = 'active', updated_at = NOW()
            WHERE stripe_subscription_id = :sid
              AND status IN ('past_due', 'trialing')
        """), {"sid": subscription_id})


_HANDLERS = {
    "checkout.session.completed": _on_checkout_completed,
    "customer.subscription.updated": _on_subscription_updated,
    "customer.subscription.deleted": _on_subscription_deleted,
    "invoice.payment_failed": _on_payment_failed,
    "invoice.payment_succeeded": _on_payment_succeeded,
}


def _owns_subscription(db: Session, subscription_id: Optional[str]) -> bool:
    """True if the given Stripe subscription id belongs to a bankruptcy_alert row.

    Used to claim invoice.* events, which don't carry product metadata directly.
    One indexed lookup on the unique stripe_subscription_id column.
    """
    if not subscription_id:
        return False
    row = db.execute(sa_text("""
        SELECT 1 FROM bankruptcy_alert_subscriptions
        WHERE stripe_subscription_id = :sid LIMIT 1
    """), {"sid": subscription_id}).first()
    return row is not None


def resolve_handler(event_type: str, data: dict, db: Session):
    """Return the bankruptcy handler for this event, or None if the event doesn't
    belong to the bankruptcy-alert product.

    Called by the main stripe_webhooks.handle_webhook BEFORE the property-product
    handlers run, so a bankruptcy event is routed to the right place and never
    touches ZIP/founding/GHL logic. Shares the one webhook endpoint + signing
    secret with the property product.

    Ownership detection:
      checkout.session.completed / customer.subscription.*  → metadata.product
      invoice.payment_*                                     → subscription id lookup
    """
    handler = _HANDLERS.get(event_type)
    if handler is None:
        return None

    if event_type in ("checkout.session.completed",
                      "customer.subscription.updated",
                      "customer.subscription.deleted"):
        meta = _attr(data, "metadata", {}) or {}
        if _attr(meta, "product") == "bankruptcy_alerts":
            return handler
        return None

    if event_type in ("invoice.payment_failed", "invoice.payment_succeeded"):
        if _owns_subscription(db, _attr(data, "subscription")):
            return handler
        return None

    return None
