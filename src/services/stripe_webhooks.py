"""
Stripe webhook handlers — M1-B.

All 5 required handlers:
  1. checkout.session.completed   → lock ZIP, increment founding count, GHL stage 5, welcome email, deliver leads
  2. invoice.payment_succeeded    → update billing_date
  3. invoice.payment_failed       → fire GHL payment retry sequence
  4. customer.subscription.updated → sync plan changes
  5. customer.subscription.deleted → 48hr grace, GHL stage 7, forfeit modal flag

Entry point: handle_webhook(raw_body, sig_header) — call this from your web framework route.
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import stripe
from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import (
    FoundingSubscriberCount,
    Subscriber,
    ZipTerritory,
)
from src.services.ghl_webhook import push_subscriber_to_ghl

logger = logging.getLogger(__name__)


def _stripe() -> bool:
    key = settings.stripe_secret_key
    if not key:
        logger.debug("STRIPE_SECRET_KEY not set — webhooks disabled")
        return False
    stripe.api_key = key.get_secret_value()
    return True


def handle_webhook(raw_body: bytes, sig_header: str, db: Session) -> tuple[bool, str]:
    """
    Verify and dispatch a Stripe webhook event.

    Returns (success, message) — your route should return HTTP 200 on success,
    400 on verification failure, and 200 (with logged error) on handler failure
    so Stripe doesn't retry indefinitely.
    """
    if not _stripe():
        return False, "Stripe not configured"

    secret = settings.stripe_webhook_secret
    if not secret:
        return False, "STRIPE_WEBHOOK_SECRET not set"

    try:
        event = stripe.Webhook.construct_event(
            raw_body, sig_header, secret.get_secret_value()
        )
    except stripe.error.SignatureVerificationError as e:
        logger.warning(f"Stripe webhook signature invalid: {e}")
        return False, "Invalid signature"

    event_type = event["type"]
    data = event["data"]["object"]

    logger.info(f"Stripe webhook received: {event_type} id={event['id']}")

    handlers = {
        "checkout.session.completed":    _on_checkout_completed,
        "invoice.payment_succeeded":     _on_payment_succeeded,
        "invoice.payment_failed":        _on_payment_failed,
        "customer.subscription.updated": _on_subscription_updated,
        "customer.subscription.deleted": _on_subscription_deleted,
    }

    handler = handlers.get(event_type)
    if handler is None:
        logger.debug(f"Unhandled event type: {event_type}")
        return True, "Ignored"

    try:
        handler(data, db)
        db.commit()
        return True, "OK"
    except Exception as e:
        db.rollback()
        logger.exception(f"Error handling {event_type}: {e}")
        return True, f"Handler error (logged): {e}"  # 200 so Stripe doesn't retry


# ---------------------------------------------------------------------------
# 1. checkout.session.completed
# ---------------------------------------------------------------------------

def _on_checkout_completed(session: dict, db: Session) -> None:
    """
    - Increment founding_subscriber_counts (atomic — already locked by stripe_service at checkout)
    - Create Subscriber record with rate lock
    - Lock ZIP territories
    - Set GHL stage 5
    - Generate event_feed_uuid
    """
    meta = session.get("metadata", {})
    tier        = meta.get("tier")
    vertical    = meta.get("vertical")
    county_id   = meta.get("county_id")
    zip_codes   = [z.strip() for z in meta.get("zip_codes", "").split(",") if z.strip()]
    is_founding = meta.get("is_founding") == "True"
    founding_price_id = meta.get("founding_price_id") or None

    stripe_customer_id    = session.get("customer")
    stripe_subscription_id = session.get("subscription")
    customer_email        = session.get("customer_details", {}).get("email")
    customer_name         = session.get("customer_details", {}).get("name")

    if not all([tier, vertical, county_id, stripe_customer_id]):
        logger.error(f"checkout.session.completed missing required metadata: {meta}")
        return

    now = datetime.now(timezone.utc)

    # ── Increment founding count ───────────────────────────────────────────
    if is_founding:
        row = db.execute(
            select(FoundingSubscriberCount)
            .where(
                FoundingSubscriberCount.tier == tier,
                FoundingSubscriberCount.vertical == vertical,
                FoundingSubscriberCount.county_id == county_id,
            )
            .with_for_update()
        ).scalar_one_or_none()

        if row:
            row.count += 1
            if row.count == 10:
                logger.info(
                    f"FOUNDING LIMIT REACHED: tier={tier} vertical={vertical} county={county_id} "
                    f"— landing page will now show regular price"
                )

    # ── Create or update Subscriber ────────────────────────────────────────
    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        subscriber = Subscriber(
            stripe_customer_id=stripe_customer_id,
            stripe_subscription_id=stripe_subscription_id,
            tier=tier,
            vertical=vertical,
            county_id=county_id,
            founding_member=is_founding,
            founding_price_id=founding_price_id if is_founding else None,
            rate_locked_at=now if is_founding else None,
            status="active",
            event_feed_uuid=str(uuid.uuid4()),
            email=customer_email,
            name=customer_name,
            ghl_stage=5,
        )
        db.add(subscriber)
    else:
        # Existing customer upgrading — never overwrite founding_price_id
        subscriber.stripe_subscription_id = stripe_subscription_id
        subscriber.tier = tier
        subscriber.vertical = vertical
        subscriber.status = "active"
        subscriber.ghl_stage = 5
        if is_founding and not subscriber.founding_member:
            subscriber.founding_member = True
            subscriber.founding_price_id = founding_price_id
            subscriber.rate_locked_at = now

    db.flush()  # get subscriber.id

    # ── Lock ZIP territories (same transaction) ────────────────────────────
    for zip_code in zip_codes:
        territory = db.execute(
            select(ZipTerritory).where(
                ZipTerritory.zip_code == zip_code,
                ZipTerritory.vertical == vertical,
                ZipTerritory.county_id == county_id,
            ).with_for_update()
        ).scalar_one_or_none()

        if territory is None:
            territory = ZipTerritory(
                zip_code=zip_code,
                vertical=vertical,
                county_id=county_id,
                subscriber_id=subscriber.id,
                status="locked",
                locked_at=now,
            )
            db.add(territory)
        elif territory.status in ("available", "grace"):
            territory.subscriber_id = subscriber.id
            territory.status = "locked"
            territory.locked_at = now
            territory.grace_expires_at = None
        else:
            logger.warning(
                f"ZIP {zip_code}/{vertical}/{county_id} already locked by "
                f"subscriber {territory.subscriber_id} — skipping"
            )

    # ── Push to GHL stage 5 ────────────────────────────────────────────────
    try:
        push_subscriber_to_ghl(subscriber, stage=5)
    except Exception as e:
        logger.error(f"GHL push failed for subscriber {subscriber.id}: {e}")

    logger.info(
        f"checkout.session.completed: subscriber={subscriber.id} "
        f"tier={tier} vertical={vertical} founding={is_founding} "
        f"zips={zip_codes} feed_uuid={subscriber.event_feed_uuid}"
    )


# ---------------------------------------------------------------------------
# 2. invoice.payment_succeeded
# ---------------------------------------------------------------------------

def _on_payment_succeeded(invoice: dict, db: Session) -> None:
    stripe_customer_id = invoice.get("customer")
    if not stripe_customer_id:
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(f"invoice.payment_succeeded: no subscriber for customer {stripe_customer_id}")
        return

    period_end = invoice.get("lines", {}).get("data", [{}])[0].get("period", {}).get("end")
    if period_end:
        subscriber.billing_date = datetime.fromtimestamp(period_end, tz=timezone.utc)

    logger.info(f"invoice.payment_succeeded: subscriber={subscriber.id} billing_date={subscriber.billing_date}")


# ---------------------------------------------------------------------------
# 3. invoice.payment_failed
# ---------------------------------------------------------------------------

def _on_payment_failed(invoice: dict, db: Session) -> None:
    stripe_customer_id = invoice.get("customer")
    if not stripe_customer_id:
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(f"invoice.payment_failed: no subscriber for customer {stripe_customer_id}")
        return

    try:
        push_subscriber_to_ghl(subscriber, stage=None, tags=["payment_failed"])
    except Exception as e:
        logger.error(f"GHL payment-failed tag push error: {e}")

    logger.info(f"invoice.payment_failed: subscriber={subscriber.id} — GHL retry sequence queued")


# ---------------------------------------------------------------------------
# 4. customer.subscription.updated
# ---------------------------------------------------------------------------

def _on_subscription_updated(subscription: dict, db: Session) -> None:
    stripe_customer_id = subscription.get("customer")
    if not stripe_customer_id:
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(f"subscription.updated: no subscriber for customer {stripe_customer_id}")
        return

    stripe_status = subscription.get("status")
    status_map = {
        "active":   "active",
        "past_due": "active",   # still active, payment catching up
        "canceled": "cancelled",
        "unpaid":   "churned",
    }
    new_status = status_map.get(stripe_status, subscriber.status)

    # Never overwrite founding_price_id — only update status
    subscriber.status = new_status
    subscriber.stripe_subscription_id = subscription.get("id", subscriber.stripe_subscription_id)

    logger.info(
        f"subscription.updated: subscriber={subscriber.id} "
        f"stripe_status={stripe_status} → local_status={new_status}"
    )


# ---------------------------------------------------------------------------
# 5. customer.subscription.deleted
# ---------------------------------------------------------------------------

def _on_subscription_deleted(subscription: dict, db: Session) -> None:
    """
    - Set status → grace
    - Set grace_expires_at = now + 48hr
    - Release ZIPs to grace status
    - Push GHL stage 7
    - Log churn type (founding vs regular) for forfeit modal
    """
    stripe_customer_id = subscription.get("customer")
    if not stripe_customer_id:
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(f"subscription.deleted: no subscriber for customer {stripe_customer_id}")
        return

    now = datetime.now(timezone.utc)
    grace_expires = now + timedelta(hours=48)

    subscriber.status = "grace"
    subscriber.grace_expires_at = grace_expires
    subscriber.ghl_stage = 7

    # Set ZIP territories to grace — they remain locked for 48hr
    territories = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.subscriber_id == subscriber.id,
            ZipTerritory.status == "locked",
        )
    ).scalars().all()

    for territory in territories:
        territory.status = "grace"
        territory.grace_expires_at = grace_expires

    churn_tag = "churned_founding" if subscriber.founding_member else "churned_regular"

    try:
        push_subscriber_to_ghl(subscriber, stage=7, tags=[churn_tag])
    except Exception as e:
        logger.error(f"GHL stage 7 push failed for subscriber {subscriber.id}: {e}")

    logger.info(
        f"subscription.deleted: subscriber={subscriber.id} "
        f"founding={subscriber.founding_member} tag={churn_tag} "
        f"grace_expires={grace_expires.isoformat()} zips_in_grace={len(territories)}"
    )
