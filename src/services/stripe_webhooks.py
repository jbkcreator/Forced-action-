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
from sqlalchemy import select, and_, desc, func
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import (
    FoundingSubscriberCount,
    LeadPackPurchase,
    Property,
    DistressScore,
    Subscriber,
    ZipTerritory,
)
from src.services.ghl_webhook import push_subscriber_to_ghl

logger = logging.getLogger(__name__)


def _init_stripe() -> bool:
    """Initialise Stripe API key. Returns False if not configured."""
    key = settings.stripe_secret_key
    if not key:
        logger.debug("STRIPE_SECRET_KEY not set — webhooks disabled")
        return False
    stripe.api_key = key.get_secret_value()
    return True


def handle_webhook(raw_body: bytes, sig_header: str, db: Session) -> tuple[bool, str]:
    """
    Verify and dispatch a Stripe webhook event.

    Returns (success, message).
    - Raises ValueError on signature verification failure (caller should return 400).
    - Returns (True, "Handler error (logged): ...") on handler failure so Stripe
      doesn't retry indefinitely for application-level errors.
    - Raises SQLAlchemyError on DB infrastructure failure (caller should return 503).
    """
    if not _init_stripe():
        return False, "Stripe not configured"

    secret = settings.stripe_webhook_secret
    if not secret:
        raise ValueError("STRIPE_WEBHOOK_SECRET not set")

    try:
        event = stripe.Webhook.construct_event(
            raw_body, sig_header, secret.get_secret_value()
        )
    except stripe.error.SignatureVerificationError as exc:
        logger.warning("Stripe webhook signature invalid: %s", exc)
        raise ValueError("Invalid signature") from exc

    event_type = event["type"]
    data = event["data"]["object"]

    logger.info("Stripe webhook received: %s id=%s", event_type, event["id"])

    handlers = {
        "checkout.session.completed":    _on_checkout_completed,
        "invoice.payment_succeeded":     _on_payment_succeeded,
        "invoice.payment_failed":        _on_payment_failed,
        "customer.subscription.updated": _on_subscription_updated,
        "customer.subscription.deleted": _on_subscription_deleted,
        "payment_intent.succeeded":      _on_lead_pack_payment,
    }

    handler = handlers.get(event_type)
    if handler is None:
        logger.debug("Unhandled Stripe event type: %s", event_type)
        return True, "Ignored"

    try:
        handler(data, db)
        db.commit()
        return True, "OK"
    except (OperationalError, SQLAlchemyError):
        db.rollback()
        # Re-raise DB errors — let the caller return 503 so Stripe retries
        logger.error("Database error handling %s — will retry", event_type, exc_info=True)
        raise
    except Exception as exc:
        db.rollback()
        logger.error("Error handling %s: %s", event_type, exc, exc_info=True)
        # Return 200 so Stripe doesn't retry for application-level errors
        return True, f"Handler error (logged): {exc}"


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

    stripe_customer_id     = session.get("customer")
    stripe_subscription_id = session.get("subscription")
    customer_email         = session.get("customer_details", {}).get("email")
    customer_name          = session.get("customer_details", {}).get("name")

    if not all([tier, vertical, county_id, stripe_customer_id]):
        logger.error(
            "checkout.session.completed missing required metadata — skipping. meta=%s", meta
        )
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
                    "FOUNDING LIMIT REACHED: tier=%s vertical=%s county=%s"
                    " — landing page will now show regular price",
                    tier, vertical, county_id,
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

    db.flush()  # get subscriber.id before ZIP territory inserts

    if not subscriber.id:
        logger.warning(
            "checkout.session.completed: subscriber.id is None after flush for customer %s"
            " — ZIP locking may fail if DB did not assign PK",
            stripe_customer_id,
        )

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
                "ZIP %s/%s/%s already locked by subscriber %s — skipping",
                zip_code, vertical, county_id, territory.subscriber_id,
            )

    # ── Push to GHL stage 5 ────────────────────────────────────────────────
    try:
        push_subscriber_to_ghl(subscriber, stage=5)
    except Exception:
        logger.error(
            "GHL push failed for subscriber %s — continuing without CRM sync",
            subscriber.id,
            exc_info=True,
        )

    logger.info(
        "checkout.session.completed: subscriber=%s tier=%s vertical=%s"
        " founding=%s zips=%s feed_uuid=%s",
        subscriber.id, tier, vertical, is_founding,
        zip_codes, subscriber.event_feed_uuid,
    )


# ---------------------------------------------------------------------------
# 2. invoice.payment_succeeded
# ---------------------------------------------------------------------------

def _on_payment_succeeded(invoice: dict, db: Session) -> None:
    stripe_customer_id = invoice.get("customer")
    if not stripe_customer_id:
        logger.warning("invoice.payment_succeeded: no customer ID in payload")
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(
            "invoice.payment_succeeded: no subscriber for customer %s", stripe_customer_id
        )
        return

    try:
        period_end = invoice.get("lines", {}).get("data", [{}])[0].get("period", {}).get("end")
        if period_end:
            subscriber.billing_date = datetime.fromtimestamp(period_end, tz=timezone.utc)
    except (IndexError, TypeError, KeyError) as exc:
        logger.warning(
            "invoice.payment_succeeded: could not parse period.end for customer %s: %s",
            stripe_customer_id, exc,
        )

    logger.info(
        "invoice.payment_succeeded: subscriber=%s billing_date=%s",
        subscriber.id, subscriber.billing_date,
    )

    # Send payment receipt email
    if subscriber.email:
        from src.services.email import send_email
        from config.settings import get_settings
        settings = get_settings()
        billing_str = (
            subscriber.billing_date.strftime("%B %d, %Y")
            if subscriber.billing_date else "N/A"
        )
        feed_url = (
            f"{settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
            if subscriber.event_feed_uuid else settings.app_base_url
        )
        send_email(
            to=subscriber.email,
            subject=f"Payment confirmed — Forced Action {subscriber.tier.title()}",
            body_text=(
                f"Hi {subscriber.name or 'there'},\n\n"
                f"Your payment has been processed successfully.\n\n"
                f"Plan: {subscriber.tier.title()} / {subscriber.vertical.title()}\n"
                f"Next billing date: {billing_str}\n\n"
                f"Access your lead feed:\n{feed_url}\n\n"
                f"Questions? support@forcedaction.io\n\n"
                f"— Forced Action Team"
            ),
        )


# ---------------------------------------------------------------------------
# 3. invoice.payment_failed
# ---------------------------------------------------------------------------

def _on_payment_failed(invoice: dict, db: Session) -> None:
    stripe_customer_id = invoice.get("customer")
    if not stripe_customer_id:
        logger.warning("invoice.payment_failed: no customer ID in payload")
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(
            "invoice.payment_failed: no subscriber for customer %s", stripe_customer_id
        )
        return

    try:
        push_subscriber_to_ghl(subscriber, stage=None, tags=["payment_failed"])
    except Exception:
        logger.error(
            "GHL payment-failed tag push error for subscriber %s",
            subscriber.id,
            exc_info=True,
        )

    logger.info(
        "invoice.payment_failed: subscriber=%s — GHL retry sequence queued", subscriber.id
    )

    # Send payment failure alert email
    if subscriber.email:
        from src.services.email import send_email
        from config.settings import get_settings
        settings = get_settings()
        portal_url = settings.app_base_url  # placeholder — portal session created on demand
        send_email(
            to=subscriber.email,
            subject="Action required — payment failed for your Forced Action subscription",
            body_text=(
                f"Hi {subscriber.name or 'there'},\n\n"
                f"We were unable to process your payment for your Forced Action "
                f"{subscriber.tier.title()} subscription.\n\n"
                f"To keep your ZIP territories locked and avoid losing your founding rate, "
                f"please update your payment method as soon as possible.\n\n"
                f"Update your card:\n{settings.app_base_url}/dashboard/"
                f"{subscriber.event_feed_uuid or ''}\n\n"
                f"If payment is not resolved within 48 hours, your subscription will enter "
                f"a grace period and your territories may be released.\n\n"
                f"Questions? support@forcedaction.io\n\n"
                f"— Forced Action Team"
            ),
        )


# ---------------------------------------------------------------------------
# 4. customer.subscription.updated
# ---------------------------------------------------------------------------

def _on_subscription_updated(subscription: dict, db: Session) -> None:
    stripe_customer_id = subscription.get("customer")
    if not stripe_customer_id:
        logger.warning("subscription.updated: no customer ID in payload")
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(
            "subscription.updated: no subscriber for customer %s", stripe_customer_id
        )
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
        "subscription.updated: subscriber=%s stripe_status=%s → local_status=%s",
        subscriber.id, stripe_status, new_status,
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
        logger.warning("subscription.deleted: no customer ID in payload")
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(
            "subscription.deleted: no subscriber for customer %s", stripe_customer_id
        )
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
    except Exception:
        logger.error(
            "GHL stage 7 push failed for subscriber %s",
            subscriber.id,
            exc_info=True,
        )

    logger.info(
        "subscription.deleted: subscriber=%s founding=%s tag=%s"
        " grace_expires=%s zips_in_grace=%d",
        subscriber.id, subscriber.founding_member, churn_tag,
        grace_expires.isoformat(), len(territories),
    )


# ---------------------------------------------------------------------------
# 6. payment_intent.succeeded — lead pack purchases
# ---------------------------------------------------------------------------

def _on_lead_pack_payment(payment_intent: dict, db: Session) -> None:
    """
    Handle $99 lead pack purchases.

    Expected metadata on the PaymentIntent:
        product    = "lead_pack"
        feed_uuid  = subscriber's event_feed_uuid
        zip_code   = target ZIP
        vertical   = e.g. "roofing"
        county_id  = e.g. "hillsborough"
    """
    meta = payment_intent.get("metadata", {})
    if meta.get("product") != "lead_pack":
        # Not a lead pack payment — silently ignore
        return

    stripe_payment_intent_id = payment_intent.get("id")
    feed_uuid  = meta.get("feed_uuid")
    zip_code   = meta.get("zip_code")
    vertical   = meta.get("vertical")
    county_id  = meta.get("county_id", "hillsborough")

    if not all([stripe_payment_intent_id, feed_uuid, zip_code, vertical]):
        logger.error(
            "[LeadPack] payment_intent.succeeded missing required metadata: %s", meta
        )
        return

    # Idempotency — skip if already processed
    existing = db.execute(
        select(LeadPackPurchase).where(
            LeadPackPurchase.stripe_payment_intent_id == stripe_payment_intent_id
        )
    ).scalar_one_or_none()
    if existing:
        logger.info(
            "[LeadPack] Already processed payment_intent %s — skipping", stripe_payment_intent_id
        )
        return

    # Find subscriber
    subscriber = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if subscriber is None:
        logger.error("[LeadPack] No subscriber for feed_uuid %s", feed_uuid)
        return

    now = datetime.now(timezone.utc)

    # Create purchase record
    purchase = LeadPackPurchase(
        subscriber_id=subscriber.id,
        zip_code=zip_code,
        vertical=vertical,
        county_id=county_id,
        stripe_payment_intent_id=stripe_payment_intent_id,
        status="pending",
        purchased_at=now,
        exclusive_until=now + timedelta(hours=72),
    )
    db.add(purchase)
    db.flush()  # get purchase.id before exclusivity query

    # Exclude property_ids already under active exclusivity for this ZIP+vertical
    active_exclusive_ids = _get_exclusive_property_ids(db, zip_code, vertical, now, exclude_purchase_id=purchase.id)

    # Select top 5 scored properties not already exclusively held
    try:
        score_col = DistressScore.vertical_scores[vertical].as_float()
    except KeyError:
        logger.error("[LeadPack] Unknown vertical '%s' for purchase %s", vertical, purchase.id)
        purchase.status = "expired"
        return

    lead_filter = [
        Property.zip == zip_code,
        Property.county_id == county_id,
        DistressScore.qualified == True,  # noqa: E712
    ]
    if active_exclusive_ids:
        lead_filter.append(~Property.id.in_(active_exclusive_ids))

    top_leads = db.execute(
        select(Property, DistressScore)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .where(and_(*lead_filter))
        .order_by(desc(score_col))
        .limit(5)
    ).all()

    purchase.lead_ids = [prop.id for prop, _ in top_leads]
    purchase.status = "delivered"
    purchase.delivered_at = now

    logger.info(
        "[LeadPack] Delivered purchase %s — %d leads for %s/%s/%s to subscriber %s",
        purchase.id, len(top_leads), zip_code, vertical, county_id, subscriber.id,
    )

    if subscriber.email:
        _send_lead_pack_email(subscriber, purchase, top_leads)


def _get_exclusive_property_ids(
    db: Session,
    zip_code: str,
    vertical: str,
    now: datetime,
    exclude_purchase_id: Optional[int] = None,
) -> list[int]:
    """Return property_ids currently under active exclusivity for a ZIP+vertical."""
    q = select(LeadPackPurchase).where(
        LeadPackPurchase.zip_code == zip_code,
        LeadPackPurchase.vertical == vertical,
        LeadPackPurchase.exclusive_until > now,
        LeadPackPurchase.lead_ids != None,  # noqa: E711
    )
    if exclude_purchase_id is not None:
        q = q.where(LeadPackPurchase.id != exclude_purchase_id)

    active_purchases = db.execute(q).scalars().all()
    exclusive_ids: list[int] = []
    for p in active_purchases:
        if p.lead_ids:
            exclusive_ids.extend(p.lead_ids)
    return exclusive_ids


def _send_lead_pack_email(
    subscriber: "Subscriber",
    purchase: LeadPackPurchase,
    top_leads: list,
) -> None:
    """Send lead pack delivery email with the 5 selected properties."""
    from src.services.email import send_email
    from config.settings import get_settings
    _settings = get_settings()

    exclusive_until_str = (
        purchase.exclusive_until.strftime("%B %d, %Y at %I:%M %p UTC")
        if purchase.exclusive_until else "72 hours from purchase"
    )

    lead_lines = []
    for i, (prop, score) in enumerate(top_leads, start=1):
        v_score = score.vertical_scores.get(subscriber.vertical) if score.vertical_scores else None
        score_str = f"{v_score:.1f}" if v_score is not None else "N/A"
        lead_lines.append(
            f"{i}. {prop.address}, {prop.city}, FL {prop.zip}\n"
            f"   Score: {score_str}  |  Tier: {score.lead_tier or 'N/A'}"
            f"  |  Type: {', '.join(score.distress_types or []) or 'N/A'}\n"
        )

    dashboard_url = (
        f"{_settings.app_base_url}/api/lead-pack/{purchase.id}"
        if _settings.app_base_url else ""
    )

    send_email(
        to=subscriber.email,
        subject="Your Forced Action Lead Pack — 5 Exclusive Leads",
        body_text=(
            f"Hi {subscriber.name or 'there'},\n\n"
            f"Your lead pack purchase is confirmed. Here are your 5 exclusive leads "
            f"for ZIP {purchase.zip_code} ({purchase.vertical.title()}):\n\n"
            + "\n".join(lead_lines) +
            f"\nThese leads are exclusively yours until {exclusive_until_str}.\n\n"
            f"View full lead details:\n{dashboard_url}\n\n"
            f"Questions? support@forcedaction.io\n\n"
            f"— Forced Action Team"
        ),
    )
