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
    StripeWebhookEvent,
    ZipTerritory,
)
from src.services.ghl_webhook import push_subscriber_to_ghl

logger = logging.getLogger(__name__)


def _init_stripe() -> bool:
    """Initialise Stripe API key. Returns False if not configured."""
    key = settings.active_stripe_secret_key
    if not key:
        logger.debug("Stripe secret key not set — webhooks disabled")
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

    secret = settings.active_stripe_webhook_secret
    if not secret:
        raise ValueError("Stripe webhook secret not set")

    try:
        event = stripe.Webhook.construct_event(
            raw_body, sig_header, secret.get_secret_value()
        )
    except stripe.error.SignatureVerificationError as exc:
        logger.warning("Stripe webhook signature invalid: %s", exc)
        raise ValueError("Invalid signature") from exc

    event_type = event["type"]
    event_id   = event["id"]
    data = event["data"]["object"]

    logger.info("Stripe webhook received: %s id=%s", event_type, event_id)

    # ── Idempotency guard ─────────────────────────────────────────────────────
    # Insert the event_id before processing. If the unique constraint fires,
    # this event was already handled — return immediately without side effects.
    try:
        db.add(StripeWebhookEvent(event_id=event_id, event_type=event_type))
        db.flush()
    except Exception:
        db.rollback()
        logger.info("Stripe event %s already processed — skipping", event_id)
        return True, "Already processed"

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
        push_subscriber_to_ghl(
            subscriber,
            stage=5,
            zip_codes=list(zip_codes),
            is_founding=is_founding,
        )
    except Exception:
        logger.error(
            "GHL push failed for subscriber %s — continuing without CRM sync",
            subscriber.id,
            exc_info=True,
        )

    # ── Welcome email with Event Feed UUID ────────────────────────────────
    if subscriber.email:
        _send_welcome_email(subscriber)

    # ── Immediate first-leads delivery (eliminates 13-hour silence) ───────
    if subscriber.email and zip_codes:
        try:
            _send_first_leads_email(subscriber, zip_codes, db)
        except Exception:
            logger.error(
                "First leads email failed for subscriber %s — non-critical",
                subscriber.id, exc_info=True,
            )

    logger.info(
        "checkout.session.completed: subscriber=%s tier=%s vertical=%s"
        " founding=%s zips=%s feed_uuid=%s",
        subscriber.id, tier, vertical, is_founding,
        zip_codes, subscriber.event_feed_uuid,
    )


# ---------------------------------------------------------------------------
# Welcome email helper
# ---------------------------------------------------------------------------

def _send_welcome_email(subscriber: "Subscriber") -> None:
    """Send the post-checkout welcome email containing the Event Feed UUID link."""
    from src.services.email import send_email
    from config.settings import get_settings

    _settings = get_settings()

    name = subscriber.name or "there"
    tier = (subscriber.tier or "starter").title()
    vertical = (subscriber.vertical or "").replace("_", " ").title()
    founding = subscriber.founding_member

    feed_url = (
        f"{_settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
        if subscriber.event_feed_uuid
        else _settings.app_base_url
    )

    subject = (
        "You're in — your Forced Action feed is ready"
        if not founding
        else "Founding member confirmed — your rate is locked forever"
    )

    # ── Plain-text body ────────────────────────────────────────────────────
    founding_line = (
        "\nAs a founding member your rate is locked for as long as you stay subscribed.\n"
        if founding else ""
    )
    body_text = (
        f"Hi {name},\n\n"
        f"Welcome to Forced Action.\n"
        f"{founding_line}\n"
        f"Plan: {tier} — {vertical}\n\n"
        f"Your private Event Feed is live. Bookmark this link — it's yours alone:\n"
        f"{feed_url}\n\n"
        f"New distressed property leads matching your territory and vertical will appear "
        f"here automatically as our scrapers run each day.\n\n"
        f"Questions? Reply to this email or reach us at support@forcedaction.io\n\n"
        f"— Forced Action Team"
    )

    # ── HTML body ──────────────────────────────────────────────────────────
    founding_badge = (
        '<p style="margin:0 0 16px;padding:10px 16px;background:#451a03;'
        'border:1px solid #92400e;border-radius:8px;color:#fbbf24;font-size:14px;">'
        "⭐ Founding Member — your rate is locked for life."
        "</p>"
        if founding else ""
    )
    body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">

        <!-- Header -->
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:26px;font-weight:800;color:#ffffff;">
              You&rsquo;re in, {name}.
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Your Event Feed is live and your territory is reserved.
            </p>

            {founding_badge}

            <!-- Plan pill -->
            <p style="margin:0 0 24px;">
              <span style="display:inline-block;background:rgba(251,191,36,0.1);border:1px solid rgba(251,191,36,0.3);
                           color:#fbbf24;font-size:13px;font-weight:700;padding:5px 14px;border-radius:999px;">
                {tier} &middot; {vertical}
              </span>
            </p>

            <!-- CTA -->
            <p style="margin:0 0 12px;font-size:14px;color:#94a3b8;">
              Your private feed link — bookmark it:
            </p>
            <table cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{feed_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    Open My Event Feed &rarr;
                  </a>
                </td>
              </tr>
            </table>

            <!-- What to expect -->
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);
                          border-radius:12px;padding:20px 24px;margin-bottom:24px;">
              <tr>
                <td>
                  <p style="margin:0 0 12px;font-size:14px;font-weight:700;color:#ffffff;">What happens next</p>
                  <p style="margin:0 0 8px;font-size:13px;color:#94a3b8;">
                    ✓ &nbsp;Scrapers run daily — new distressed property leads appear automatically.
                  </p>
                  <p style="margin:0 0 8px;font-size:13px;color:#94a3b8;">
                    ✓ &nbsp;Leads are scored across your selected vertical and ranked by urgency.
                  </p>
                  <p style="margin:0;font-size:13px;color:#94a3b8;">
                    ✓ &nbsp;Your territory ZIPs are exclusively yours — no other subscriber sees the same leads.
                  </p>
                </td>
              </tr>
            </table>

            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? Reply to this email or reach us at
              <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">
                support@forcedaction.io
              </a>
            </p>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);
                     font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence<br/>
            <a href="{_settings.app_base_url}" style="color:#475569;">forcedaction.io</a>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    send_email(
        to=subscriber.email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
    )

    logger.info("Welcome email sent → %s (subscriber=%s)", subscriber.email, subscriber.id)


def _send_first_leads_email(subscriber, zip_codes: list, db) -> None:
    """
    Immediately after checkout, deliver the top 10 existing leads in the
    subscriber's territory so there's no silence between payment and first value.
    Runs in the same webhook request — failure is caught and logged, never fatal.
    """
    from src.tasks.subscriber_email import query_top_leads, send_subscriber_lead_email

    leads = query_top_leads(db, subscriber, zip_codes, limit=10)
    if not leads:
        logger.info(
            "No existing leads to deliver immediately for subscriber %s (zips=%s) — skipping first-leads email",
            subscriber.id, zip_codes,
        )
        return

    send_subscriber_lead_email(
        subscriber,
        leads,
        subject_prefix="Here are your first leads",
        zip_codes=zip_codes,
    )


# ---------------------------------------------------------------------------
# 2. invoice.payment_succeeded
# ---------------------------------------------------------------------------

def _on_payment_succeeded(invoice: dict, db: Session) -> None:
    stripe_customer_id = invoice.get("customer")
    if not stripe_customer_id:
        logger.warning("invoice.payment_succeeded: no customer ID in payload")
        return

    # Skip payment receipt on initial checkout — checkout.session.completed already
    # sent the welcome email and first-leads email for that payment.
    billing_reason = invoice.get("billing_reason")
    if billing_reason == "subscription_create":
        logger.info(
            "invoice.payment_succeeded: skipping receipt email for subscription_create"
            " (handled by checkout.session.completed) customer=%s",
            stripe_customer_id,
        )
        # Still update billing_date below — just no email.

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

    # Send payment receipt email only for renewals, not initial signup
    if subscriber.email and billing_reason != "subscription_create":
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
        payment_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">

        <!-- Header -->
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>

        <!-- Success banner -->
        <tr>
          <td style="padding:0;">
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:rgba(34,197,94,0.12);border-bottom:1px solid rgba(34,197,94,0.25);">
              <tr>
                <td style="padding:14px 40px;font-size:14px;font-weight:700;color:#4ade80;text-align:center;">
                  &#10003; &nbsp;Payment confirmed
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:24px;font-weight:800;color:#ffffff;">
              Thanks, {subscriber.name or 'there'}.
            </h1>
            <p style="margin:0 0 28px;color:#94a3b8;font-size:15px;">
              Your payment has been processed successfully. Here are the details:
            </p>

            <!-- Payment details table -->
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);
                          border-radius:12px;margin-bottom:28px;">
              <tr>
                <td style="padding:16px 24px;border-bottom:1px solid rgba(255,255,255,0.08);
                           font-size:13px;color:#94a3b8;width:40%;">Plan</td>
                <td style="padding:16px 24px;border-bottom:1px solid rgba(255,255,255,0.08);
                           font-size:14px;font-weight:600;color:#ffffff;">
                  {subscriber.tier.title()} &middot; {subscriber.vertical.title()}
                </td>
              </tr>
              <tr>
                <td style="padding:16px 24px;border-bottom:1px solid rgba(255,255,255,0.08);
                           font-size:13px;color:#94a3b8;">Amount</td>
                <td style="padding:16px 24px;border-bottom:1px solid rgba(255,255,255,0.08);
                           font-size:14px;font-weight:600;color:#ffffff;">
                  See invoice from Stripe
                </td>
              </tr>
              <tr>
                <td style="padding:16px 24px;font-size:13px;color:#94a3b8;">Next billing date</td>
                <td style="padding:16px 24px;font-size:14px;font-weight:600;color:#ffffff;">
                  {billing_str}
                </td>
              </tr>
            </table>

            <!-- CTA -->
            <table cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{feed_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    Access Your Lead Feed &rarr;
                  </a>
                </td>
              </tr>
            </table>

            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? Reply to this email or reach us at
              <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">
                support@forcedaction.io
              </a>
            </p>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);
                     font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence<br/>
            <a href="{settings.app_base_url}" style="color:#475569;">forcedaction.io</a>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

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
            body_html=payment_html,
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
        name = subscriber.name or "there"
        tier = (subscriber.tier or "").title()
        feed_url = (
            f"{settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
            if subscriber.event_feed_uuid else settings.app_base_url
        )
        founding_line = (
            "\nThis also puts your founding rate lock at risk — it cannot be reclaimed if your subscription lapses.\n"
            if subscriber.founding_member else ""
        )
        body_text = (
            f"Hi {name},\n\n"
            f"We were unable to process your payment for your Forced Action {tier} subscription.\n\n"
            f"To keep your ZIP territories locked and avoid losing your founding rate, "
            f"please update your payment method as soon as possible.\n"
            f"{founding_line}\n"
            f"Update your card:\n{feed_url}\n\n"
            f"If payment is not resolved within 48 hours, your subscription will enter "
            f"a grace period and your territories may be released.\n\n"
            f"Questions? support@forcedaction.io\n\n"
            f"— Forced Action Team"
        )
        body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px 40px;">
            <!-- Alert banner -->
            <p style="margin:0 0 24px;padding:12px 16px;background:#450a0a;border:1px solid #7f1d1d;
                      border-radius:8px;color:#fca5a5;font-size:14px;font-weight:600;">
              ⚠️ &nbsp;Action required — payment failed
            </p>
            <h1 style="margin:0 0 8px;font-size:24px;font-weight:800;color:#ffffff;">
              We couldn't process your payment
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Hi {name}, your <strong style="color:#ffffff;">{tier}</strong> subscription payment failed.
              Please update your payment method to keep your territories locked.
            </p>
            {"<p style='margin:0 0 24px;padding:10px 16px;background:#451a03;border:1px solid #92400e;border-radius:8px;color:#fbbf24;font-size:14px;'>⭐ Your founding rate lock is at risk — it cannot be reclaimed if your subscription lapses.</p>" if subscriber.founding_member else ""}
            <p style="margin:0 0 12px;font-size:14px;color:#94a3b8;">
              You have <strong style="color:#ffffff;">48 hours</strong> before your ZIP territories enter grace period.
            </p>
            <table cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#ef4444;border-radius:8px;">
                  <a href="{feed_url}"
                     style="display:inline-block;padding:14px 28px;color:#ffffff;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    Update Payment Method &rarr;
                  </a>
                </td>
              </tr>
            </table>
            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">support@forcedaction.io</a>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
        send_email(
            to=subscriber.email,
            subject="Action required — payment failed for your Forced Action subscription",
            body_text=body_text,
            body_html=body_html,
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
    cancel_at_period_end = subscription.get("cancel_at_period_end", False)
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
        "subscription.updated: subscriber=%s stripe_status=%s → local_status=%s cancel_at_period_end=%s",
        subscriber.id, stripe_status, new_status, cancel_at_period_end,
    )

    # Send cancellation email when cancel_at is set (scheduled cancellation)
    cancel_at = subscription.get("cancel_at")
    if cancel_at and subscriber.email:
        from src.services.email import send_email
        from config.settings import get_settings
        import datetime as _dt
        _settings = get_settings()
        cancel_at = subscription.get("cancel_at")
        cancel_str = (
            _dt.datetime.fromtimestamp(cancel_at, tz=_dt.timezone.utc).strftime("%B %d, %Y")
            if cancel_at else "at the end of your billing period"
        )
        feed_url = (
            f"{_settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
            if subscriber.event_feed_uuid else _settings.app_base_url
        )
        founding_line = (
            "\nNote: your founding rate cannot be reclaimed once your subscription ends.\n"
            if subscriber.founding_member else ""
        )
        name = subscriber.name or "there"
        tier = (subscriber.tier or "starter").title()
        founding_html = (
            '<p style="margin:0 0 16px;padding:10px 16px;background:#451a03;'
            'border:1px solid #92400e;border-radius:8px;color:#fbbf24;font-size:14px;">'
            "⭐ Founding Member — your locked rate will be permanently lost if you don't reactivate."
            "</p>"
            if subscriber.founding_member else ""
        )
        body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:24px;font-weight:800;color:#ffffff;">
              Cancellation scheduled, {name}.
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Your {tier} subscription will end on <strong style="color:#ffffff;">{cancel_str}</strong>.
              You keep full access until then.
            </p>
            {founding_html}
            <p style="margin:0 0 20px;font-size:14px;color:#94a3b8;">
              Changed your mind? Reactivate before {cancel_str} to keep your territory and leads:
            </p>
            <table cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{feed_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    Reactivate My Subscription &rarr;
                  </a>
                </td>
              </tr>
            </table>
            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">support@forcedaction.io</a>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);
                     font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
        send_email(
            to=subscriber.email,
            subject="Your Forced Action subscription has been cancelled",
            body_text=(
                f"Hi {name},\n\n"
                f"Your Forced Action {tier} subscription has been cancelled "
                f"and will end on {cancel_str}.\n\n"
                f"You'll keep full access to your ZIP territories and lead feed until then.\n"
                f"{founding_line}\n"
                f"Changed your mind? Reactivate before {cancel_str}:\n{feed_url}\n\n"
                f"Questions? support@forcedaction.io\n\n"
                f"— Forced Action Team"
            ),
            body_html=body_html,
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

    # ── Cancellation email ─────────────────────────────────────────────────
    if subscriber.email:
        from src.services.email import send_email
        from config.settings import get_settings
        _settings = get_settings()
        grace_str = grace_expires.strftime("%B %d, %Y at %I:%M %p UTC")
        feed_url = (
            f"{_settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
            if subscriber.event_feed_uuid else _settings.app_base_url
        )
        name = subscriber.name or "there"
        tier = (subscriber.tier or "starter").title()
        founding_line = (
            "\nNote: your founding rate cannot be reclaimed once the grace period ends.\n"
            if subscriber.founding_member else ""
        )
        founding_html = (
            '<p style="margin:0 0 16px;padding:10px 16px;background:#451a03;'
            'border:1px solid #92400e;border-radius:8px;color:#fbbf24;font-size:14px;">'
            "⭐ Founding Member — your locked rate will be permanently lost if you don't reactivate before the grace period ends."
            "</p>"
            if subscriber.founding_member else ""
        )
        body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:24px;font-weight:800;color:#ffffff;">
              Subscription cancelled, {name}.
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Your {tier} subscription has been cancelled. Your 48-hour grace period runs until
              <strong style="color:#ffffff;">{grace_str}</strong> — you keep full access until then.
            </p>
            {founding_html}
            <p style="margin:0 0 20px;font-size:14px;color:#94a3b8;">
              Changed your mind? Reactivate before your grace period expires:
            </p>
            <table cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{feed_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    Reactivate My Subscription &rarr;
                  </a>
                </td>
              </tr>
            </table>
            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">support@forcedaction.io</a>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);
                     font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
        send_email(
            to=subscriber.email,
            subject="Your Forced Action subscription has been cancelled",
            body_text=(
                f"Hi {name},\n\n"
                f"Your Forced Action {tier} subscription has been cancelled.\n\n"
                f"Your ZIP territories and lead access will remain active until your 48-hour "
                f"grace period expires on:\n{grace_str}\n"
                f"{founding_line}\n"
                f"Changed your mind? Reactivate before the grace period ends:\n{feed_url}\n\n"
                f"Questions? support@forcedaction.io\n\n"
                f"— Forced Action Team"
            ),
            body_html=body_html,
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

    # Build HTML lead cards
    lead_cards_html = ""
    for i, (prop, score) in enumerate(top_leads, start=1):
        v_score = score.vertical_scores.get(subscriber.vertical) if score.vertical_scores else None
        score_str = f"{v_score:.1f}" if v_score is not None else "N/A"
        tier = score.lead_tier or "N/A"
        distress = ", ".join(score.distress_types or []) or "N/A"
        border_color = "#c084fc" if tier in ("Ultra Platinum", "Platinum") else "#fbbf24"
        lead_cards_html += f"""
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);
                          border-left:4px solid {border_color};border-radius:10px;
                          padding:16px 20px;margin-bottom:12px;">
              <tr><td>
                <p style="margin:0 0 4px;font-size:14px;font-weight:700;color:#ffffff;">
                  {i}. {prop.address}, {prop.city}, FL {prop.zip}
                </p>
                <p style="margin:0;font-size:13px;color:#94a3b8;">
                  Score: <span style="color:#fbbf24;font-weight:600;">{score_str}</span>
                  &nbsp;&middot;&nbsp; Tier: <span style="color:{border_color};font-weight:600;">{tier}</span>
                  &nbsp;&middot;&nbsp; Type: {distress}
                </p>
              </td></tr>
            </table>"""

    body_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">

        <!-- Header -->
        <tr>
          <td style="padding:32px 40px 24px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
            </p>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:26px;font-weight:800;color:#ffffff;">
              Your Lead Pack is ready.
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              5 exclusive leads for ZIP <strong style="color:#ffffff;">{purchase.zip_code}</strong>
              &nbsp;&middot;&nbsp; {subscriber.vertical.title() if subscriber.vertical else 'General'}
            </p>

            <!-- Exclusivity badge -->
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:rgba(251,191,36,0.08);border:1px solid rgba(251,191,36,0.25);
                          border-radius:10px;padding:14px 20px;margin-bottom:24px;">
              <tr><td>
                <p style="margin:0;font-size:13px;font-weight:700;color:#fbbf24;">
                  &#128274; Exclusive Access
                </p>
                <p style="margin:4px 0 0;font-size:13px;color:#94a3b8;">
                  These leads are exclusively yours until <strong style="color:#ffffff;">{exclusive_until_str}</strong>.
                  No other subscriber will receive them.
                </p>
              </td></tr>
            </table>

            <!-- Lead Cards -->
            {lead_cards_html}

            <!-- CTA -->
            <table cellpadding="0" cellspacing="0" style="margin:28px 0 28px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{dashboard_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;
                            font-weight:700;text-decoration:none;">
                    View Full Lead Details &rarr;
                  </a>
                </td>
              </tr>
            </table>

            <p style="margin:0;font-size:13px;color:#64748b;">
              Questions? Reply to this email or reach us at
              <a href="mailto:support@forcedaction.io" style="color:#fbbf24;text-decoration:none;">
                support@forcedaction.io
              </a>
            </p>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);
                     font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Hillsborough County Property Intelligence<br/>
            <a href="{_settings.app_base_url}" style="color:#475569;">forcedaction.io</a>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

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
        body_html=body_html,
    )
