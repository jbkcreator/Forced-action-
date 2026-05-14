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
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import stripe
from sqlalchemy import select, and_, desc, func
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
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


def _attr(obj, key: str, default=None):
    """Read `key` from a Stripe SDK object or a plain dict.

    The Stripe Python SDK (>=6) returns StripeObject instances that are NOT
    dict subclasses — calling .get() on them raises AttributeError. This
    helper resolves the key via attribute access for StripeObjects and via
    .get() for plain dicts (sandbox simulate-stripe-event path).
    """
    if obj is None:
        return default
    if isinstance(obj, dict):
        v = obj.get(key, default)
    else:
        try:
            v = getattr(obj, key)
        except AttributeError:
            return default
    return v if v is not None else default


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

    # ── Stale event guard (checkout only) ────────────────────────────────────
    # Stripe can replay checkout.session.completed hours or days later on retry.
    # A stale replay re-runs the full subscriber creation path and is the primary
    # cause of duplicate subscriber rows.  Return 200 so Stripe stops retrying,
    # but skip all processing.  Invoice events are legitimately delayed (dunning),
    # so we only guard checkout here.
    if event_type == "checkout.session.completed":
        age_seconds = time.time() - event.get("created", 0)
        if age_seconds > 86400:
            logger.warning(
                "Stale checkout.session.completed event %s (age=%ds) — skipping to prevent duplicate subscriber",
                event_id, int(age_seconds),
            )
            return True, "Stale event skipped"

    # ── Idempotency guard (fa016 followup #21) ───────────────────────────────
    # Look up the dedupe row first; if present, this event has already been
    # handled and we return without re-running.
    #
    # Crucially the dedupe row is INSERTED AFTER the handler succeeds, not
    # before. The old behaviour inserted at the start: if the handler then
    # crashed, the dedupe row stayed planted (committed in a separate
    # session flush) and Stripe's retries were silently swallowed. With
    # post-commit insertion a crashed handler leaves no trace and the retry
    # actually re-runs.
    #
    # Multi-listener race note: when two backends share a DB and race the
    # same event, both can pass this check, both run the handler, one wins
    # the dedupe insert (unique constraint), the other catches the
    # IntegrityError and logs it. Handler writes are idempotent at the row
    # level (PremiumPurchase.stripe_payment_intent_id unique, WalletBalance
    # uniq on subscriber_id, WalletPushOffer.stripe_subscription_id unique,
    # etc.) so duplicate work is harmless.
    from src.services.webhook_log import log_webhook_event

    existing_dedupe = db.execute(
        select(StripeWebhookEvent).where(StripeWebhookEvent.event_id == event_id)
    ).scalar_one_or_none()
    if existing_dedupe is not None:
        logger.info("Stripe event %s already processed — skipping", event_id)
        log_webhook_event(
            source="stripe", event_type=event_type, source_event_id=event_id,
            status="duplicate", payload=event, payload_kind="stripe",
        )
        return True, "Already processed"

    # Audit-log the received event before dispatching — captures it even if
    # the handler below raises mid-flight.
    log_webhook_event(
        source="stripe", event_type=event_type, source_event_id=event_id,
        status="received", payload=event, payload_kind="stripe", db=db,
    )

    handlers = {
        "checkout.session.completed":    _on_checkout_completed,
        "invoice.payment_succeeded":     _on_payment_succeeded,
        "invoice.payment_failed":        _on_payment_failed,
        "customer.subscription.updated": _on_subscription_updated,
        "customer.subscription.deleted": _on_subscription_deleted,
        "payment_intent.succeeded":      _on_payment_intent_succeeded,
        "payment_method.attached":       _on_payment_method_attached,
        "charge.refunded":                _on_charge_refunded,
        "charge.dispute.created":         _on_dispute_created,
        "charge.dispute.funds_withdrawn": _on_dispute_funds_withdrawn,
    }

    handler = handlers.get(event_type)
    if handler is None:
        logger.debug("Unhandled Stripe event type: %s", event_type)
        return True, "Ignored"

    try:
        handler(data, db)
        # Plant the dedupe row in the SAME transaction as the handler writes,
        # so they commit together. If another listener already committed for
        # this event_id, the unique constraint fires and we treat it as a
        # successful idempotent retry (the handler's writes were also
        # idempotent at the row level).
        try:
            db.add(StripeWebhookEvent(event_id=event_id, event_type=event_type))
            db.commit()
        except IntegrityError:
            db.rollback()
            logger.info(
                "Stripe event %s dedupe insert lost the race — handler still ran successfully",
                event_id,
            )
            return True, "OK (lost dedupe race)"
        return True, "OK"
    except (OperationalError, SQLAlchemyError):
        db.rollback()
        logger.error("Database error handling %s — will retry", event_type, exc_info=True)
        raise
    except Exception as exc:
        db.rollback()
        logger.error("Error handling %s: %s", event_type, exc, exc_info=True)
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
    # Normalize email immediately — prevents case-variant duplicates (Fix 1)
    _raw_email             = session.get("customer_details", {}).get("email") or ""
    customer_email         = _raw_email.lower().strip() or None
    customer_name          = session.get("customer_details", {}).get("name")
    # Phone is collected by Stripe Checkout when phone_number_collection.enabled=True
    # (see stripe_service.create_subscription_checkout). E.164 format already.
    customer_phone         = (session.get("customer_details", {}) or {}).get("phone") or None

    if not all([tier, vertical, county_id, stripe_customer_id]):
        logger.error(
            "checkout.session.completed missing required metadata — skipping. meta=%s", meta
        )
        return

    # Stripe fires checkout.session.completed even when initial payment fails
    # (subscription lands in 'incomplete' state, payment_status = 'unpaid').
    # On failure: create a churned record so we can follow up, but skip founding
    # count increment and ZIP locking (they haven't paid).
    #
    # payment_status is the primary signal. For embedded checkout (ui_mode='embedded')
    # it can arrive as None even on failure, so we also inspect the subscription
    # status directly when the field is missing or ambiguous.
    payment_status = session.get("payment_status")
    logger.info(
        "checkout.session.completed: customer=%s payment_status=%r subscription=%s",
        stripe_customer_id, payment_status, stripe_subscription_id,
    )

    payment_failed = False
    if payment_status == "unpaid":
        payment_failed = True
    elif payment_status != "paid" and stripe_subscription_id:
        # payment_status absent or unexpected — ask Stripe directly
        try:
            sub = stripe.Subscription.retrieve(stripe_subscription_id)
            if sub.get("status") in ("incomplete", "incomplete_expired", "past_due", "unpaid"):
                payment_failed = True
                logger.info(
                    "checkout.session.completed: subscription %s status=%s → treating as payment_failed",
                    stripe_subscription_id, sub.get("status"),
                )
        except Exception:
            logger.warning(
                "checkout.session.completed: could not retrieve subscription %s — assuming paid",
                stripe_subscription_id, exc_info=True,
            )

    now = datetime.now(timezone.utc)

    if payment_failed:
        logger.warning(
            "checkout.session.completed payment_status=%s — creating churned subscriber. customer=%s",
            payment_status, stripe_customer_id,
        )
        churned = Subscriber(
            stripe_customer_id=stripe_customer_id,
            stripe_subscription_id=stripe_subscription_id,
            tier=tier,
            vertical=vertical,
            county_id=county_id,
            founding_member=False,
            status="churned",
            event_feed_uuid=str(uuid.uuid4()),
            email=customer_email,
            name=customer_name,
            ghl_stage=7,
        )
        db.add(churned)
        db.flush()
        try:
            push_subscriber_to_ghl(churned, stage=7, tags=["checkout_payment_failed"], db=db)
        except Exception:
            logger.error(
                "GHL stage 7 push failed for churned checkout subscriber %s",
                churned.id,
                exc_info=True,
            )
        logger.info(
            "checkout.session.completed (payment_failed): churned subscriber=%s customer=%s",
            churned.id, stripe_customer_id,
        )
        return

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

        if row is None:
            row = FoundingSubscriberCount(
                tier=tier,
                vertical=vertical,
                county_id=county_id,
                count=0,
            )
            db.add(row)
            db.flush()
        row.count += 1
        if row.count == settings.founding_spot_limit:
            logger.info(
                "FOUNDING LIMIT REACHED: tier=%s vertical=%s county=%s"
                " — landing page will now show regular price",
                tier, vertical, county_id,
            )

    # ── Create or update Subscriber (two-stage lookup) ────────────────────
    # Stage 1: look up by stripe_customer_id (returning customer, plan change)
    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    # Stage 2: if not found by Stripe ID, look for an existing active/grace row
    # with the same normalised email + vertical + county.  This catches the case
    # where the same person checks out again (stale replay, duplicate browser tab,
    # re-subscribe after cancel before the DB constraint was in place).
    is_new_subscriber = False
    if subscriber is None and customer_email:
        subscriber = db.execute(
            select(Subscriber).where(
                Subscriber.email == customer_email,
                Subscriber.vertical == vertical,
                Subscriber.county_id == county_id,
                Subscriber.status.in_(["active", "grace"]),
            )
        ).scalar_one_or_none()
        if subscriber is not None:
            logger.warning(
                "checkout.session.completed: duplicate detected — email=%s vertical=%s county=%s"
                " already has subscriber id=%s — merging onto existing row, skipping welcome email",
                customer_email, vertical, county_id, subscriber.id,
            )

    if subscriber is None:
        # Genuinely new subscriber
        is_new_subscriber = True
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
            phone=customer_phone,
            ghl_stage=5,
        )
        db.add(subscriber)
    else:
        # Existing row (by Stripe ID or by email match) — update billing fields only.
        # Never overwrite founding_price_id, event_feed_uuid, or rate_locked_at.
        subscriber.stripe_customer_id     = stripe_customer_id
        subscriber.stripe_subscription_id = stripe_subscription_id
        subscriber.tier    = tier
        subscriber.status  = "active"
        subscriber.ghl_stage = 5
        # Backfill phone if Stripe collected one and we don't have it yet.
        if customer_phone and not subscriber.phone:
            subscriber.phone = customer_phone
        if is_founding and not subscriber.founding_member:
            subscriber.founding_member    = True
            subscriber.founding_price_id  = founding_price_id
            subscriber.rate_locked_at     = now

    db.flush()  # get subscriber.id before ZIP territory inserts

    # ── Race-free saved-card flag (fa016 followup #20) ───────────────────────
    # We're in the same transaction that just committed the Subscriber row, so
    # there's no race against payment_method.attached / payment_intent.succeeded
    # webhooks running in parallel. Read default_payment_method from the
    # session's payment_intent (synchronous, in-payload) — fall back to a
    # Customer.retrieve only if the inline path is missing.
    if not subscriber.has_saved_card:
        pm_id = None
        # 1) Try the session's payment_intent block (present for subscription
        #    + one-time modes when expand was set; sometimes a bare id).
        pi_block = session.get("payment_intent") or {}
        if isinstance(pi_block, dict):
            pm_id = pi_block.get("payment_method")
        # 2) Try the session's subscription_details (Basil API shape).
        if not pm_id:
            sd = (session.get("parent") or {}).get("subscription_details") or {}
            if isinstance(sd, dict):
                pm_id = sd.get("default_payment_method")
        # 3) Fall back to retrieving the customer once.
        if not pm_id and stripe_customer_id:
            try:
                cust = stripe.Customer.retrieve(stripe_customer_id)
                pm_id = (cust.get("invoice_settings") or {}).get("default_payment_method")
            except Exception as exc:
                logger.warning(
                    "checkout: customer retrieve failed for %s: %s",
                    stripe_customer_id, exc,
                )
        # 4) Last resort: list attached PMs and take the most recent card.
        if not pm_id and stripe_customer_id:
            try:
                pms = stripe.PaymentMethod.list(customer=stripe_customer_id, type="card", limit=1)
                if pms.get("data"):
                    pm_id = pms["data"][0]["id"]
            except Exception as exc:
                logger.warning(
                    "checkout: PM list failed for customer=%s: %s",
                    stripe_customer_id, exc,
                )

        if pm_id:
            subscriber.has_saved_card = True
            subscriber.stripe_payment_method_id = pm_id
            db.flush()
            logger.info(
                "checkout: saved-card flag set inline — subscriber=%s pm=%s",
                subscriber.id, pm_id,
            )

            # If they already had paid activity, the accelerated wallet push
            # detector becomes eligible the moment has_saved_card flips. Fire
            # it now — race-free since we're in the same transaction.
            try:
                from src.services import wallet_engine
                eligible = wallet_engine.accelerated_push_eligible(subscriber.id, db)
                if eligible:
                    from src.agents.supervisor import dispatch_event
                    dispatch_event({
                        "event_type": "accelerated_wallet_push_eligible",
                        "subscriber_id": subscriber.id,
                        "payload": eligible,
                    })
            except Exception as exc:
                logger.warning(
                    "accelerated_wallet_push from _on_checkout_completed failed sub=%s: %s",
                    subscriber.id, exc,
                )

    # ── TCPA opt-in record ─────────────────────────────────────────────────
    # Stripe Checkout's phone collection field is presented next to the
    # subscription-purchase consent on the same form. Treat completion as
    # an opt-in to operational SMS (BALANCE / WALLET / etc) and the
    # accelerated-wallet-push offer. Only insert if we have a phone AND
    # no opt-in row exists yet for this subscriber.
    if customer_phone and subscriber.id:
        try:
            from src.core.models import SmsOptIn as _SmsOptIn
            existing_opt = db.execute(
                select(_SmsOptIn).where(_SmsOptIn.subscriber_id == subscriber.id)
            ).scalar_one_or_none()
            if existing_opt is None:
                db.add(_SmsOptIn(
                    phone=customer_phone,
                    subscriber_id=subscriber.id,
                    source="widget",
                    opt_in_message="Stripe Checkout phone collection",
                    opted_in_at=now,
                ))
                db.flush()
                logger.info(
                    "SmsOptIn created from Stripe Checkout: subscriber=%s phone=%s",
                    subscriber.id, customer_phone,
                )
        except Exception as exc:
            # Never block checkout flow on opt-in row insert failures
            logger.warning("SmsOptIn insert failed for subscriber=%s: %s", subscriber.id, exc)

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
    # Pass `db=` so the GHL push's audit row joins the parent transaction —
    # otherwise webhook_log opens its own session that can't see the
    # not-yet-committed subscriber row, FK-fails, and we lose the audit.
    try:
        push_subscriber_to_ghl(
            subscriber,
            stage=5,
            zip_codes=list(zip_codes),
            is_founding=is_founding,
            db=db,
        )
    except Exception:
        logger.error(
            "GHL push failed for subscriber %s — continuing without CRM sync",
            subscriber.id,
            exc_info=True,
        )

    # ── Welcome email + first leads (new subscribers only) ────────────────
    # Skipped when we merged onto an existing row — subscriber is already onboarded.
    if is_new_subscriber:
        if subscriber.email:
            from src.services.email import send_welcome_email
            send_welcome_email(subscriber)

        if subscriber.email and zip_codes:
            try:
                _send_first_leads_email(subscriber, zip_codes, db)
            except Exception:
                logger.error(
                    "First leads email failed for subscriber %s — non-critical",
                    subscriber.id, exc_info=True,
                )

    # ── Partner tier: provision multi-ZIP access ──────────────────────────
    # When a subscriber upgrades to the partner tier via checkout, we need to
    # lock all their chosen ZIPs and create the PartnerSubscription audit row.
    # The ZIP locking loop above already handles individual ZIPs; this call
    # sets the tier and creates the PartnerSubscription record.
    if tier == "partner" and zip_codes:
        try:
            from src.services.partner_tier import provision_partner_access
            provision_partner_access(db, subscriber.id, zip_codes, vertical, county_id)
        except Exception:
            logger.error(
                "partner provision failed for subscriber %s — non-fatal, tier already set",
                subscriber.id,
                exc_info=True,
            )

    # ── Referral confirmation (Phase A.1, 2026-05-04) ────────────────────
    # The referee just made their first paid purchase — flip any pending
    # ReferralEvent to confirmed and credit the referrer. Idempotent:
    # confirm_purchase only matches pending rows, so a duplicate webhook
    # delivery is a no-op. Best-effort — referral failures must not break
    # the checkout flow.
    try:
        from src.services.referral_engine import confirm_purchase
        event = confirm_purchase(subscriber.id, db)
        if event is not None:
            logger.info(
                "[Referral] confirmed: referee=%d event=%d",
                subscriber.id, event.id,
            )
    except Exception:
        logger.error(
            "[Referral] confirm/reward failed for subscriber %d — non-fatal",
            subscriber.id, exc_info=True,
        )

    logger.info(
        "checkout.session.completed: subscriber=%s tier=%s vertical=%s"
        " founding=%s zips=%s feed_uuid=%s",
        subscriber.id, tier, vertical, is_founding,
        zip_codes, subscriber.event_feed_uuid,
    )


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

    # Accelerated Wallet Push (fa016): wallet subscription first invoice cleared.
    # Branch out BEFORE the regular billing_date update so wallet activation
    # doesn't accidentally overwrite a non-wallet subscriber's billing_date.
    if _is_wallet_subscription_invoice(invoice):
        _on_wallet_subscription_invoice(invoice, db)
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

    # Clear recovery state on successful payment
    subscriber.payment_failed_at = None
    subscriber.recovery_day1_sent = False
    subscriber.recovery_day3_sent = False

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

    # fa016: wallet subscription first-invoice failures don't enter the regular
    # recovery sequence — mark the offer 'failed' so the funnel reflects it.
    if _is_wallet_subscription_invoice(invoice):
        _on_wallet_subscription_invoice_failed(invoice, db)
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()

    if subscriber is None:
        logger.warning(
            "invoice.payment_failed: no subscriber for customer %s", stripe_customer_id
        )
        return

    subscriber.payment_failed_at = datetime.now(timezone.utc)
    subscriber.recovery_day1_sent = False
    subscriber.recovery_day3_sent = False
    db.flush()

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

    # When Stripe fires subscription.updated after pause_collection is set, the Stripe-side
    # status remains "active" — don't let that overwrite our local "paused" status.
    # Guard: keep "paused" if pause_collection is still active in the event payload.
    if subscriber.status == "paused" and new_status == "active":
        pause_collection = subscription.get("pause_collection")
        if pause_collection:
            logger.info(
                "subscription.updated: subscriber=%s keeping local status=paused "
                "(Stripe status=%s but pause_collection is set)",
                subscriber.id, stripe_status,
            )
            new_status = "paused"

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

    from config.settings import get_settings
    now = datetime.now(timezone.utc)
    grace_expires = now + timedelta(hours=get_settings().grace_period_hours)

    subscriber.status = "grace"
    subscriber.grace_expires_at = grace_expires
    subscriber.ghl_stage = 7
    # Clear recovery sweep state — sweep query has no status filter, so without this
    # a subscriber who cancelled mid-recovery would keep receiving Day 1/Day 3 emails.
    subscriber.payment_failed_at = None
    subscriber.recovery_day1_sent = False
    subscriber.recovery_day3_sent = False

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
# 6. payment_intent.succeeded — router (lead pack + default card save + bundles)
# ---------------------------------------------------------------------------

def _on_payment_intent_succeeded(payment_intent, db: Session) -> None:
    """Route payment_intent.succeeded to the appropriate sub-handler."""
    meta = _attr(payment_intent, "metadata") or {}
    product = _attr(meta, "product") or _attr(meta, "kind")   # tolerate both keys
    pi_id = _attr(payment_intent, "id")
    customer_id = _attr(payment_intent, "customer")
    amount = _attr(payment_intent, "amount_received") or _attr(payment_intent, "amount")

    logger.info(
        "[PI] payment_intent.succeeded received: pi=%s product=%s customer=%s amount=%s meta_keys=%s",
        pi_id, product, customer_id, amount,
        sorted(list(meta.keys())) if isinstance(meta, dict) else "stripe_obj",
    )

    if product == "lead_pack":
        logger.info("[PI] routing -> lead_pack pi=%s", pi_id)
        _on_lead_pack_payment(payment_intent, db)
    elif product == "bundle":
        logger.info("[PI] routing -> bundle pi=%s", pi_id)
        _on_bundle_payment(payment_intent, db)
    elif product == "premium":
        logger.info("[PI] routing -> premium pi=%s", pi_id)
        _on_premium_payment(payment_intent, db)
    elif product == "wallet_topup":
        logger.info("[PI] routing -> wallet_topup pi=%s", pi_id)
        _on_wallet_topup_payment(payment_intent, db)
    elif product == "lead_unlock":
        logger.info("[PI] routing -> lead_unlock (+ card_save) pi=%s", pi_id)
        _on_lead_unlock_payment(payment_intent, db)
        # Fall through to card-save so the unlock also triggers the saved-card flow
        _on_card_saved(payment_intent, db)
    else:
        logger.info("[PI] routing -> card_save (no product metadata) pi=%s", pi_id)
        _on_card_saved(payment_intent, db)

    # ── Referral confirmation (any PI-based paid action) ─────────────────
    # checkout.session.completed handles subscription first-payments; this
    # branch covers wallet top-ups, premium credits, bundles, lead packs,
    # and one-off lead unlocks. confirm_purchase is idempotent (matches only
    # pending events), so a duplicate webhook delivery — or a referee whose
    # event was already confirmed by an earlier checkout — is a no-op.
    subscriber_id = _resolve_subscriber_id_from_pi(payment_intent, db)
    if subscriber_id is None:
        logger.info(
            "[Referral] PI %s — no subscriber resolved (meta.subscriber_id missing and "
            "customer=%s did not match a Subscriber); skipping referral confirm",
            pi_id, customer_id,
        )
        return

    logger.info(
        "[Referral] PI %s — attempting confirm_purchase(referee=%d, product=%s)",
        pi_id, subscriber_id, product,
    )
    try:
        from src.services.referral_engine import confirm_purchase
        event = confirm_purchase(subscriber_id, db)
        if event is None:
            logger.info(
                "[Referral] PI %s — confirm_purchase returned None (no pending event "
                "for referee=%d; already confirmed or never referred)",
                pi_id, subscriber_id,
            )
        else:
            logger.info(
                "[Referral] confirmed via PI: referee=%d event=%d pi=%s product=%s "
                "referrer=%d confirmed_at=%s",
                subscriber_id, event.id, pi_id, product,
                event.referrer_subscriber_id, event.confirmed_at,
            )
    except Exception as exc:
        logger.error(
            "[Referral] PI-path confirm failed for subscriber %s — non-fatal: %s",
            subscriber_id, exc, exc_info=True,
        )


def _resolve_subscriber_id_from_pi(payment_intent, db: Session) -> Optional[int]:
    """Best-effort subscriber resolution for a PaymentIntent.

    `payment_intent` arrives from the Stripe webhook handler as a
    stripe.StripeObject, which exposes fields as attributes (not as plain
    dict keys reachable via .get()). Use getattr throughout so the same
    code works for both the SDK object and any plain-dict payload that
    may come through the sandbox simulate-stripe-event path.

    Prefers metadata.subscriber_id (set by wallet_topup, premium, bundle,
    lead_pack flows). Falls back to Stripe customer id -> Subscriber lookup
    for paths that don't set the metadata (lead_unlock, plain card-save).
    Returns None if neither yields a hit.
    """
    from src.core.models import Subscriber

    meta = _attr(payment_intent, "metadata") or {}
    raw = _attr(meta, "subscriber_id")
    if raw is not None:
        try:
            sid = int(raw)
            logger.debug("[Referral] subscriber resolved via metadata.subscriber_id=%d", sid)
            return sid
        except (TypeError, ValueError):
            logger.warning("[Referral] non-int subscriber_id in PI metadata: %r", raw)

    customer_id = _attr(payment_intent, "customer")
    if customer_id:
        sub = db.execute(
            select(Subscriber).where(Subscriber.stripe_customer_id == customer_id)
        ).scalar_one_or_none()
        if sub is not None:
            logger.debug(
                "[Referral] subscriber resolved via stripe_customer_id=%s -> id=%d",
                customer_id, sub.id,
            )
            return sub.id
        logger.debug(
            "[Referral] stripe_customer_id=%s did not match any Subscriber row",
            customer_id,
        )
    return None


def _on_lead_unlock_payment(payment_intent: dict, db: Session) -> None:
    """
    Handle a single-lead $2.50–$7 unlock purchase.

    Looks up the subscriber by Stripe customer id, looks up the property by
    metadata.property_id, and emails the full lead details (address, owner
    name, enriched contact if available). Logs a SentLead row so the
    unlock is auditable from the subscriber's dashboard.
    """
    from src.core.models import Property, Owner, DistressScore, EnrichedContact, Subscriber, SentLead

    meta = _attr(payment_intent, "metadata") or {}
    property_id_raw = _attr(meta, "property_id")
    customer_id = _attr(payment_intent, "customer")

    if not property_id_raw or not customer_id:
        logger.warning(
            "lead_unlock payment missing property_id or customer: pi=%s",
            _attr(payment_intent, "id"),
        )
        return

    try:
        property_id = int(property_id_raw)
    except (TypeError, ValueError):
        logger.warning("lead_unlock payment non-int property_id=%r", property_id_raw)
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == customer_id)
    ).scalar_one_or_none()
    if not subscriber:
        logger.warning("lead_unlock: no subscriber for customer=%s", customer_id)
        return

    prop = db.get(Property, property_id)
    if not prop:
        logger.warning("lead_unlock: property %s not found", property_id)
        return

    # Latest score for this property
    score = db.execute(
        select(DistressScore)
        .where(DistressScore.property_id == property_id)
        .order_by(DistressScore.score_date.desc())
        .limit(1)
    ).scalar_one_or_none()

    owner = db.execute(
        select(Owner).where(Owner.property_id == property_id).limit(1)
    ).scalar_one_or_none()

    enriched = db.execute(
        select(EnrichedContact).where(
            EnrichedContact.property_id == property_id,
            EnrichedContact.match_success == True,  # noqa: E712
        ).limit(1)
    ).scalar_one_or_none()

    # Audit row — SentLead marks this lead as delivered to this subscriber
    try:
        existing_sent = db.execute(
            select(SentLead).where(
                SentLead.subscriber_id == subscriber.id,
                SentLead.property_id == property_id,
            )
        ).scalar_one_or_none()
        if not existing_sent:
            db.add(SentLead(
                subscriber_id=subscriber.id,
                property_id=property_id,
                source="lead_unlock_payment",
                stripe_payment_intent_id=_attr(payment_intent, "id"),
            ))
        elif existing_sent and not existing_sent.stripe_payment_intent_id:
            existing_sent.stripe_payment_intent_id = _attr(payment_intent, "id")
    except (IntegrityError, OperationalError) as exc:
        logger.warning("lead_unlock: SentLead insert failed: %s", exc)

    # Send the email with full lead details
    try:
        _send_lead_unlock_email(subscriber, prop, score, owner, enriched)
    except Exception as exc:
        logger.error("lead_unlock: email send failed: %s", exc, exc_info=True)

    # Welcome email — deferred from /api/free-signup with intent='unlock'.
    # Sent only on the first unlock so repeat unlocks don't spam the inbox.
    try:
        first_unlock = db.execute(
            select(func.count()).select_from(SentLead).where(
                SentLead.subscriber_id == subscriber.id,
                SentLead.source == "lead_unlock_payment",
            )
        ).scalar() or 0
        if first_unlock <= 1:
            from src.services.email import send_welcome_email
            send_welcome_email(subscriber)
    except Exception as exc:
        logger.warning("lead_unlock: welcome email failed sub=%s: %s",
                       subscriber.id, exc)

    logger.info(
        "lead_unlock complete: subscriber=%s property=%s pi=%s",
        subscriber.id, property_id, _attr(payment_intent, "id"),
    )


def _send_lead_unlock_email(subscriber, prop, score, owner, enriched) -> None:
    """Send a single-lead confirmation + details email after $4 unlock."""
    from src.services.email import send_email
    from config.settings import get_settings

    _settings = get_settings()
    if not subscriber.email:
        logger.info("lead_unlock: subscriber %s has no email — skipping send", subscriber.id)
        return

    tier = (score.lead_tier if score else None) or "Scored"
    vertical = subscriber.vertical or "roofing"
    v_score = None
    if score and score.vertical_scores:
        v_score = score.vertical_scores.get(vertical)
    score_str = f"{v_score:.1f}" if v_score is not None else (
        f"{float(score.final_cds_score):.1f}" if score and score.final_cds_score else "N/A"
    )
    distress = ", ".join(score.distress_types or []) if score and score.distress_types else "—"
    owner_name = (owner.owner_name if owner and owner.owner_name else "Not on public record")
    phone = (enriched.mobile_phone if enriched and enriched.mobile_phone else "—")
    email_addr = (enriched.email if enriched and enriched.email else "—")

    dashboard_url = (
        f"{_settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
        if _settings.app_base_url and subscriber.event_feed_uuid else ""
    )

    subject = f"Your unlocked lead: {prop.address or 'Property #' + str(prop.id)}"

    text_body = (
        f"You unlocked a Forced Action lead.\n\n"
        f"Address:      {prop.address or '—'}\n"
        f"City/State:   {(prop.city or '—')}, {(prop.state or 'FL')} {prop.zip or ''}\n"
        f"Owner:        {owner_name}\n"
        f"Tier:         {tier}   (Score: {score_str})\n"
        f"Distress:     {distress}\n"
        f"Phone:        {phone}\n"
        f"Email:        {email_addr}\n\n"
        f"Dashboard: {dashboard_url}\n\n"
        "Card saved — next unlock is one tap.\n"
    )

    html_body = f"""<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#111827;border:1px solid rgba(255,255,255,0.08);border-radius:14px;padding:28px;">
        <tr><td>
          <h2 style="margin:0 0 4px;color:#fbbf24;font-size:22px;">Lead unlocked</h2>
          <p style="margin:0 0 18px;color:#94a3b8;font-size:13px;">Card saved — next unlock is one tap.</p>
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);
                        border-left:4px solid #fbbf24;border-radius:10px;padding:18px 20px;">
            <tr><td>
              <p style="margin:0 0 6px;font-size:16px;font-weight:700;color:#ffffff;">{prop.address or '—'}</p>
              <p style="margin:0 0 10px;font-size:13px;color:#94a3b8;">{(prop.city or '—')}, {(prop.state or 'FL')} {prop.zip or ''}</p>
              <p style="margin:0 0 4px;font-size:13px;color:#e2e8f0;"><b>Owner:</b> {owner_name}</p>
              <p style="margin:0 0 4px;font-size:13px;color:#e2e8f0;"><b>Tier:</b> <span style="color:#fbbf24;">{tier}</span>  &middot; <b>Score:</b> {score_str}</p>
              <p style="margin:0 0 4px;font-size:13px;color:#e2e8f0;"><b>Distress:</b> {distress}</p>
              <p style="margin:0 0 4px;font-size:13px;color:#e2e8f0;"><b>Phone:</b> {phone}</p>
              <p style="margin:0;font-size:13px;color:#e2e8f0;"><b>Email:</b> {email_addr}</p>
            </td></tr>
          </table>
          {('<p style="margin:24px 0 0;text-align:center;"><a href="' + dashboard_url + '" style="background:#fbbf24;color:#0f172a;padding:12px 22px;border-radius:8px;text-decoration:none;font-weight:700;">Open your dashboard</a></p>') if dashboard_url else ''}
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""

    send_email(
        to=subscriber.email,
        subject=subject,
        body_text=text_body,
        body_html=html_body,
    )
    logger.info("lead_unlock email sent → %s (property=%s)", subscriber.email, prop.id)


def _on_card_saved(payment_intent, db: Session) -> None:
    customer_id = _attr(payment_intent, "customer")
    pm_id = _attr(payment_intent, "payment_method")
    setup_future = _attr(payment_intent, "setup_future_usage")
    pi_id = _attr(payment_intent, "id")
    if not all([customer_id, pm_id, setup_future == "off_session"]):
        logger.debug(
            "[CardSave] skipping pi=%s — customer=%s pm=%s setup_future=%s",
            pi_id, customer_id, pm_id, setup_future,
        )
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == customer_id)
    ).scalar_one_or_none()
    if not subscriber:
        logger.info("[CardSave] no subscriber for customer=%s pi=%s", customer_id, pi_id)
        return
    if subscriber.has_saved_card:
        logger.debug("[CardSave] subscriber=%s already has saved card; pi=%s",
                     subscriber.id, pi_id)
        return

    subscriber.has_saved_card = True
    subscriber.stripe_payment_method_id = pm_id

    from src.core.redis_client import rset
    rset(f"saved_card_window:{subscriber.id}", "1", ttl_seconds=600)
    db.flush()

    logger.info("Default card saved for subscriber=%s pm=%s", subscriber.id, pm_id)

    # +2 bonus credits on first save-card event. Creates WalletBalance so the
    # dashboard wallet card becomes visible (Stage 5).
    try:
        from src.services import wallet_engine
        granted = wallet_engine.check_saved_card_bonus(subscriber.id, db)
        if granted:
            logger.info("+2 bonus credits granted to subscriber=%s (saved_card)", subscriber.id)
    except Exception as exc:
        logger.warning("saved_card_bonus grant failed sub=%s: %s", subscriber.id, exc)

    # fa017: business event audit
    try:
        from src.services.business_events import log_business_event
        log_business_event(
            "CARD_SAVED", subscriber_id=subscriber.id,
            payload={"trigger": "_on_card_saved", "pm": pm_id}, db=db,
        )
    except Exception:
        pass

    # fa016 Accelerated Wallet Push — if the subscriber already had a debit
    # before they saved the card, schedule the offer immediately.
    try:
        db.flush()  # ensure has_saved_card is visible to the detector query
        from src.services import wallet_engine
        eligible = wallet_engine.accelerated_push_eligible(subscriber.id, db)
        if eligible:
            try:
                wallet_engine.ensure_offer_row(subscriber.id, eligible, db)
            except Exception as exc_offer:
                logger.warning("ensure_offer_row failed sub=%s: %s", subscriber.id, exc_offer)
            try:
                from src.services.business_events import log_business_event
                log_business_event(
                    "ACCELERATED_WALLET_ELIGIBLE", subscriber_id=subscriber.id,
                    payload={"trigger": "_on_card_saved"}, db=db,
                )
            except Exception:
                pass
            from src.agents.supervisor import dispatch_event
            dispatch_event({
                "event_type": "accelerated_wallet_push_eligible",
                "subscriber_id": subscriber.id,
                "payload": eligible,
            })
    except Exception as exc:
        logger.warning("accelerated_wallet_push from _on_card_saved failed sub=%s: %s",
                       subscriber.id, exc)


def _on_payment_method_attached(pm: dict, db: Session) -> None:
    """fa016: belt-and-suspenders save-card flag setter.

    `payment_intent.succeeded` races with `checkout.session.completed` for paid
    signups — `_on_card_saved` often runs before the Subscriber row has been
    committed, silently exits, and `has_saved_card` stays false forever.
    `payment_method.attached` fires later in the sequence, by which point the
    Subscriber row exists, so it's a reliable secondary trigger.

    Also fires the accelerated_wallet_push detector so saved-card users with
    prior paid activity get the offer without needing another payment event.
    """
    customer_id = pm.get("customer")
    pm_id = pm.get("id")
    if not (customer_id and pm_id):
        return

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == customer_id)
    ).scalar_one_or_none()
    if subscriber is None:
        logger.info("payment_method.attached: no subscriber yet for customer=%s pm=%s",
                    customer_id, pm_id)
        return
    if subscriber.has_saved_card and subscriber.stripe_payment_method_id == pm_id:
        return  # already recorded, nothing to do

    subscriber.has_saved_card = True
    subscriber.stripe_payment_method_id = pm_id

    from src.core.redis_client import redis_available, rset
    if redis_available():
        rset(f"saved_card_window:{subscriber.id}", "1", ttl_seconds=600)

    db.flush()
    logger.info("payment_method.attached: subscriber=%s pm=%s saved", subscriber.id, pm_id)

    # +2 bonus credits on first save-card event.
    try:
        from src.services import wallet_engine
        granted = wallet_engine.check_saved_card_bonus(subscriber.id, db)
        if granted:
            logger.info("+2 bonus credits granted to subscriber=%s (pm.attached)", subscriber.id)
    except Exception as exc:
        logger.warning("saved_card_bonus grant failed sub=%s: %s", subscriber.id, exc)

    # fa017: business event audit
    try:
        from src.services.business_events import log_business_event
        log_business_event(
            "CARD_SAVED", subscriber_id=subscriber.id,
            payload={"trigger": "_on_payment_method_attached", "pm": pm_id}, db=db,
        )
    except Exception:
        pass

    # fa016 Accelerated Wallet Push — if they already have paid intent, fire now.
    try:
        from src.services import wallet_engine
        eligible = wallet_engine.accelerated_push_eligible(subscriber.id, db)
        if eligible:
            try:
                wallet_engine.ensure_offer_row(subscriber.id, eligible, db)
            except Exception as exc_offer:
                logger.warning("ensure_offer_row failed sub=%s: %s", subscriber.id, exc_offer)
            try:
                from src.services.business_events import log_business_event
                log_business_event(
                    "ACCELERATED_WALLET_ELIGIBLE", subscriber_id=subscriber.id,
                    payload={"trigger": "_on_payment_method_attached"}, db=db,
                )
            except Exception:
                pass
            from src.agents.supervisor import dispatch_event
            dispatch_event({
                "event_type": "accelerated_wallet_push_eligible",
                "subscriber_id": subscriber.id,
                "payload": eligible,
            })
    except Exception as exc:
        logger.warning("accelerated_wallet_push from pm.attached failed sub=%s: %s",
                       subscriber.id, exc)


def _on_bundle_payment(payment_intent, db: Session) -> None:
    from src.core.models import BundlePurchase
    meta = _attr(payment_intent, "metadata") or {}
    pi_id = _attr(payment_intent, "id")
    bundle_type = _attr(meta, "bundle_type")
    subscriber_id_str = _attr(meta, "subscriber_id")
    zip_code = _attr(meta, "zip_code")
    vertical = _attr(meta, "vertical")
    ab_variant = _attr(meta, "ab_variant") or None  # Stage 5: optional 'a'/'b'

    if not all([pi_id, bundle_type, subscriber_id_str]):
        logger.error("[Bundle] payment_intent.succeeded missing metadata: %s", meta)
        return

    # Idempotency
    existing = db.execute(
        select(BundlePurchase).where(BundlePurchase.stripe_payment_intent_id == pi_id)
    ).scalar_one_or_none()
    if existing:
        logger.info("[Bundle] Already processed PI %s — skipping", pi_id)
        return

    subscriber_id = int(subscriber_id_str)
    purchase = BundlePurchase(
        subscriber_id=subscriber_id,
        bundle_type=bundle_type,
        stripe_payment_intent_id=pi_id,
        status="pending",
        zip_code=zip_code,
        vertical=vertical,
        ab_variant=ab_variant if ab_variant in ("a", "b") else None,
    )
    db.add(purchase)
    db.flush()

    from src.services.bundle_engine import deliver
    deliver(purchase.id, db)
    logger.info("[Bundle] Delivered purchase=%d type=%s subscriber=%d", purchase.id, bundle_type, subscriber_id)

    # Stage 5: record A/B conversion if a variant was assigned
    if ab_variant in ("a", "b"):
        try:
            from src.services.ab_engine import record_outcome
            test_name = f"bundle_{bundle_type}_pricing"
            record_outcome(subscriber_id, test_name, "converted", db)
        except Exception as exc:
            logger.warning("[Bundle] A/B record_outcome failed: %s", exc)


# ---------------------------------------------------------------------------
# 6c. payment_intent.succeeded — Stage 5 premium credit SKUs
# ---------------------------------------------------------------------------

def _on_premium_payment(payment_intent, db: Session) -> None:
    """
    Handle cash-paid premium SKU purchases (report / brief / transfer / byol).

    Expected metadata on the PaymentIntent:
        product         = "premium"
        sku             = report | brief | transfer | byol
        subscriber_id   = numeric Subscriber.id
        property_id     = optional, required for report/brief/transfer
        target_address  = optional, required for byol
    """
    from src.core.models import PremiumPurchase
    from src.services.premium_engine import record_card_purchase, fulfill

    meta = _attr(payment_intent, "metadata") or {}
    pi_id = _attr(payment_intent, "id")
    sku = _attr(meta, "sku")
    subscriber_id_str = _attr(meta, "subscriber_id")

    if not all([pi_id, sku, subscriber_id_str]):
        logger.error("[Premium] payment_intent missing metadata: pi=%s meta=%s", pi_id, meta)
        return

    # Idempotency
    existing = db.execute(
        select(PremiumPurchase).where(PremiumPurchase.stripe_payment_intent_id == pi_id)
    ).scalar_one_or_none()
    if existing:
        logger.info("[Premium] Already processed PI %s — skipping", pi_id)
        return

    try:
        subscriber_id = int(subscriber_id_str)
    except (TypeError, ValueError):
        logger.error("[Premium] non-int subscriber_id=%r", subscriber_id_str)
        return

    # fa017 orphan-safety: validate the subscriber exists BEFORE handing off
    # to record_card_purchase, which would otherwise raise IntegrityError on
    # the FK constraint and abort the transaction with no audit trail.
    if db.get(Subscriber, subscriber_id) is None:
        logger.error(
            "[Premium] orphan PI %s — subscriber_id=%d not found; logging audit row",
            pi_id, subscriber_id,
        )
        try:
            from src.services.webhook_log import log_webhook_event
            log_webhook_event(
                source="stripe", event_type="payment_intent.succeeded",
                source_event_id=pi_id, status="failed",
                status_detail="orphan_subscriber",
                payload={"subscriber_id": subscriber_id, "sku": sku},
                payload_kind="generic", db=db,
            )
        except Exception:
            pass
        return

    property_id_raw = _attr(meta, "property_id")
    property_id: Optional[int] = None
    if property_id_raw:
        try:
            property_id = int(property_id_raw)
        except (TypeError, ValueError):
            logger.warning("[Premium] non-int property_id=%r", property_id_raw)

    target_address = _attr(meta, "target_address")
    amount_cents = _attr(payment_intent, "amount_received") or _attr(payment_intent, "amount")

    purchase = record_card_purchase(
        subscriber_id=subscriber_id,
        sku=sku,
        stripe_payment_intent_id=pi_id,
        db=db,
        property_id=property_id,
        target_address=target_address,
        amount_cents=amount_cents,
    )

    try:
        fulfill(purchase.id, db)
    except Exception as exc:
        # fulfillment errors don't fail the webhook — purchase row is already
        # persisted with status='failed' and ops can re-run fulfillment.
        logger.error("[Premium] fulfillment failed for purchase=%d: %s", purchase.id, exc)

    logger.info(
        "[Premium] Purchase recorded: id=%d sku=%s subscriber=%d pi=%s",
        purchase.id, sku, subscriber_id, pi_id,
    )

    # fa017: business event audit trail
    try:
        from src.services.business_events import log_business_event
        log_business_event(
            "PREMIUM_PURCHASE_COMPLETED", subscriber_id=subscriber_id,
            property_id=property_id,
            payload={"sku": sku, "pi": pi_id, "amount_cents": amount_cents},
            db=db,
        )
        log_business_event(
            "PAYMENT_SUCCEEDED", subscriber_id=subscriber_id,
            property_id=property_id,
            payload={"product": "premium", "sku": sku, "pi": pi_id}, db=db,
        )
    except Exception:
        pass

    # fa016 Accelerated Wallet Push — cash premium purchase is "first paid
    # intent" for a saved-card user. Wrap in try/except so failure never
    # disturbs the webhook ack.
    try:
        from src.services import wallet_engine
        eligible = wallet_engine.accelerated_push_eligible(subscriber_id, db)
        if eligible:
            try:
                from src.services.business_events import log_business_event
                log_business_event(
                    "ACCELERATED_WALLET_ELIGIBLE", subscriber_id=subscriber_id,
                    payload={"reason": eligible.get("reason"),
                             "tier": eligible.get("tier")}, db=db,
                )
            except Exception:
                pass
            from src.agents.supervisor import dispatch_event
            dispatch_event({
                "event_type": "accelerated_wallet_push_eligible",
                "subscriber_id": subscriber_id,
                "payload": eligible,
            })
    except Exception as exc:
        logger.warning("accelerated_wallet_push detector failed sub=%s: %s",
                       subscriber_id, exc)


# ---------------------------------------------------------------------------
# 6e. payment_intent.succeeded — wallet top-ups (Stage 5+, fa004 2026-05-04)
# ---------------------------------------------------------------------------

def _on_wallet_topup_payment(payment_intent, db: Session) -> None:
    """Credit the subscriber's wallet for a successful wallet top-up.

    Idempotent on the PaymentIntent id — replayed events become a no-op
    via the (subscriber_id, stripe_charge_id) duplicate check on
    WalletTransaction.
    """
    from src.core.models import Subscriber, WalletTransaction
    from src.services import wallet_engine

    meta = _attr(payment_intent, "metadata") or {}
    pi_id = _attr(payment_intent, "id")
    subscriber_id_str = _attr(meta, "subscriber_id")
    credits_str = _attr(meta, "credits")
    amount_cents_str = _attr(meta, "amount_cents")

    if not all([pi_id, subscriber_id_str, credits_str]):
        logger.error("[WalletTopup] missing metadata: pi=%s meta=%s", pi_id, meta)
        return

    try:
        subscriber_id = int(subscriber_id_str)
        credits = int(credits_str)
    except (TypeError, ValueError):
        logger.error("[WalletTopup] non-int metadata: %s", meta)
        return

    sub = db.get(Subscriber, subscriber_id)
    if sub is None:
        logger.error("[WalletTopup] subscriber=%d not found", subscriber_id)
        return

    # Secondary idempotency — protect against StripeWebhookEvent table being
    # truncated. WalletTransaction.stripe_charge_id is the dedup key.
    existing = db.execute(
        select(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.stripe_charge_id == pi_id,
        )
    ).scalar_one_or_none()
    if existing:
        logger.info("[WalletTopup] already credited PI %s — skipping", pi_id)
        return

    wallet_engine.credit(
        subscriber_id=subscriber_id,
        amount=credits,
        description=f"wallet_topup:{amount_cents_str}cents",
        db=db,
        stripe_charge_id=pi_id,
    )
    logger.info(
        "[WalletTopup] subscriber=%d credited=%d cents=%s pi=%s",
        subscriber_id, credits, amount_cents_str, pi_id,
    )


# ---------------------------------------------------------------------------
# 6f. invoice.payment_succeeded — wallet subscription activation (fa016)
# ---------------------------------------------------------------------------

def _invoice_subscription_details(invoice: dict) -> dict:
    """Locate the subscription_details block for an invoice across Stripe API
    versions.

    - Pre-Basil: `invoice.subscription_details = {subscription, metadata}`
    - Basil (2025-03-31+): `invoice.parent.subscription_details = {...}`
    Returns {} when the invoice is not subscription-derived.
    """
    sd = invoice.get("subscription_details")
    if sd:
        return sd
    parent = invoice.get("parent") or {}
    if isinstance(parent, dict) and parent.get("type") == "subscription_details":
        return parent.get("subscription_details") or {}
    return {}


def _invoice_subscription_id(invoice: dict) -> Optional[str]:
    """Locate the parent subscription id across Stripe API versions."""
    sub_id = invoice.get("subscription")
    if sub_id:
        return sub_id
    sd = _invoice_subscription_details(invoice)
    return sd.get("subscription") if sd else None


def _is_wallet_subscription_invoice(invoice: dict) -> bool:
    """Return True when the invoice belongs to a wallet_subscription created by
    accelerated_wallet_push (or in-app accept)."""
    inv_meta = (invoice.get("metadata") or {})
    if inv_meta.get("product") == "wallet_subscription":
        return True
    sub_meta = (_invoice_subscription_details(invoice).get("metadata") or {})
    if sub_meta.get("product") == "wallet_subscription":
        return True
    sub_id = _invoice_subscription_id(invoice)
    if not sub_id:
        return False
    try:
        from config.settings import settings
        import stripe as _stripe
        key = settings.active_stripe_secret_key
        if not key:
            return False
        _stripe.api_key = key.get_secret_value()
        sub = _stripe.Subscription.retrieve(sub_id)
        return (sub.get("metadata") or {}).get("product") == "wallet_subscription"
    except Exception:
        return False


def _extract_wallet_sub_metadata(invoice: dict) -> dict:
    """Pull subscriber_id / wallet_offer_id / tier / subscription_id from the
    invoice metadata, parent.subscription_details.metadata, or (as a last
    resort) the parent Subscription itself."""
    out: dict = {}
    for src in (invoice.get("metadata"), _invoice_subscription_details(invoice).get("metadata")):
        if src:
            out.update({k: v for k, v in src.items() if v is not None})
    sub_id = _invoice_subscription_id(invoice)
    if sub_id and "tier" not in out:
        try:
            from config.settings import settings
            import stripe as _stripe
            key = settings.active_stripe_secret_key
            if key:
                _stripe.api_key = key.get_secret_value()
                sub = _stripe.Subscription.retrieve(sub_id)
                out.update(sub.get("metadata") or {})
        except Exception:
            pass
    if sub_id:
        out["subscription_id"] = sub_id
    return out


def _on_wallet_subscription_invoice(invoice: dict, db: Session) -> None:
    """Activate the wallet on the first successful invoice of a wallet
    subscription. Idempotent — replay via either StripeWebhookEvent (handled
    by dispatcher) or WalletPushOffer.status == 'activated' short-circuit."""
    from src.core.models import Subscriber, WalletPushOffer
    from src.services import wallet_engine

    meta = _extract_wallet_sub_metadata(invoice)
    subscriber_id_str = meta.get("subscriber_id")
    offer_id_str = meta.get("wallet_offer_id")
    tier = meta.get("tier") or "starter_wallet"
    subscription_id = meta.get("subscription_id")
    pi = (invoice.get("payment_intent") or invoice.get("id"))

    if not subscriber_id_str:
        logger.error("[WalletSub] invoice missing subscriber_id meta=%s", meta)
        return
    try:
        subscriber_id = int(subscriber_id_str)
    except (TypeError, ValueError):
        logger.error("[WalletSub] non-int subscriber_id=%r", subscriber_id_str)
        return

    sub = db.get(Subscriber, subscriber_id)
    if sub is None:
        logger.error("[WalletSub] subscriber=%d not found", subscriber_id)
        return

    # Secondary idempotency on the funnel table
    offer = None
    if offer_id_str:
        try:
            offer = db.get(WalletPushOffer, int(offer_id_str))
        except (TypeError, ValueError):
            offer = None
    if offer is None and subscription_id:
        offer = db.execute(
            select(WalletPushOffer)
            .where(WalletPushOffer.stripe_subscription_id == subscription_id)
        ).scalar_one_or_none()
    if offer is not None and offer.status == "activated":
        logger.info("[WalletSub] offer=%s already activated — skipping", offer.id)
        return

    # Enroll wallet (creates WalletBalance and credits the cycle credits)
    wallet_engine.enroll(subscriber_id, tier, db=db)

    if offer is not None:
        offer.status = "activated"
        offer.activated_at = datetime.now(timezone.utc)
        if subscription_id:
            offer.stripe_subscription_id = subscription_id
        db.flush()
    elif subscription_id:
        logger.warning(
            "[WalletSub] no WalletPushOffer matched subscription=%s — wallet activated regardless",
            subscription_id,
        )

    # Transactional confirmation SMS (bypasses Cora — not marketing)
    try:
        from src.services.sms_compliance import send_sms as _send_sms
        if sub.phone:
            credits = wallet_engine.get_balance(subscriber_id, db)
            _send_sms(
                to=sub.phone,
                body=f"Wallet active. {credits} credits loaded. Reply BALANCE any time.",
                db=db,
                subscriber_id=subscriber_id,
                task_type="wallet_activated",
                campaign="accelerated_wallet_push_activation",
            )
    except Exception as exc:
        logger.warning("[WalletSub] confirmation SMS failed sub=%s: %s", subscriber_id, exc)

    logger.info(
        "[WalletSub] activated subscriber=%d offer=%s sub=%s tier=%s pi=%s",
        subscriber_id, getattr(offer, "id", None), subscription_id, tier, pi,
    )

    try:
        from src.services.business_events import log_business_event
        log_business_event(
            "WALLET_ACTIVATED", subscriber_id=subscriber_id,
            payload={
                "tier": tier,
                "subscription_id": subscription_id,
                "offer_id": getattr(offer, "id", None),
            },
            db=db,
        )
    except Exception:
        pass


def _on_wallet_subscription_invoice_failed(invoice: dict, db: Session) -> None:
    """Mark a wallet_push_offers row as 'failed' when the first invoice fails.
    Does not touch the subscriber's regular billing recovery flags."""
    from src.core.models import WalletPushOffer

    meta = _extract_wallet_sub_metadata(invoice)
    subscription_id = meta.get("subscription_id")
    offer_id_str = meta.get("wallet_offer_id")

    offer = None
    if offer_id_str:
        try:
            offer = db.get(WalletPushOffer, int(offer_id_str))
        except (TypeError, ValueError):
            offer = None
    if offer is None and subscription_id:
        offer = db.execute(
            select(WalletPushOffer)
            .where(WalletPushOffer.stripe_subscription_id == subscription_id)
        ).scalar_one_or_none()

    if offer is None:
        logger.warning("[WalletSub] no offer matched failed invoice meta=%s", meta)
        return

    if offer.status not in ("offered", "accepted"):
        return  # already terminal — do not regress

    offer.status = "failed"
    db.flush()
    logger.info("[WalletSub] offer=%s marked failed sub=%s", offer.id, subscription_id)


# ---------------------------------------------------------------------------
# 6d. charge.refunded — refund clawback (Stage 5+, fa004 2026-05-04)
# ---------------------------------------------------------------------------

# SKUs whose fulfillment surrenders data that can't be unsent. Refunding the
# card payment does NOT credit-back the wallet for these; we log the loss.
_DATA_SURRENDERED_SKUS = {"transfer", "byol"}


def _resolve_premium_purchase_from_charge(charge: dict, db: Session):
    """Look up a PremiumPurchase by the charge's payment_intent or charge id.

    Stripe sends `charge.refunded` and `charge.dispute.*` events whose object
    is a Charge. We persisted the PaymentIntent ID on the original purchase,
    not the Charge ID — so prefer payment_intent first, fall back to
    stripe_charge_id (set by record_card_purchase or by this handler).
    """
    from src.core.models import PremiumPurchase
    pi_id = charge.get("payment_intent")
    charge_id = charge.get("id")
    purchase = None
    if pi_id:
        purchase = db.execute(
            select(PremiumPurchase)
            .where(PremiumPurchase.stripe_payment_intent_id == pi_id)
            .with_for_update()
        ).scalar_one_or_none()
    if purchase is None and charge_id:
        purchase = db.execute(
            select(PremiumPurchase)
            .where(PremiumPurchase.stripe_charge_id == charge_id)
            .with_for_update()
        ).scalar_one_or_none()
    return purchase


def _send_founder_alert(message: str) -> None:
    """Founder SMS alert via the existing Revenue Pulse SMS path. Best-effort."""
    try:
        from src.tasks.revenue_pulse import _send_sms
        _send_sms(message[:320])
    except Exception as exc:
        logger.error("Founder alert failed: %s", exc)


def _on_charge_refunded(charge: dict, db: Session) -> None:
    """charge.refunded — flip purchase status, optionally clawback credits.

    Policy:
      - Card-paid purchases: status → refunded, log refund_amount_cents.
      - Credit-paid purchases of artifact SKUs (report/brief): credit-back the
        wallet via wallet_engine.refund_credits() so the user isn't double-charged.
      - Credit-paid purchases of data-surrendered SKUs (transfer/byol): no
        credit clawback — the underlying cost (BatchData lookup) was paid and
        the data can't be unsent. Log the loss; ops can manually adjust.
    """
    from src.core.models import PremiumPurchase
    purchase = _resolve_premium_purchase_from_charge(charge, db)
    if purchase is None:
        # Not one of our premium charges (could be a wallet topup, lead pack, etc.)
        # Future: extend to handle those if/when their refund handlers land.
        logger.debug("[Refund] no PremiumPurchase for charge=%s", charge.get("id"))
        return

    if purchase.status == "refunded":
        logger.info("[Refund] purchase %d already refunded — skipping", purchase.id)
        return

    refund_amount = charge.get("amount_refunded") or charge.get("amount") or 0
    refunds = (charge.get("refunds", {}) or {}).get("data") or [{}]
    reason = (refunds[0].get("reason") if refunds else None) or "unspecified"

    purchase.status = "refunded"
    purchase.refunded_at = datetime.now(timezone.utc)
    purchase.refund_reason = reason[:100]
    purchase.refund_amount_cents = refund_amount
    if not purchase.stripe_charge_id and charge.get("id"):
        purchase.stripe_charge_id = charge["id"]
    db.flush()

    if purchase.paid_via == "credits" and purchase.sku not in _DATA_SURRENDERED_SKUS:
        from src.services import wallet_engine
        wallet_engine.refund_credits(
            subscriber_id=purchase.subscriber_id,
            amount=purchase.credits_spent or 0,
            description=f"refund_clawback:{purchase.sku}:{purchase.id}",
            db=db,
            stripe_charge_id=charge.get("id"),
        )
        logger.info(
            "[Refund] credit clawback: purchase=%d sku=%s credits=%d",
            purchase.id, purchase.sku, purchase.credits_spent or 0,
        )
    elif purchase.paid_via == "credits":
        logger.warning(
            "[Refund] data-surrendered SKU not credit-clawed: purchase=%d sku=%s",
            purchase.id, purchase.sku,
        )

    # Revoke any active referral teams the refunded subscriber belongs to
    from src.services.referral_engine import revoke_team_for_subscriber
    revoke_team_for_subscriber(purchase.subscriber_id, "refund", db)

    _send_founder_alert(
        f"REFUND: {purchase.sku} ${(refund_amount or 0) / 100:.0f} sub={purchase.subscriber_id} "
        f"purchase={purchase.id} reason={reason}"
    )
    logger.info(
        "[Refund] purchase=%d sku=%s amount_cents=%d reason=%s",
        purchase.id, purchase.sku, refund_amount, reason,
    )


def _on_dispute_created(dispute: dict, db: Session) -> None:
    """charge.dispute.created — set status='disputed', bump subscriber counter.

    Funds aren't withdrawn yet, but the dispute itself is the trust signal.
    Two disputes in 90 days flips the subscriber to status='disputed' which
    blocks future premium purchases at the API layer.
    """
    from src.core.models import PremiumPurchase
    charge = dispute.get("charge")
    # Stripe wraps the charge id when expanded, or sends the id as a string
    if isinstance(charge, dict):
        charge_obj = charge
    else:
        # fall back to a stub so the resolver can match by charge id
        charge_obj = {"id": charge, "payment_intent": dispute.get("payment_intent")}

    purchase = _resolve_premium_purchase_from_charge(charge_obj, db)
    if purchase is None:
        logger.debug("[Dispute] no PremiumPurchase for charge=%s", charge_obj.get("id"))
        return

    reason = dispute.get("reason") or "unknown"
    purchase.status = "disputed"
    purchase.disputed_at = datetime.now(timezone.utc)
    purchase.dispute_reason = reason[:100]
    if not purchase.stripe_charge_id and charge_obj.get("id"):
        purchase.stripe_charge_id = charge_obj["id"]

    # Bump subscriber-level counter and check the 2-in-90-day flip
    sub = db.execute(
        select(Subscriber).where(Subscriber.id == purchase.subscriber_id).with_for_update()
    ).scalar_one_or_none()
    if sub:
        sub.disputed_count = (sub.disputed_count or 0) + 1
        sub.disputed_at = datetime.now(timezone.utc)
        # Flip to 'disputed' status if 2+ disputes in 90 days
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        recent = db.execute(
            select(func.count()).select_from(PremiumPurchase).where(
                PremiumPurchase.subscriber_id == sub.id,
                PremiumPurchase.disputed_at.isnot(None),
                PremiumPurchase.disputed_at >= cutoff,
            )
        ).scalar() or 0
        if recent >= 2 and sub.status not in ("churned", "cancelled"):
            sub.status = "disputed"
            logger.warning(
                "[Dispute] subscriber=%d flipped to status=disputed (%d disputes in 90d)",
                sub.id, recent,
            )
    db.flush()

    # Revoke any active referral teams the disputed subscriber belongs to
    from src.services.referral_engine import revoke_team_for_subscriber
    revoke_team_for_subscriber(purchase.subscriber_id, "dispute", db)

    _send_founder_alert(
        f"DISPUTE: {purchase.sku} sub={purchase.subscriber_id} "
        f"purchase={purchase.id} reason={reason}"
    )
    logger.info(
        "[Dispute] purchase=%d sku=%s reason=%s",
        purchase.id, purchase.sku, reason,
    )


def _on_dispute_funds_withdrawn(dispute: dict, db: Session) -> None:
    """charge.dispute.funds_withdrawn — funds actually pulled by the bank.

    This is the realised-loss event. We treat it like a refund for the
    purpose of credit clawback (artifact SKUs only) and update the running
    refund_amount_cents on the purchase. Status stays 'disputed' so ops can
    distinguish a chargeback from a friendly refund.
    """
    from src.core.models import PremiumPurchase
    charge = dispute.get("charge")
    if isinstance(charge, dict):
        charge_obj = charge
    else:
        charge_obj = {"id": charge, "payment_intent": dispute.get("payment_intent")}

    purchase = _resolve_premium_purchase_from_charge(charge_obj, db)
    if purchase is None:
        logger.debug(
            "[DisputeFunds] no PremiumPurchase for charge=%s", charge_obj.get("id"),
        )
        return

    amount = dispute.get("amount") or 0
    purchase.refund_amount_cents = amount
    if not purchase.refunded_at:
        purchase.refunded_at = datetime.now(timezone.utc)
    db.flush()

    if purchase.paid_via == "credits" and purchase.sku not in _DATA_SURRENDERED_SKUS:
        from src.services import wallet_engine
        wallet_engine.refund_credits(
            subscriber_id=purchase.subscriber_id,
            amount=purchase.credits_spent or 0,
            description=f"dispute_clawback:{purchase.sku}:{purchase.id}",
            db=db,
            stripe_charge_id=charge_obj.get("id"),
        )

    _send_founder_alert(
        f"CHARGEBACK: {purchase.sku} ${amount / 100:.0f} sub={purchase.subscriber_id} "
        f"purchase={purchase.id}"
    )
    logger.warning(
        "[DisputeFunds] purchase=%d sku=%s amount_cents=%d",
        purchase.id, purchase.sku, amount,
    )


# ---------------------------------------------------------------------------
# 6b. payment_intent.succeeded — lead pack purchases (kept for internal use)
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
    meta = _attr(payment_intent, "metadata") or {}
    if _attr(meta, "product") != "lead_pack":
        # Not a lead pack payment — silently ignore
        return

    stripe_payment_intent_id = _attr(payment_intent, "id")
    feed_uuid  = _attr(meta, "feed_uuid")
    zip_code   = _attr(meta, "zip_code")
    vertical   = _attr(meta, "vertical")
    county_id  = _attr(meta, "county_id", "hillsborough")

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

    from src.core.models import Owner
    from src.utils.lead_filters import has_contact_filter, phone_priority_order
    lead_filter = [
        Property.zip == zip_code,
        Property.county_id == county_id,
        DistressScore.qualified == True,  # noqa: E712
    ]
    if active_exclusive_ids:
        lead_filter.append(~Property.id.in_(active_exclusive_ids))
    contact_clause = has_contact_filter(settings)
    if contact_clause is not None:
        lead_filter.append(contact_clause)

    top_leads = db.execute(
        select(Property, DistressScore)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(and_(*lead_filter))
        .order_by(*phone_priority_order(score_col))
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
