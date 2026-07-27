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

import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import stripe
from sqlalchemy import select, and_, desc, func, text
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
from src.services import lead_exclusivity

logger = logging.getLogger(__name__)

# Months prepaid per billing interval — used to normalize a period charge into
# a monthly run-rate for `Subscriber.plan_price` (read as MRR app-wide).
_MONTHS_PER_INTERVAL = {"monthly": 1, "annual": 12}


def normalized_monthly_price(amount_cents: int, interval: str) -> float:
    """Convert a period charge (cents) into a monthly dollar run-rate.

    An annual charge is a full year prepaid up front, so its MRR contribution is
    the charge divided by 12. Unknown intervals fall back to monthly (divide by 1)
    to preserve the historical behavior.
    """
    months = _MONTHS_PER_INTERVAL.get((interval or "monthly").lower(), 1)
    return round(amount_cents / 100 / months, 2)


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


# Campaign attribution fields stamped from Stripe metadata onto a subscriber.
_CAMPAIGN_META_FIELDS = (
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "campaign_id",
    "attribution_token",
)


def _stamp_campaign_fields(subscriber, meta, db: Session) -> None:
    """Backfill subscriber campaign fields from Stripe metadata — NULL-only.

    The buyer's attribution (utm_*, campaign_id, fbclid) is stamped into Stripe
    metadata at checkout/PI creation. Persist it onto the subscriber row so paid
    checkouts — not just free-signup origins — carry campaign attribution for the
    ROAS endpoint. Only writes fields that are currently NULL so it never
    overwrites attribution captured earlier via /api/free-signup. Reads via
    `_attr` so it works for both plain dicts and Stripe SDK objects.

    `signup_source` is handled separately (not NULL-only, since it has a
    NOT NULL "direct" default): a still-unattributed subscriber (source in
    direct/unknown/empty) upgrades to "landing_page" when campaign metadata
    is present, via the same first-touch rule signup_engine._apply_signup_source
    uses — so a subscriber who paid before /api/free-signup ran isn't stuck
    recorded as "direct" even though utm_* is now known.
    """
    if meta is None:
        return
    changed = False
    for field in _CAMPAIGN_META_FIELDS:
        value = _attr(meta, field)
        if value and getattr(subscriber, field, None) is None:
            setattr(subscriber, field, value)
            changed = True
    if (_attr(meta, "utm_source") or _attr(meta, "campaign_id")) and (
        (subscriber.signup_source or "").strip().lower() in ("", "direct", "unknown")
    ):
        subscriber.signup_source = "landing_page"
        changed = True
    if changed:
        db.flush()


def _fire_capi_for_pi(payment_intent, subscriber, source: str, event_id: str, db: Session) -> None:
    """Fire a Meta CAPI Purchase for a PaymentIntent-based product. Never raises.

    Shared by the lead-unlock / bundle / premium handlers. Reads buyer context
    (IP/UA + attribution) from the PaymentIntent metadata stamped at creation
    time, NULL-stamps the subscriber's campaign fields, then reports the event.
    Isolated in its own try/except so a Meta failure never disturbs fulfillment.
    """
    try:
        meta = _attr(payment_intent, "metadata") or {}
        _stamp_campaign_fields(subscriber, meta, db)
        amount_cents = _attr(payment_intent, "amount_received") or _attr(payment_intent, "amount") or 0
        from src.services.meta_capi_service import fire_purchase_event
        fire_purchase_event(
            subscriber=subscriber,
            amount=round(amount_cents / 100, 2),
            source=source,
            request_context={
                "buyer_ip": _attr(meta, "buyer_ip"),
                "buyer_user_agent": _attr(meta, "buyer_user_agent"),
                "fbclid": _attr(meta, "fbclid"),
                "utm_campaign": _attr(meta, "utm_campaign"),
                "campaign_id": _attr(meta, "campaign_id"),
                "currency": (_attr(payment_intent, "currency") or "usd").upper(),
            },
            event_id=event_id,
        )
    except Exception:
        logger.warning("Meta CAPI %s purchase failed — non-fatal", source, exc_info=True)


def handle_webhook(raw_body: bytes, sig_header: str, db: Session, background_tasks=None) -> tuple[bool, str]:
    """
    Verify and dispatch a Stripe webhook event.

    `background_tasks` (a FastAPI BackgroundTasks instance, or None) is only
    used for checkout.session.completed — see _on_checkout_completed's
    docstring for the fast/deferred split this enables. Every other event
    type's handler signature is untouched.

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
        "checkout.session.expired":       _on_checkout_expired,
    }

    # Standalone products share this endpoint + signing secret. Check ownership
    # FIRST so events route to their own handler and never run the property path.

    handler = None

    # Bankruptcy Filing Alerts
    try:
        from src.services.bankruptcy_alert.subscription import resolve_handler as _bk_resolve
        handler = _bk_resolve(event_type, data, db)
        if handler is not None:
            logger.info("Routing %s to bankruptcy-alert handler", event_type)
    except Exception:
        logger.warning("bankruptcy resolve_handler errored — falling back", exc_info=True)

    # Supplier Intelligence Foundation (fa067)
    if handler is None:
        try:
            from src.services.supplier_intel.subscription import resolve_handler as _si_resolve
            handler = _si_resolve(event_type, data, db)
            if handler is not None:
                logger.info("Routing %s to supplier-intel handler", event_type)
        except Exception:
            logger.warning("supplier_intel resolve_handler errored — falling back", exc_info=True)

    if handler is None:
        handler = handlers.get(event_type)
    if handler is None:
        logger.debug("Unhandled Stripe event type: %s", event_type)
        return True, "Ignored"

    try:
        if handler is _on_checkout_completed:
            handler(data, db, background_tasks=background_tasks)
        else:
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

def _on_checkout_completed(session: dict, db: Session, background_tasks=None) -> None:
    """
    FAST PATH — synchronous, runs inside the webhook request's transaction.
    Must stay short: this is what blocks Stripe's ack, and it's the only part
    the subscriber's own dashboard depends on becoming visible.

    Does what's needed for the subscriber to see their upgrade land, PLUS
    anything that must never be silently lost (consent audit trail, revenue
    accounting) — everything here commits atomically in one transaction:
      - Increment founding_subscriber_counts (atomic — already locked by stripe_service at checkout)
      - Create/update the Subscriber row (tier, status, Stripe ids, rate lock)
      - Lock ZIP territories
      - Bust the ZIP-availability cache
      - Generate event_feed_uuid (new subscribers only)
      - Trial/price detection + link the checkout consent row to the
        subscriber + activate the Customer Account/MRR ledger row (B1/M9)

    That last group (consent linking, Customer Account/MRR activation) was
    originally deferred alongside the marketing/analytics work below, but a
    2026-07 review caught that this is wrong: FastAPI BackgroundTasks is
    in-process and non-durable, so a deploy/crash/restart between the
    webhook's ack and the background task running would silently lose that
    work forever — and by then the webhook dedupe row has already committed,
    so Stripe never retries to recover it. A missing consent link undermines
    audit compliance; a missing MRR row makes revenue reporting silently
    diverge from active subscribers. Neither is acceptable to lose that way,
    so both moved into this committed path instead, each behind its own
    db.begin_nested() savepoint so a failure in one can't poison the
    subscriber/ZIP-lock commit or the sibling block.

    Everything else — GHL sync, welcome/upgrade/first-leads/founder-alert
    emails, saved-card detection (an extra Stripe API call), referral/
    segmentation/attribution/A-B-holdout bookkeeping, Meta CAPI, campaign
    attribution, affiliate confirm, subscriber-memory projection — genuinely
    is best-effort (losing one doesn't corrupt an audit trail or a revenue
    number, just a marketing/analytics side-channel) and is handed off to
    _checkout_completed_deferred() via `background_tasks`, which runs AFTER
    the HTTP response to Stripe has already been sent.

    Why this split exists (2026-07-20): this handler used to do all of the
    above inline, in one ~30+ second synchronous call chain — several
    sequential external round-trips (Stripe subscription/customer/payment-
    method retrieves, GHL, two emails, Meta CAPI) blocking one after another.
    That was slow enough to risk Stripe's own webhook delivery timeout
    triggering a retry of the whole event, and — the more serious bug this
    session's debugging uncovered — a failure in ANY one of those unrelated
    side-effects (a bare `except Exception:` with no `db.rollback()`, e.g. the
    referral_events.prompt_funnel_id schema-drift incident) left the DB
    session in Postgres's "current transaction is aborted" state, cascading
    into every later statement and, if it escaped uncaught, silently rolling
    back the ENTIRE transaction — including the subscriber tier/ZIP-lock
    upgrade that had already been staged. Splitting fast-critical from
    deferred-best-effort fixes both: the fast path commits and Stripe gets
    acked in well under a second, and nothing in the deferred half can touch
    an upgrade that's already durably committed.

    `background_tasks` is optional. When None — e.g. called directly from a
    script, test, or an admin webhook-replay tool rather than the real HTTP
    route — the deferred work runs inline on the SAME session instead of
    being scheduled, matching this function's pre-split, fully-synchronous
    behavior for those callers. (It cannot open a second session and run
    inline while this fast path's transaction is still uncommitted — a
    separate connection wouldn't see the subscriber row yet under Postgres's
    read-committed isolation.)

    No retry: every deferred side-effect below already only ran once with no
    retry before this split either (a caught exception never retried; only an
    *uncaught* one triggered Stripe's whole-event redelivery, which is exactly
    the dangerous behavior removed here). This split doesn't make any of them
    less reliable than they already were — it just removes the path where a
    failure in one could corrupt the others or the core upgrade. If retryable
    delivery for these side-effects is ever needed, that's a separate, bigger
    piece of work (a durable outbox table + periodic cron sweep, the same
    shape as `lifecycle_event_queue`'s 60s sweep) — deliberately out of scope here.

    Add-on products (auto_mode_addon, etc.) short-circuit at the top — they
    don't create a Subscriber row, they activate an entitlement flag on an
    existing one. Identification is by `metadata.subscriber_id` (set by the
    /api/checkout/auto-mode endpoint).
    """
    meta = session.get("metadata", {}) or {}

    # ── Auto Mode add-on branch ─────────────────────────────────────────────
    # Triggered by POST /api/checkout/auto-mode → Stripe Checkout completion.
    # Match by line item price == settings.stripe_price_auto_mode AND by the
    # metadata flag (belt-and-braces against schema drift).
    if meta.get("product") == "auto_mode_addon":
        _on_auto_mode_addon_purchase(session, db)
        return

    # Hot lead unlock is a mode="payment" session fulfilled by
    # payment_intent.succeeded (metadata travels on the PI via
    # payment_intent_data) — nothing to do here, and falling through would
    # log a false "missing required metadata" error.
    if meta.get("product") == "hot_lead_unlock":
        return

    tier        = meta.get("tier")
    vertical    = meta.get("vertical")
    county_id   = meta.get("county_id")
    zip_codes   = [z.strip() for z in meta.get("zip_codes", "").split(",") if z.strip()]
    is_founding = meta.get("is_founding") == "True"
    founding_price_id = meta.get("founding_price_id") or None
    # Buyer already has an authenticated dashboard session (e.g. a free-tier
    # subscriber upgrading in-place) — send an upgrade confirmation instead of
    # the new-subscriber magic-link welcome; they don't need a fresh login link.
    dashboard_upgrade = meta.get("dashboard_upgrade") == "True"

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
    # re-subscribe after cancel before the DB constraint was in place), AND the
    # Phase 2B upgrade path where /api/free-signup pre-provisions a tier='free'
    # row before the paid checkout opens.
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
            # tier='free' row → this is the Phase 2B upgrade-from-free path
            # (welcome email was deferred during /api/free-signup with
            # intent='upgrade'); send it now that payment succeeded.
            # Any other tier → genuine duplicate (stale replay, dup tab) — skip.
            if subscriber.tier == "free":
                is_new_subscriber = True
                logger.info(
                    "checkout.session.completed: upgrading pre-provisioned free row"
                    " — email=%s subscriber=%s tier=free → %s (welcome will fire)",
                    customer_email, subscriber.id, tier,
                )
            else:
                logger.warning(
                    "checkout.session.completed: duplicate detected — email=%s vertical=%s county=%s"
                    " already has subscriber id=%s tier=%s — merging onto existing row, skipping welcome email",
                    customer_email, vertical, county_id, subscriber.id, subscriber.tier,
                )

    if subscriber is None:
        # Genuinely new subscriber
        is_new_subscriber = True
        # A buyer who checks out without ever hitting /api/free-signup first
        # (no pre-provisioned tier='free' row) still carries campaign metadata
        # on the Stripe session — record it as signup_source now so channel
        # attribution isn't silently lost to the "direct" default (fa### fix).
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
            signup_source="landing_page" if (meta.get("utm_source") or meta.get("campaign_id")) else "direct",
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

    # First paid conversion, matched by email — suppresses non-buyer nurture
    # and closes any in-flight abandoned-checkout recovery (Task 7). Each gets
    # its own savepoint: this runs before the ZIP-lock flush below, so a
    # failure here must not be able to poison that (see the module-level note
    # in the docstring above re: db.begin_nested() vs bare try/except).
    if customer_email:
        try:
            with db.begin_nested():
                from src.services import non_buyer_nurture
                non_buyer_nurture.mark_converted(db, customer_email)
        except Exception:
            logger.warning(
                "non_buyer_nurture mark_converted failed for subscriber=%s", subscriber.id, exc_info=True,
            )
        try:
            with db.begin_nested():
                from src.services import checkout_recovery
                checkout_recovery.mark_recovered(db, customer_email)
        except Exception:
            logger.warning(
                "checkout_recovery mark_recovered failed for email=%s", customer_email, exc_info=True,
            )

    db.flush()  # get subscriber.id before ZIP territory inserts

    if not subscriber.id:
        logger.warning(
            "checkout.session.completed: subscriber.id is None after flush for customer %s"
            " — ZIP locking may fail if DB did not assign PK",
            stripe_customer_id,
        )

    # ── Plan price + trial flags (fa048) + S1 plan resolution ────────────────
    # Consent linking and MRR/revenue activation live in the fast, committed
    # path (not the deferred BackgroundTasks half) because they must not be
    # lost: BackgroundTasks is in-process and non-durable — a deploy, worker
    # recycle, or crash between the webhook's ack and the background task
    # running means it never executes, and the webhook dedupe row has already
    # committed by then, so Stripe never retries to recover it. A missing
    # consent link undermines audit compliance; a missing Customer Account/MRR
    # row makes revenue reporting silently diverge from active subscribers.
    # Each still gets its own db.begin_nested() savepoint so a failure in one
    # can't poison the subscriber/ZIP-lock commit or each other.
    #
    # amount_total is in cents = the charge for this billing period. `plan_price`
    # is read as MONTHLY recurring revenue across the app, so an annual charge
    # (a full year prepaid up front, e.g. founder annual) must be normalized to a
    # monthly run-rate — otherwise it inflates MRR ~12x for every annual sub.
    _interval = (meta.get("interval") or "monthly").lower()
    _amount_total = session.get("amount_total") or 0
    if _amount_total > 0:
        subscriber.plan_price = normalized_monthly_price(_amount_total, _interval)

    # Trial/price detection AND revenue_engine's price-based plan resolution
    # below both need the subscription expanded with its price — fetch once,
    # reuse for both (this used to be two separate, identical Stripe calls).
    _sub_expanded = None
    if stripe_subscription_id:
        try:
            _sub_expanded = stripe.Subscription.retrieve(
                stripe_subscription_id, expand=["items.data.price"]
            )
        except Exception:
            logger.warning(
                "checkout: could not retrieve subscription %s for trial/price/plan resolution",
                stripe_subscription_id, exc_info=True,
            )

    if _sub_expanded is not None and _amount_total == 0:
        # Trial detection: Stripe sets amount_total=0 when trial_period_days > 0.
        try:
            with db.begin_nested():
                _items = (_sub_expanded.get("items") or {}).get("data") or []
                if _items:
                    _unit = (_items[0].get("price") or {}).get("unit_amount") or 0
                    if _unit:
                        subscriber.plan_price = normalized_monthly_price(_unit, _interval)
                _trial_end = _sub_expanded.get("trial_end")
                if _trial_end:
                    subscriber.is_trial = True
                    subscriber.trial_ends_at = datetime.fromtimestamp(_trial_end, tz=timezone.utc)
        except Exception:
            logger.warning(
                "checkout: trial/price extraction failed for subscription %s",
                stripe_subscription_id, exc_info=True,
            )

    # B0-06: link the checkout consent row (written pre-subscriber, no subscriber_id
    # yet) to the now-created subscriber, matched by this exact checkout session —
    # not email, which could also match an old abandoned-checkout/waitlist row for
    # the same address and wrongly hand its voice consent to this subscriber.
    # Idempotent — guarded on subscriber_id IS NULL so a replayed webhook never
    # re-touches an already-linked row.
    _checkout_session_id = session.get("id")
    if _checkout_session_id:
        try:
            with db.begin_nested():
                from sqlalchemy import text as _text
                db.execute(_text("""
                    UPDATE consent_acceptances SET subscriber_id = :sid
                    WHERE checkout_session_id = :session_id
                      AND source_flow = 'checkout'
                      AND subscriber_id IS NULL
                """), {"sid": subscriber.id, "session_id": _checkout_session_id})
        except Exception:
            logger.warning(
                "consent_acceptances subscriber_id link failed for session=%s (non-fatal)",
                _checkout_session_id, exc_info=True,
            )

    # ── B1/M9: activate the bridged Customer Account + record MRR ────────────
    # The only production entrypoint that seeds customer_accounts. Map the tier
    # to a plan; if the tier isn't in the catalog yet (legacy/founding), skip
    # activation rather than break checkout. Idempotent on the subscription id
    # so a stale-replayed checkout never double-counts MRR.
    try:
        with db.begin_nested():
            from src.services.revenue_engine import (
                get_or_create_account, plan_id_for_price, plan_id_for_tier,
                record_subscription_active,
            )
            # Resolve the plan by the subscription's price id first — the tier
            # alone is ambiguous when several plans share it (e.g.
            # founder_monthly and founder_annual both have tier='founder', so
            # a tier-only lookup would pick one arbitrarily and record the
            # wrong interval/MRR). Fall back to the tier when the price can't
            # be determined.
            _price_id = None
            if _sub_expanded is not None:
                _pitems = (_sub_expanded.get("items") or {}).get("data") or []
                if _pitems:
                    _price_id = (_pitems[0].get("price") or {}).get("id")
            plan_id = plan_id_for_price(db, _price_id) or plan_id_for_tier(db, tier)
            if plan_id is not None:
                account = get_or_create_account(
                    db, stripe_customer_id=stripe_customer_id, subscriber_id=subscriber.id,
                )
                record_subscription_active(
                    db, account,
                    plan_id=plan_id,
                    stripe_subscription_id=stripe_subscription_id,
                    current_period_end=None,
                    stripe_event_id=f"checkout:{stripe_subscription_id}" if stripe_subscription_id else None,
                    now=now,
                )
            else:
                logger.warning(
                    "checkout: no plan mapped for tier=%s — skipping S1 account activation "
                    "(customer=%s)", tier, stripe_customer_id,
                )
    except Exception:
        logger.error(
            "revenue_engine checkout activation failed for customer %s",
            stripe_customer_id, exc_info=True,
        )

    # ── T-B12-07: redeem a win-back offer token, if this checkout carried one ──
    # (PR #172 review fix) A completed checkout is the only point that proves
    # an actual reactivation happened — this is where the promised benefit is
    # finally realized. zip_held's 50%-off was already applied as a Stripe
    # coupon on the session itself (see /api/checkout); here we only mark the
    # token redeemed. zip_released never had a discount — its 5-credit grant
    # happens ONLY here, not at message-send time.
    #
    # redeem_offer() (sets redeemed_at) and grant_winback_credits() (sets
    # credits_granted_at) are intentionally NOT wrapped so that a grant
    # failure rolls back the redemption too — grant_winback_credits already
    # catches its own exceptions and returns False rather than raising, by
    # design, so it can never poison this savepoint (PR #172 follow-up
    # review). A failed grant instead leaves redeemed_at set and
    # credits_granted_at NULL, which reconcile_pending_credit_grants() (run
    # periodically, see scripts/cron/crontab.txt) finds and retries — so the
    # benefit is delayed, never lost, without needing to fail the whole
    # webhook or block Stripe's ack.
    _winback_token = meta.get("winback_token")
    if _winback_token:
        try:
            with db.begin_nested():
                from src.services.winback_offers import redeem_offer, grant_winback_credits
                redeemed = redeem_offer(_winback_token, db)
                if redeemed and redeemed["branch"] == "zip_released":
                    grant_winback_credits(redeemed["subscriber_id"], db, token=_winback_token)
        except Exception:
            logger.error(
                "winback offer redemption failed for token=%s customer=%s",
                _winback_token, stripe_customer_id, exc_info=True,
            )

    # ── Lock ZIP territories (same transaction) ────────────────────────────
    # A buyer paid for exclusive territory on every requested ZIP. If ANY of
    # them is lost to a concurrent checkout (the exact TOCTOU window
    # claim_zip_territory closes for a single ZIP, but two buyers can still
    # each win a subset of a multi-ZIP cart), the whole checkout must fail
    # rather than activate a paying subscriber who didn't get what they paid
    # for. Raising here propagates to handle_webhook's outer except, which
    # rolls back this entire transaction — no subscriber, no account
    # activation, no MRR record for this event.
    from src.services.zip_territory import ZipTerritoryUnavailableError, claim_zip_territory
    unclaimed = [
        zip_code for zip_code in zip_codes
        if not claim_zip_territory(
            db, zip_code=zip_code, vertical=vertical, county_id=county_id,
            subscriber_id=subscriber.id, now=now,
        )
    ]
    if unclaimed:
        # Stripe has ALREADY captured this charge and created the subscription
        # — the DB rollback below undoes our side only. That's not durably
        # recorded anywhere else (this same `db` session is what's about to
        # roll back, and the webhook audit row lives on it too), so ops would
        # otherwise have no reliable way to find this customer at all short of
        # grepping logs. Write the recovery record on its OWN committed
        # session — get_db_context() opens a fresh connection, independent of
        # `db` — so it survives regardless of what happens to this
        # transaction. Deciding HOW to recover (auto-refund vs. cancel vs.
        # manual outreach) is a product/finance policy call outside this
        # fix's scope; making the failure durable, queryable, and actionable
        # for ops is not.
        try:
            from src.core.database import get_db_context
            from src.core.models import CheckoutProvisioningFailure
            with get_db_context() as recovery_db:
                recovery_db.add(CheckoutProvisioningFailure(
                    stripe_customer_id=stripe_customer_id,
                    stripe_subscription_id=stripe_subscription_id,
                    email=customer_email,
                    tier=tier,
                    vertical=vertical,
                    county_id=county_id,
                    requested_zips=zip_codes,
                    unclaimed_zips=unclaimed,
                ))
        except Exception:
            logger.critical(
                "checkout provisioning failure AND its recovery record failed to write — "
                "customer=%s subscription=%s zips=%s — this customer is now findable only "
                "via log search, follow up manually",
                stripe_customer_id, stripe_subscription_id, unclaimed, exc_info=True,
            )
        raise ZipTerritoryUnavailableError(
            f"checkout for subscriber={subscriber.id} tier={tier} vertical={vertical} "
            f"county={county_id} could not claim ZIP(s) {unclaimed} — lost to a concurrent "
            f"checkout; entire checkout rolled back, durable recovery record written "
            f"to checkout_provisioning_failures"
        )

    # Bust zip_availability cache for every (county_id, vertical) pair that was locked.
    from src.core.redis_client import rdelete
    _seen_pairs: set = set()
    for _zip_code in zip_codes:
        _pair = (county_id, vertical)
        if _pair not in _seen_pairs:
            rdelete(f"zip_availability:{county_id}:{vertical}")
            _seen_pairs.add(_pair)

    logger.info(
        "checkout.session.completed: fast path done — subscriber=%s tier=%s vertical=%s"
        " founding=%s zips=%s feed_uuid=%s (deferring GHL/email/CAPI/attribution work)",
        subscriber.id, tier, vertical, is_founding,
        zip_codes, subscriber.event_feed_uuid,
    )

    if background_tasks is not None:
        background_tasks.add_task(
            _run_checkout_completed_deferred, subscriber.id, session, is_new_subscriber,
        )
    else:
        # No BackgroundTasks available (direct call — script/test/replay tool).
        # Run on the SAME session, inline, synchronously: matches this
        # function's behavior before the fast/background split.
        _checkout_completed_deferred(db, subscriber, session, is_new_subscriber)


def _run_checkout_completed_deferred(subscriber_id: int, session: dict, is_new_subscriber: bool) -> None:
    """
    FastAPI BackgroundTasks entry point for the deferred half of
    _on_checkout_completed(). Runs strictly after the HTTP response to Stripe
    has already been sent (Starlette guarantees background tasks run after
    the response is transmitted), so it needs its OWN fresh DB session — the
    request's session is closed by the time this executes, and by now the
    fast path's subscriber tier/ZIP-lock update is durably committed and
    visible to this new session.

    Re-fetches the subscriber by id rather than reusing the ORM object built
    in the request's (now-closed) session — that object belongs to a session
    that no longer exists.

    One-shot, best-effort, no retry — see the docstring on
    _on_checkout_completed for why that's an acceptable, unchanged trade-off
    for everything handled here.
    """
    from src.core.database import get_db_context

    try:
        with get_db_context() as db:
            subscriber = db.execute(
                select(Subscriber).where(Subscriber.id == subscriber_id)
            ).scalar_one_or_none()
            if subscriber is None:
                logger.error(
                    "checkout deferred: subscriber %s not found — cannot run deferred"
                    " side-effects (fast path should already have created/updated this row)",
                    subscriber_id,
                )
                return
            _checkout_completed_deferred(db, subscriber, session, is_new_subscriber)
    except Exception:
        # Final safety net: nothing inside _checkout_completed_deferred should
        # escape uncaught (every block below has its own try/except or
        # begin_nested savepoint), but this background task has no caller to
        # report to if one does — log it clearly instead of letting it surface
        # as a bare unhandled-exception trace in the server's background-task
        # runner.
        logger.error(
            "checkout deferred: unhandled failure for subscriber %s", subscriber_id, exc_info=True,
        )


def _checkout_completed_deferred(db: Session, subscriber, session: dict, is_new_subscriber: bool) -> None:
    """
    Core deferred logic for checkout.session.completed — everything that is
    NOT required for the subscriber to see their tier/ZIP upgrade, which the
    fast path (_on_checkout_completed) already committed before this ever
    runs. Shared by both the real background-task path
    (_run_checkout_completed_deferred, fresh session) and the inline fallback
    _on_checkout_completed uses when called without `background_tasks` (same
    session — scripts/tests/admin replay).

    Covers: saved-card detection, TCPA opt-in, GHL sync, welcome/upgrade +
    first-leads + founder-alert emails, partner-tier provisioning, referral
    confirmation, segmentation, attribution, A/B holdout, Meta CAPI, campaign
    attribution, affiliate confirm, subscriber-memory projection.

    Trial/price detection, S1 plan resolution, consent-row linking, and
    Customer Account/MRR activation are NOT here — they moved to the fast,
    committed path in _on_checkout_completed. BackgroundTasks is in-process
    and non-durable (lost on a deploy/crash/restart between the webhook ack
    and this running, with no retry since the dedupe row already committed),
    which is an acceptable trade-off for the marketing/analytics work below
    but not for consent audit records or revenue accounting.
    """
    meta = session.get("metadata", {}) or {}
    tier        = meta.get("tier")
    vertical    = meta.get("vertical")
    county_id   = meta.get("county_id")
    zip_codes   = [z.strip() for z in meta.get("zip_codes", "").split(",") if z.strip()]
    is_founding = meta.get("is_founding") == "True"
    dashboard_upgrade = meta.get("dashboard_upgrade") == "True"

    stripe_customer_id     = session.get("customer")
    stripe_subscription_id = session.get("subscription")
    _raw_email             = session.get("customer_details", {}).get("email") or ""
    customer_email         = _raw_email.lower().strip() or None
    customer_phone         = (session.get("customer_details", {}) or {}).get("phone") or None
    now = datetime.now(timezone.utc)

    # ── Saved-card flag (fa016 followup #20) ─────────────────────────────────
    # Read default_payment_method from the session's payment_intent
    # (synchronous, in-payload) — fall back to a Customer.retrieve / PM list
    # only if the inline path is missing.
    if not subscriber.has_saved_card:
        pm_id = None
        pi_block = session.get("payment_intent") or {}
        if isinstance(pi_block, dict):
            pm_id = pi_block.get("payment_method")
        if not pm_id:
            sd = (session.get("parent") or {}).get("subscription_details") or {}
            if isinstance(sd, dict):
                pm_id = sd.get("default_payment_method")
        if not pm_id and stripe_customer_id:
            try:
                cust = stripe.Customer.retrieve(stripe_customer_id)
                pm_id = (cust.get("invoice_settings") or {}).get("default_payment_method")
            except Exception as exc:
                logger.warning(
                    "checkout: customer retrieve failed for %s: %s",
                    stripe_customer_id, exc,
                )
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
            try:
                with db.begin_nested():
                    subscriber.has_saved_card = True
                    subscriber.stripe_payment_method_id = pm_id
                logger.info(
                    "checkout: saved-card flag set (deferred) — subscriber=%s pm=%s",
                    subscriber.id, pm_id,
                )
                try:
                    with db.begin_nested():
                        from src.services import wallet_engine
                        eligible = wallet_engine.accelerated_push_eligible(subscriber.id, db)
                        if eligible:
                            from src.agents.events.ingestion import publish_lifecycle_event
                            publish_lifecycle_event({
                                "event_type": "accelerated_wallet_push_eligible",
                                "subscriber_id": subscriber.id,
                                "payload": eligible,
                            })
                except Exception as exc:
                    logger.warning(
                        "accelerated_wallet_push from checkout deferred failed sub=%s: %s",
                        subscriber.id, exc,
                    )
            except Exception as exc:
                logger.warning(
                    "checkout: saved-card flag update failed sub=%s: %s", subscriber.id, exc,
                )

    # ── TCPA opt-in record ─────────────────────────────────────────────────
    # Stripe Checkout's phone collection field is presented next to the
    # subscription-purchase consent on the same form. Treat completion as
    # an opt-in to operational SMS (BALANCE / WALLET / etc) and the
    # accelerated-wallet-push offer. Only insert if we have a phone AND
    # no opt-in row exists yet for this subscriber.
    if customer_phone and subscriber.id:
        try:
            with db.begin_nested():
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
                    logger.info(
                        "SmsOptIn created from Stripe Checkout: subscriber=%s phone=%s",
                        subscriber.id, customer_phone,
                    )
        except Exception as exc:
            logger.warning("SmsOptIn insert failed for subscriber=%s: %s", subscriber.id, exc)

    # ── Push to GHL stage 5 ────────────────────────────────────────────────
    try:
        with db.begin_nested():
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

    # ── Welcome/upgrade email + first leads (new subscribers only) ────────
    # Skipped when we merged onto an existing row — subscriber is already onboarded.
    if is_new_subscriber:
        if subscriber.email and dashboard_upgrade:
            # Already has dashboard access (e.g. free-tier subscriber upgrading
            # from their own dashboard) — no magic link needed, just confirm
            # the plan change.
            try:
                from src.services.email import send_upgrade_confirmation_email
                send_upgrade_confirmation_email(subscriber)
            except Exception:
                logger.error(
                    "Upgrade confirmation email failed for subscriber %s", subscriber.id, exc_info=True,
                )
        elif subscriber.email:
            from src.services.email import send_welcome_email
            from src.services import subscriber_auth
            # Magic-link login — issue a fresh one-time link for the welcome
            # email. No password is ever generated or emailed.
            magic_url = None
            try:
                with db.begin_nested():
                    raw = subscriber_auth.issue_magic_link(subscriber, db)
                magic_url = subscriber_auth.magic_link_url(raw)
            except Exception:
                magic_url = None
                logger.warning(
                    "Magic-link issuance failed for subscriber %s — sending welcome without it",
                    subscriber.id, exc_info=True,
                )
            try:
                send_welcome_email(subscriber, magic_link_url=magic_url)
            except Exception:
                logger.error("Welcome email failed for subscriber %s", subscriber.id, exc_info=True)

        if subscriber.email and zip_codes:
            try:
                _send_first_leads_email(subscriber, zip_codes, db)
            except Exception:
                logger.error(
                    "First leads email failed for subscriber %s — non-critical",
                    subscriber.id, exc_info=True,
                )

        # ── Speed-to-lead: instantly alert the founder of the new signup ────
        try:
            from src.services.owner_alert import notify_owner
            notify_owner(
                subject=f"New subscriber — {subscriber.tier} {vertical}",
                body=(
                    f"New subscriber signed up.\nTier: {subscriber.tier}\nVertical: {vertical}\n"
                    f"County: {county_id}\nZIPs: {', '.join(zip_codes)}\nEmail: {subscriber.email}"
                ),
                idempotency_key=f"stripe:{session.get('id', '')}",
            )
        except Exception:
            logger.error(
                "Founder alert notify_owner failed for subscriber %s", subscriber.id, exc_info=True,
            )

        # Stage 12 — schedule the bankruptcy-alert invite (sent T+X min by the
        # invite sweep). Best-effort; never breaks checkout processing.
        try:
            with db.begin_nested():
                from src.services.bankruptcy_alert.invite import schedule_invite
                schedule_invite(db, subscriber.id)
        except Exception:
            logger.warning(
                "Bankruptcy invite scheduling failed for subscriber %s — non-critical",
                subscriber.id, exc_info=True,
            )

    # ── Partner tier: provision multi-ZIP access ──────────────────────────
    # When a subscriber upgrades to the partner tier via checkout, we need to
    # lock all their chosen ZIPs and create the PartnerSubscription audit row.
    # The fast path's ZIP locking loop already handles individual ZIPs; this
    # call sets the tier and creates the PartnerSubscription record.
    if tier == "partner" and zip_codes:
        try:
            with db.begin_nested():
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
    # anything after it.
    # Each of these post-activation side-effects gets its own SAVEPOINT
    # (db.begin_nested()) rather than a bare try/except. A bare except here
    # only stops the *Python* exception from propagating — it does nothing
    # about the DB session, which Postgres leaves in "current transaction is
    # aborted" state after any failed statement. Every subsequent db.execute()
    # on that same session then fails too, cascading into unrelated "non-fatal"
    # warnings below. begin_nested() rolls back only to the savepoint on
    # failure, leaving everything before it intact.
    try:
        with db.begin_nested():
            from src.services.referral_engine import confirm_purchase
            event = confirm_purchase(subscriber.id, db)
            if event is not None:
                logger.info(
                    "[Referral] confirmed: referee=%d event=%d",
                    subscriber.id, event.id,
                )
                try:
                    with db.begin_nested():
                        from src.services.referral_prompt_service import mark_confirmed
                        mark_confirmed(
                            event.referrer_subscriber_id, event.id, db,
                            prompt_funnel_id=getattr(event, "prompt_funnel_id", None),
                        )
                except Exception:
                    logger.warning(
                        "[ReferralPrompt] funnel confirm advance failed for event=%d — non-fatal",
                        event.id, exc_info=True,
                    )
    except Exception:
        logger.error(
            "[Referral] confirm/reward failed for subscriber %d — non-fatal",
            subscriber.id, exc_info=True,
        )

    # Segmentation is post-activation analytics — never let it abort the handler
    # before the revenue-attribution + Meta CAPI steps below.
    try:
        with db.begin_nested():
            from src.services.segmentation_engine import reclassify_safe
            from src.services.revenue_signal import ACTION_CHECKOUT_COMPLETED
            reclassify_safe(subscriber.id, db, action_type=ACTION_CHECKOUT_COMPLETED)
    except Exception:
        logger.warning("checkout: reclassify failed sub=%s — non-fatal", subscriber.id, exc_info=True)

    try:
        with db.begin_nested():
            from src.services.attribution_service import record_conversion_attribution
            _tier_conv = {
                "annual_lock":    "annual_upgrade",
                "autopilot_lite": "autopilot_lite_upgrade",
                "autopilot_pro":  "autopilot_pro_upgrade",
            }
            record_conversion_attribution(
                conversion_type=_tier_conv.get(tier, "territory_lock_purchase"),
                source_table="checkout_sessions",
                source_event_id=session.get("id", ""),
                subscriber_id=subscriber.id,
                occurred_at=now,
                zip_code=zip_codes[0] if zip_codes else None,
                revenue_amount=round((session.get("amount_total") or 0) / 100, 2),
                db=db,
            )
    except Exception:
        logger.warning("Attribution recording failed sub=%s", subscriber.id, exc_info=True)

    # Task 4.1 frozen control holdout — a completed checkout is the
    # conversion event for the lock_close_v1 sequence. No-op for any
    # subscriber without a lock_close_holdout AbAssignment (anyone the
    # wallet_to_lock_close graph never nudged), so this fires safely across
    # every tier this handler processes, not just ZIP lock upgrades —
    # matching the per-tier (not per-message) granularity already used
    # above for attribution/Meta CAPI.
    try:
        with db.begin_nested():
            from src.services.ab_engine import record_holdout_conversion
            record_holdout_conversion(subscriber.id, "lock_close_holdout", db)
    except Exception:
        logger.warning("checkout: holdout conversion record failed sub=%s — non-fatal", subscriber.id, exc_info=True)

    # ── Meta CAPI (S2): report server-side Purchase ──────────────────────────
    # Observer only — runs after the subscriber is active and ZIPs are locked.
    # Stamps campaign fields onto the subscriber when they're still NULL
    # (never overwriting free-signup attribution), then fires the Purchase
    # event. Feature-gated and fully isolated: a missing/failed Meta call must
    # never affect anything else here.
    try:
        with db.begin_nested():
            _stamp_campaign_fields(subscriber, meta, db)
        from src.services.meta_capi_service import fire_purchase_event
        fire_purchase_event(
            subscriber=subscriber,
            amount=round((session.get("amount_total") or 0) / 100, 2),
            source="subscription",
            request_context={
                "buyer_ip": meta.get("buyer_ip"),
                "buyer_user_agent": meta.get("buyer_user_agent"),
                "fbclid": meta.get("fbclid"),
                "utm_campaign": meta.get("utm_campaign"),
                "campaign_id": meta.get("campaign_id"),
                "currency": (session.get("currency") or "usd").upper(),
            },
            event_id=f"sub_{session.get('id', '')}",
        )
    except Exception:
        logger.warning("Meta CAPI subscription purchase failed sub=%s — non-fatal", subscriber.id, exc_info=True)

    # Campaign conversion attribution (B6)
    try:
        with db.begin_nested():
            from src.services.campaign_attribution import (
                decode_attribution_token,
                record_conversion,
                try_email_fallback,
            )
            campaign_token = meta.get("campaign_attribution_token")
            attributed = False
            if campaign_token:
                cc_id = decode_attribution_token(campaign_token)
                if cc_id:
                    attributed = record_conversion(
                        db=db,
                        campaign_contact_id=cc_id,
                        subscriber_id=subscriber.id,
                        signed_up_at=now,
                    )
            if not attributed and customer_email:
                attributed = try_email_fallback(db=db, email=customer_email, subscriber_id=subscriber.id, signed_up_at=now)
            # Stamp signup_source if this was an email campaign signup. Subscriber
            # has no `acquisition_source` column (that field only exists on
            # CustomerAccount) — the previous write here was silently discarded
            # by SQLAlchemy as a transient attribute. First-touch: only upgrades
            # a still-unattributed subscriber, matching signup_engine's rule.
            if attributed and (subscriber.signup_source or "").strip().lower() in ("", "direct", "unknown"):
                subscriber.signup_source = "dbpr_email"
                db.add(subscriber)
    except Exception:
        logger.warning("Campaign attribution failed sub=%s", subscriber.id, exc_info=True)

    # Affiliate referral: a paid checkout confirms a pending Affiliate Referral.
    try:
        with db.begin_nested():
            from src.services.affiliate_engine import confirm_referral
            confirm_referral(db, subscriber.id)
    except Exception:
        logger.warning("Affiliate confirm failed sub=%s", subscriber.id, exc_info=True)

    try:
        with db.begin_nested():
            from src.services.subscriber_memory import append_memory_event

            occurred_at = now
            if session.get("created"):
                try:
                    occurred_at = datetime.fromtimestamp(session["created"], tz=timezone.utc)
                except Exception:
                    occurred_at = now

            append_memory_event(
                db,
                subscriber_id=subscriber.id,
                stream_source="STRIPE",
                event_type="checkout_completed",
                source_event_id=session.get("id") or f"checkout:{stripe_customer_id}",
                source_event_name="checkout.session.completed",
                occurred_at=occurred_at,
                status="completed",
                summary=f"Subscriber completed checkout for {tier} plan",
                channel="stripe",
                actor={"type": "system", "id": "stripe"},
                raw={
                    "stripe_customer_id": stripe_customer_id,
                    "stripe_subscription_id": stripe_subscription_id,
                    "tier": tier,
                    "vertical": vertical,
                    "county_id": county_id,
                },
            )
    except Exception:
        logger.warning(
            "Subscriber memory projection failed for checkout sub=%s",
            subscriber.id,
            exc_info=True,
        )

    logger.info(
        "checkout.session.completed: deferred work finished — subscriber=%s tier=%s vertical=%s",
        subscriber.id, tier, vertical,
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
    had_failed_payment = subscriber.payment_failed_at is not None
    subscriber.payment_failed_at = None
    subscriber.recovery_day1_sent = False
    subscriber.recovery_day3_sent = False

    # B1/M9: a cleared invoice restores a past_due Customer Account to active.
    try:
        from src.services.revenue_engine import account_by_stripe_customer, record_recovery
        account = account_by_stripe_customer(db, stripe_customer_id)
        if account is not None:
            record_recovery(db, account)
    except Exception:
        logger.error(
            "revenue_engine recovery mirror failed for customer %s",
            stripe_customer_id, exc_info=True,
        )

    logger.info(
        "invoice.payment_succeeded: subscriber=%s billing_date=%s",
        subscriber.id, subscriber.billing_date,
    )

    try:
        from src.services.subscriber_memory import append_memory_event

        append_memory_event(
            db,
            subscriber_id=subscriber.id,
            stream_source="STRIPE",
            event_type="subscription_activated",
            source_event_id=invoice.get("id") or f"payment_succeeded:{stripe_customer_id}",
            source_event_name="invoice.payment_succeeded",
            occurred_at=datetime.now(timezone.utc),
            status="active",
            summary="Subscriber subscription active",
            channel="stripe",
            actor={"type": "system", "id": "stripe"},
            raw={
                "stripe_customer_id": stripe_customer_id,
                "stripe_invoice_id": invoice.get("id"),
                "billing_reason": billing_reason,
            },
        )
    except Exception:
        logger.warning(
            "Subscriber memory projection failed for payment_succeeded sub=%s",
            subscriber.id,
            exc_info=True,
        )

    # Affiliate program: record collected subscription revenue (source of truth
    # for Commission). One-time charges are ignored by the engine.
    try:
        from src.services.affiliate_engine import record_subscription_invoice
        line = invoice.get("lines", {}).get("data", [{}])[0]
        period_start = line.get("period", {}).get("start")
        paid_ts = invoice.get("status_transitions", {}).get("paid_at") or period_start
        if invoice.get("id") and period_start and paid_ts:
            recorded_invoice = record_subscription_invoice(
                db,
                stripe_invoice_id=invoice["id"],
                subscriber_id=subscriber.id,
                amount_collected_cents=int(invoice.get("amount_paid") or 0),
                period_month=datetime.fromtimestamp(period_start, tz=timezone.utc).date().replace(day=1),
                paid_at=datetime.fromtimestamp(paid_ts, tz=timezone.utc),
                is_subscription=bool(invoice.get("subscription"))
                or billing_reason in ("subscription_create", "subscription_cycle", "subscription_update"),
                # Store the PaymentIntent so refunds map back even when the
                # Stripe API version nulls charge.invoice (2026-02-25+).
                payment_intent_id=invoice.get("payment_intent")
                or _resolve_invoice_payment_intent(invoice["id"]),
            )
            # Centralized ledger (src/services/revenue_ledger.py) — additive,
            # subscription_invoices remains the affiliate system's own source
            # of truth untouched. record_revenue is idempotent on
            # (source_table, source_id), so calling it every time this
            # webhook fires (including retries of an already-recorded
            # invoice) is safe.
            if recorded_invoice is not None and recorded_invoice.amount_collected_cents:
                from src.services.revenue_ledger import record_revenue
                record_revenue(
                    db, subscriber_id=subscriber.id, product_type="subscription",
                    amount_cents=recorded_invoice.amount_collected_cents,
                    source_table="subscription_invoices", source_id=recorded_invoice.id,
                    occurred_at=recorded_invoice.paid_at,
                )
    except Exception:
        logger.warning("Affiliate invoice capture failed sub=%s", subscriber.id, exc_info=True)

    if had_failed_payment:
        try:
            from src.services.attribution_service import record_conversion_attribution
            record_conversion_attribution(
                conversion_type="failed_payment_recovered",
                source_table="stripe_invoices",
                source_event_id=invoice.get("id", ""),
                subscriber_id=subscriber.id,
                occurred_at=datetime.now(timezone.utc),
                db=db,
            )
        except Exception:
            logger.warning("Attribution recording failed sub=%s", subscriber.id, exc_info=True)

    # Funnel analytics: log a rebill event for standard subscription renewals
    # only — billing_reason values other than subscription_cycle (e.g.
    # subscription_update on a plan change, subscription_threshold, manual)
    # are not genuine renewals and must not inflate the "rebilled" count.
    # Wallet-subscription renewals go through _on_wallet_subscription_invoice
    # above and are not covered here.
    if billing_reason == "subscription_cycle":
        invoice_id = invoice.get("id")
        try:
            from src.services.business_events import log_business_event
            from src.services.webhook_log import already_logged
            # Guards against the multi-worker race documented above this
            # handler's dedupe check: two workers can both pass the Stripe
            # event-level dedupe and run this handler before either commits
            # the dedupe row, and this audit write has no unique constraint
            # of its own — so key it to the invoice id explicitly.
            if invoice_id and already_logged("business", invoice_id):
                logger.info(
                    "SUBSCRIPTION_RENEWED already logged for invoice=%s sub=%s — skipping",
                    invoice_id, subscriber.id,
                )
            else:
                log_business_event(
                    "SUBSCRIPTION_RENEWED",
                    subscriber_id=subscriber.id,
                    payload={"invoice_id": invoice_id, "billing_reason": billing_reason},
                    source_event_id=invoice_id,
                    db=db,
                )
        except Exception:
            logger.warning("SUBSCRIPTION_RENEWED business event failed sub=%s", subscriber.id, exc_info=True)

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

    from src.services.segmentation_engine import reclassify_safe
    from src.services.revenue_signal import ACTION_INVOICE_PAID
    reclassify_safe(subscriber.id, db, action_type=ACTION_INVOICE_PAID)


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

    # B1/M9: mark the bridged Customer Account past_due. Do NOT change
    # subscriber.status — 'grace' is the cancellation state and would forfeit
    # the ZIP. Defensive: never let the S1 mirror break the legacy path.
    try:
        from src.services.revenue_engine import account_by_stripe_customer, record_past_due
        account = account_by_stripe_customer(db, stripe_customer_id)
        if account is not None:
            record_past_due(db, account)
    except Exception:
        logger.error(
            "revenue_engine past_due mirror failed for customer %s",
            stripe_customer_id, exc_info=True,
        )

    try:
        push_subscriber_to_ghl(subscriber, stage=None, tags=["payment_failed"])
    except Exception:
        logger.error(
            "GHL payment-failed tag push error for subscriber %s",
            subscriber.id,
            exc_info=True,
        )

    try:
        from src.services.subscriber_memory import append_memory_event

        append_memory_event(
            db,
            subscriber_id=subscriber.id,
            stream_source="STRIPE",
            event_type="payment_failed",
            source_event_id=invoice.get("id") or f"payment_failed:{stripe_customer_id}",
            source_event_name="invoice.payment_failed",
            occurred_at=datetime.now(timezone.utc),
            status="failed",
            summary="Subscriber payment failed",
            channel="stripe",
            actor={"type": "system", "id": "stripe"},
            raw={
                "stripe_customer_id": stripe_customer_id,
                "stripe_invoice_id": invoice.get("id"),
            },
        )
    except Exception:
        logger.warning(
            "Subscriber memory projection failed for payment_failed sub=%s",
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
        "past_due": "past_due",
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
    old_status = subscriber.status
    subscriber.status = new_status
    subscriber.stripe_subscription_id = subscription.get("id", subscriber.stripe_subscription_id)

    logger.info(
        "subscription.updated: subscriber=%s stripe_status=%s → local_status=%s cancel_at_period_end=%s",
        subscriber.id, stripe_status, new_status, cancel_at_period_end,
    )

    # ── B1/M9: record plan-change MRR movement (expansion / contraction) ─────
    # Only when the subscription is active and the new price maps to a known
    # plan. record_subscription_active recomputes the run-rate and writes a
    # movement only if it actually changed (a no-op plan touch records nothing).
    # Idempotent on (subscription, run-rate) so replays don't double-count.
    if stripe_status == "active":
        try:
            from src.services.revenue_engine import (
                get_or_create_account, plan_id_for_price, record_subscription_active,
            )
            items = (subscription.get("items") or {}).get("data") or []
            price_id = (items[0].get("price") or {}).get("id") if items else None
            plan_id = plan_id_for_price(db, price_id)
            if plan_id is not None:
                account = get_or_create_account(
                    db, stripe_customer_id=stripe_customer_id, subscriber_id=subscriber.id,
                )
                sub_id = subscription.get("id")
                record_subscription_active(
                    db, account,
                    plan_id=plan_id,
                    stripe_subscription_id=sub_id,
                    current_period_end=None,
                    stripe_event_id=f"subupd:{sub_id}:{plan_id}" if sub_id else None,
                )
        except Exception:
            logger.error(
                "revenue_engine subscription.updated movement failed for customer %s",
                stripe_customer_id, exc_info=True,
            )
    # Send past_due notification on first transition into past_due
    if new_status == "past_due" and old_status != "past_due" and subscriber.email:
        from src.services.email import send_email
        from config.settings import get_settings
        import datetime as _dt
        _settings = get_settings()
        name = subscriber.name or "there"
        tier = (subscriber.tier or "starter").title()
        feed_url = (
            f"{_settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
            if subscriber.event_feed_uuid else _settings.app_base_url
        )
        founding_html = (
            '<p style="margin:0 0 16px;padding:10px 16px;background:#451a03;'
            'border:1px solid #92400e;border-radius:8px;color:#fbbf24;font-size:14px;">'
            "⭐ Founding Member — your locked rate will be permanently lost if your subscription lapses."
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
            <p style="margin:0 0 24px;padding:12px 16px;background:#450a0a;border:1px solid #7f1d1d;
                      border-radius:8px;color:#fca5a5;font-size:14px;font-weight:600;">
              ⚠️ &nbsp;Your subscription is past due
            </p>
            <h1 style="margin:0 0 8px;font-size:24px;font-weight:800;color:#ffffff;">
              Payment still outstanding, {name}.
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              Your <strong style="color:#ffffff;">{tier}</strong> subscription has entered past due status.
              Stripe is automatically retrying your payment — you don't need to do anything if your card is valid.
            </p>
            {founding_html}
            <p style="margin:0 0 8px;font-size:14px;color:#94a3b8;font-weight:600;">What happens next:</p>
            <ul style="margin:0 0 24px;padding-left:20px;color:#94a3b8;font-size:14px;line-height:1.7;">
              <li>Stripe will retry your payment over the next several days.</li>
              <li>You keep full platform access during the retry window.</li>
              <li>If all retries fail, your subscription will be cancelled and territory locks released.</li>
            </ul>
            <p style="margin:0 0 20px;font-size:14px;color:#94a3b8;">
              To resolve this now, update your payment method:
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
        body_text = (
            f"Hi {name},\n\n"
            f"Your Forced Action {tier} subscription is now past due.\n\n"
            f"Stripe is automatically retrying your payment. You keep full access during the retry window.\n\n"
            f"If all retries fail, your subscription will be cancelled and your territory locks released.\n\n"
            f"To resolve this now, update your payment method:\n{feed_url}\n\n"
            f"Questions? support@forcedaction.io\n\n"
            f"— Forced Action Team"
        )
        send_email(
            to=subscriber.email,
            subject="Your Forced Action subscription is past due",
            body_text=body_text,
            body_html=body_html,
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

    # B1/M9: churn the bridged Customer Account (status -> churned, mrr -> 0,
    # churn movement). is_involuntary is derived from the account's past_due state.
    try:
        from src.services.revenue_engine import account_by_stripe_customer, record_churn
        account = account_by_stripe_customer(db, stripe_customer_id)
        if account is not None:
            sub_id = subscription.get("id")
            record_churn(
                db, account,
                stripe_event_id=f"subdel:{sub_id}" if sub_id else None,
                effective_at=now,
            )
    except Exception:
        logger.error(
            "revenue_engine churn mirror failed for customer %s",
            stripe_customer_id, exc_info=True,
        )

    # Set ZIP territories to grace — they remain locked for 48hr
    territories = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.subscriber_id == subscriber.id,
            ZipTerritory.status == "locked",
        )
    ).scalars().all()

    from src.core.redis_client import rdelete as _rdelete
    for territory in territories:
        territory.status = "grace"
        territory.grace_expires_at = grace_expires
        _rdelete(f"zip_availability:{territory.county_id}:{territory.vertical}")

    churn_tag = "churned_founding" if subscriber.founding_member else "churned_regular"

    try:
        push_subscriber_to_ghl(subscriber, stage=7, tags=[churn_tag])
    except Exception:
        logger.error(
            "GHL stage 7 push failed for subscriber %s",
            subscriber.id,
            exc_info=True,
        )

    try:
        from src.services.subscriber_memory import append_memory_event
        append_memory_event(
            db,
            subscriber_id=subscriber.id,
            stream_source="STRIPE",
            event_type="subscription_canceled",
            source_event_id=subscription.get("id") or f"subscription_deleted:{stripe_customer_id}",
            source_event_name="customer.subscription.deleted",
            occurred_at=now,
            status="canceled",
            summary="Subscriber subscription canceled",
            channel="stripe",
            actor={"type": "system", "id": "stripe"},
            raw={
                "stripe_customer_id": stripe_customer_id,
                "stripe_subscription_id": subscription.get("id"),
                "grace_expires_at": grace_expires.isoformat(),
            },
        )
    except Exception:
        logger.warning(
            "Subscriber memory projection failed for subscription_deleted sub=%s",
            subscriber.id,
            exc_info=True,
        )

    logger.info(
        "subscription.deleted: subscriber=%s founding=%s tag=%s"
        " grace_expires=%s zips_in_grace=%d",
        subscriber.id, subscriber.founding_member, churn_tag,
        grace_expires.isoformat(), len(territories),
    )

    # fa037 — Revenue Signal Score: capture the churn as a significant
    # action so the score drops (engagement_recency cools) and a clean
    # audit row lands in revenue_signal_score_events. Wrapped in try so a
    # score-write failure cannot block the grace/ZIP/GHL side effects above.
    try:
        from src.services.segmentation_engine import reclassify_safe
        from src.services.revenue_signal import ACTION_SUBSCRIPTION_DELETED
        reclassify_safe(
            subscriber.id, db,
            action_type=ACTION_SUBSCRIPTION_DELETED,
            metadata={"churn_tag": churn_tag, "founding": bool(subscriber.founding_member)},
        )
    except Exception:
        logger.warning(
            "subscription.deleted: revenue signal update failed for sub=%s",
            subscriber.id, exc_info=True,
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
    elif product == "hot_lead_unlock":
        # $150 ($99 reduced) single-lead unlock sold via Checkout Session —
        # same deliverable as lead_unlock (reveal one lead's contact details),
        # so it shares the fulfillment handler. Metadata arrives on the PI via
        # payment_intent_data set in create_hot_lead_unlock_link.
        logger.info("[PI] routing -> hot_lead_unlock pi=%s", pi_id)
        _on_lead_unlock_payment(payment_intent, db)
    else:
        logger.info("[PI] routing -> card_save (no product metadata) pi=%s", pi_id)
        _on_card_saved(payment_intent, db)

    # ── Speed-to-lead: instantly alert the founder of the purchase ─────────
    # Guarded on `product` — the card_save branch above has no product metadata
    # (e.g. a saved-card/setup event, not a purchase) and must not page Josh.
    if product:
        from src.services.owner_alert import notify_owner
        notify_owner(
            subject=f"Purchase — {product}",
            body=f"Product: {product}\nAmount: ${(amount or 0) / 100:.2f}\nPI: {pi_id}\nCustomer: {customer_id}",
        )

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
            try:
                with db.begin_nested():
                    from src.services.referral_prompt_service import mark_confirmed
                    mark_confirmed(
                        event.referrer_subscriber_id, event.id, db,
                        prompt_funnel_id=getattr(event, "prompt_funnel_id", None),
                    )
            except Exception:
                logger.warning(
                    "[ReferralPrompt] funnel confirm advance failed for event=%d — non-fatal",
                    event.id, exc_info=True,
                )
    except Exception as exc:
        logger.error(
            "[Referral] PI-path confirm failed for subscriber %s — non-fatal: %s",
            subscriber_id, exc, exc_info=True,
        )

    from src.services.segmentation_engine import reclassify_safe
    from src.services.revenue_signal import ACTION_PAYMENT_INTENT_SUCCEEDED
    reclassify_safe(subscriber_id, db, action_type=ACTION_PAYMENT_INTENT_SUCCEEDED)


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


def fulfill_founder_comp_reveal(subscriber, property_id_raw, db: Session) -> bool:
    """Reveal a lead to a founder at $0 — the hot-lead-unlock waiver (ADR 0037).

    Same deliverable as a paid hot-lead unlock (SentLead audit row, Auto Mode
    enqueue, full-details email) but with NO Stripe charge and NO revenue-ledger
    entry. The SentLead source marks it a founder comp so revenue reporting and
    dedupe both stay correct. Idempotent per (subscriber, property). Returns True
    when the reveal was fulfilled, False on a bad property id.
    """
    from src.core.models import Property, Owner, DistressScore, EnrichedContact, SentLead

    try:
        property_id = int(property_id_raw)
    except (TypeError, ValueError):
        logger.warning("founder_comp reveal: non-int property_id=%r", property_id_raw)
        return False

    prop = db.get(Property, property_id)
    if not prop:
        logger.warning("founder_comp reveal: property %s not found", property_id)
        return False

    score = db.execute(
        select(DistressScore).where(DistressScore.property_id == property_id)
        .order_by(DistressScore.score_date.desc()).limit(1)
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

    # First-touch delivery (Auto Mode enqueue, full-details email) must fire
    # exactly once per (subscriber, property). SentLead has no unique
    # constraint on (subscriber_id, property_id) — other sources (daily_email,
    # lead_unlock) legitimately insert multiple rows for the same pair over
    # time — so a plain check-then-insert can't rely on a constraint to
    # resolve a race and isn't atomic on its own (two concurrent requests can
    # both pass the "not exists" check before either commits). A Postgres
    # transaction-scoped advisory lock serializes concurrent callers for the
    # same (subscriber, property) without a schema change: the second caller
    # blocks until the first's transaction ends, then sees the row the first
    # caller already committed and skips the side effects below
    # (PR #163 review comment 3).
    lock_key = f"founder_comp_reveal:{subscriber.id}:{property_id}"
    db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": lock_key})

    existing = db.execute(
        select(SentLead).where(
            SentLead.subscriber_id == subscriber.id,
            SentLead.property_id == property_id,
            SentLead.source == "founder_comp_reveal",
        )
    ).scalar_one_or_none()
    is_first_reveal = existing is None

    if is_first_reveal:
        try:
            with db.begin_nested():
                db.add(SentLead(
                    subscriber_id=subscriber.id,
                    property_id=property_id,
                    source="founder_comp_reveal",
                ))
                db.flush()
        except (IntegrityError, OperationalError) as exc:
            logger.warning("founder_comp reveal: SentLead insert failed: %s", exc)
    else:
        logger.info("founder_comp reveal: already delivered sub=%s prop=%s",
                    subscriber.id, property_id)

    if not is_first_reveal:
        return True

    # T-B12-05: founder comp reveal is a $0 unlock but still IS the
    # activation event (first contact reveal), so it must stamp the clock.
    try:
        from src.services.activation_tracking import stamp_first_unlock
        stamp_first_unlock(subscriber.id, db)
    except Exception:
        logger.warning("founder_comp reveal: activation stamp failed sub=%s", subscriber.id)

    try:
        from src.services.auto_mode import enqueue_action
        enqueue_action(subscriber.id, property_id, db)
    except Exception:
        logger.error("founder_comp reveal: Auto Mode enqueue failed sub=%s prop=%s",
                     subscriber.id, property_id, exc_info=True)

    try:
        _send_lead_unlock_email(subscriber, prop, score, owner, enriched)
    except Exception as exc:
        logger.error("founder_comp reveal: email send failed: %s", exc, exc_info=True)

    logger.info("founder_comp reveal complete: subscriber=%s property=%s",
                subscriber.id, property_id)
    return True


def _on_lead_unlock_payment(payment_intent: dict, db: Session) -> None:
    """
    Handle a single-lead unlock purchase — the $2.50–$7 dashboard unlock
    (product=lead_unlock) and the $150/$99 hot lead unlock
    (product=hot_lead_unlock) both land here; the deliverable is identical.

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

    # Audit row — SentLead marks this lead as delivered to this subscriber.
    # Uses begin_nested() so a race-condition IntegrityError only rolls back
    # this savepoint, leaving the outer transaction (and the session) clean.
    try:
        with db.begin_nested():
            pi_id = _attr(payment_intent, "id")
            existing_sent = db.execute(
                select(SentLead).where(
                    SentLead.subscriber_id == subscriber.id,
                    SentLead.property_id == property_id,
                )
            ).scalar_one_or_none()
            amount_cents = _attr(payment_intent, "amount_received") or _attr(payment_intent, "amount")
            if not existing_sent:
                sent_row = SentLead(
                    subscriber_id=subscriber.id,
                    property_id=property_id,
                    source="lead_unlock_payment",
                    stripe_payment_intent_id=pi_id,
                    amount_cents=amount_cents,
                )
                db.add(sent_row)
                db.flush()
                is_new_payment = True
            else:
                sent_row = existing_sent
                # A row can pre-exist with no payment intent (e.g. delivered
                # free via the daily digest), or with a DIFFERENT payment
                # intent from an earlier, separately-priced unlock of the same
                # property (e.g. a $2.50-$7 dashboard unlock followed later by
                # a $150/$99 hot-lead unlock) — both are real charges, not a
                # duplicate webhook delivery, and must still hit the ledger.
                # Only an identical stripe_payment_intent_id means this is a
                # retried webhook for a charge already recorded.
                is_new_payment = existing_sent.stripe_payment_intent_id != pi_id
                if is_new_payment:
                    existing_sent.stripe_payment_intent_id = pi_id
                    existing_sent.amount_cents = amount_cents

            # Centralized ledger — see src/services/revenue_ledger.py.
            # SentLead reuses one row per (subscriber, property) across
            # repeat/upgraded purchases, so source_id can't be sent_row.id —
            # two distinct charges on the same row would collide under
            # record_revenue's (source_table, source_id) uniqueness and the
            # second, genuinely different charge would be silently dropped.
            # Keying on a hash of the payment_intent id instead gives every
            # distinct charge its own ledger row while a retried webhook for
            # the SAME payment_intent still hashes identically, so idempotency
            # is preserved.
            if is_new_payment and amount_cents is not None:
                from src.services.revenue_ledger import (
                    record_revenue, attribute_enrichment_cost_for_property,
                    stripe_payment_intent_ledger_id,
                )
                # hot_lead_unlock and lead_unlock share this handler (see
                # docstring) but must be distinguishable in revenue reporting
                # — the PI's own metadata already tells us which one this is.
                ledger_product_type = (
                    "hot_lead_unlock" if _attr(meta, "product") == "hot_lead_unlock" else "lead_unlock"
                )
                record_revenue(
                    db, subscriber_id=subscriber.id, product_type=ledger_product_type,
                    amount_cents=amount_cents, source_table="stripe_payment_intent",
                    source_id=stripe_payment_intent_ledger_id(pi_id), property_id=property_id,
                    occurred_at=sent_row.sent_at,
                )
                attribute_enrichment_cost_for_property(db, property_id, subscriber.id)

                # Task 4.1 frozen control holdout — lead unlock is the
                # conversion event for the fomo sequence.
                from src.services.ab_engine import record_holdout_conversion
                record_holdout_conversion(subscriber.id, "fomo_holdout", db)
    except (IntegrityError, OperationalError) as exc:
        logger.warning("lead_unlock: SentLead insert failed: %s", exc)

    # Trigger Auto Mode (skip-trace + first SMS) for this unlocked lead.
    # Self-guarded on eligibility + fails soft — non-Auto-Mode subscribers
    # get a near-no-op. Wrapped in try/except so the email send below is
    # never blocked by an Auto Mode failure.
    try:
        from src.services.auto_mode import enqueue_action
        enqueue_action(subscriber.id, property_id, db)
    except Exception:
        logger.error(
            "lead_unlock: Auto Mode enqueue failed sub=%s prop=%s — continuing",
            subscriber.id, property_id, exc_info=True,
        )

    # Send the email with full lead details
    try:
        _send_lead_unlock_email(subscriber, prop, score, owner, enriched)
    except Exception as exc:
        logger.error("lead_unlock: email send failed: %s", exc, exc_info=True)

    # Welcome email — deferred from /api/free-signup with intent='unlock'.
    # Sent only on the first unlock so repeat unlocks don't spam the inbox.
    # Mirrors the checkout handler: issue a fresh magic link, never a password.
    try:
        first_unlock = db.execute(
            select(func.count()).select_from(SentLead).where(
                SentLead.subscriber_id == subscriber.id,
                SentLead.source == "lead_unlock_payment",
            )
        ).scalar() or 0
        # T-B12-05: stamp the activation event (first-ever contact unlock)
        # regardless of welcome-email eligibility above.
        from src.services.activation_tracking import stamp_first_unlock
        stamp_first_unlock(subscriber.id, db)

        if first_unlock <= 1:
            from src.services.email import send_welcome_email
            from src.services import subscriber_auth as _sub_auth
            magic_url = None
            try:
                raw = _sub_auth.issue_magic_link(subscriber, db)
                magic_url = _sub_auth.magic_link_url(raw)
            except Exception:
                magic_url = None
                logger.warning(
                    "lead_unlock: magic-link issuance failed for sub=%s",
                    subscriber.id, exc_info=True,
                )
            send_welcome_email(subscriber, magic_link_url=magic_url)
    except Exception as exc:
        logger.warning("lead_unlock: welcome email failed sub=%s: %s",
                       subscriber.id, exc)

    logger.info(
        "lead_unlock complete: subscriber=%s property=%s pi=%s",
        subscriber.id, property_id, _attr(payment_intent, "id"),
    )

    # Segmentation is post-fulfillment analytics — never let it abort the handler
    # before the revenue-attribution + Meta CAPI steps below.
    try:
        from src.services.segmentation_engine import reclassify_safe
        from src.services.revenue_signal import ACTION_LEAD_UNLOCK_PAID
        _score_value = (
            int(score.final_cds_score)
            if score is not None and score.final_cds_score is not None
            else None
        )
        reclassify_safe(
            subscriber.id, db,
            action_type=ACTION_LEAD_UNLOCK_PAID,
            metadata={"property_id": property_id, "score": _score_value},
        )
    except Exception:
        logger.warning("lead_unlock: reclassify failed sub=%s — non-fatal", subscriber.id, exc_info=True)

    try:
        from src.services.attribution_service import record_conversion_attribution
        record_conversion_attribution(
            conversion_type="paid_unlock",
            source_table="stripe_payment_intents",
            source_event_id=_attr(payment_intent, "id") or "",
            subscriber_id=subscriber.id,
            occurred_at=datetime.now(timezone.utc),
            property_id=property_id,
            db=db,
        )
    except Exception:
        logger.warning("Attribution recording failed sub=%s", subscriber.id, exc_info=True)

    # Feed the purchase to Lifecycle (D7) — last-touch nudge attribution is stamped
    # by the supervisor's unlock_purchased branch, not here.
    try:
        from src.agents.events.ingestion import publish_lifecycle_event
        _amount_cents = _attr(payment_intent, "amount_received") or _attr(payment_intent, "amount")
        publish_lifecycle_event({
            "event_type": "unlock_purchased",
            "subscriber_id": subscriber.id,
            "payload": {
                "property_id": property_id,
                "product": _attr(meta, "product") or "lead_unlock",
                "amount_cents": _amount_cents,
                "revenue": (_amount_cents / 100) if _amount_cents is not None else None,
            },
        })
    except Exception:
        logger.warning("lead_unlock: publish_lifecycle_event failed sub=%s", subscriber.id, exc_info=True)

    _fire_capi_for_pi(
        payment_intent, subscriber, "lead_unlock",
        f"unlock_{_attr(payment_intent, 'id') or ''}", db,
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
            try:
                from src.agents.events.ingestion import publish_lifecycle_event
                publish_lifecycle_event({
                    "event_type": "accelerated_wallet_push_eligible",
                    "subscriber_id": subscriber.id,
                    "payload": eligible,
                })
            except Exception as _pub_exc:
                logger.warning("publish_lifecycle_event failed sub=%s: %s", subscriber.id, _pub_exc)
    except Exception as exc:
        logger.warning("accelerated_wallet_push from _on_card_saved failed sub=%s: %s",
                       subscriber.id, exc)

    try:
        from src.services.attribution_service import record_conversion_attribution
        record_conversion_attribution(
            conversion_type="saved_card",
            source_table="stripe_payment_intents",
            source_event_id=pi_id or "",
            subscriber_id=subscriber.id,
            occurred_at=datetime.now(timezone.utc),
            db=db,
        )
    except Exception:
        logger.warning("Attribution recording failed sub=%s", subscriber.id, exc_info=True)


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
            try:
                from src.agents.events.ingestion import publish_lifecycle_event
                publish_lifecycle_event({
                    "event_type": "accelerated_wallet_push_eligible",
                    "subscriber_id": subscriber.id,
                    "payload": eligible,
                })
            except Exception as _pub_exc:
                logger.warning("publish_lifecycle_event failed sub=%s: %s", subscriber.id, _pub_exc)
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

    try:
        from src.services.attribution_service import record_conversion_attribution
        record_conversion_attribution(
            conversion_type="bundle_purchase",
            source_table="stripe_payment_intents",
            source_event_id=pi_id or "",
            subscriber_id=subscriber_id,
            occurred_at=datetime.now(timezone.utc),
            bundle_id=purchase.id,
            bundle_type=bundle_type,
            zip_code=zip_code,
            db=db,
        )
    except Exception:
        logger.warning("Attribution recording failed sub=%s", subscriber_id, exc_info=True)

    _sub_bundle = db.get(Subscriber, subscriber_id)
    if _sub_bundle is not None:
        _fire_capi_for_pi(payment_intent, _sub_bundle, "bundle", f"bundle_{pi_id}", db)

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

    _sub_premium = db.get(Subscriber, subscriber_id)
    if _sub_premium is not None:
        _fire_capi_for_pi(payment_intent, _sub_premium, "premium", f"premium_{pi_id}", db)

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

    # fa016: Re-check accelerated wallet push after PremiumPurchase is persisted.
    # `payment_method.attached` can race ahead of `payment_intent.succeeded` and
    # run its own eligibility check BEFORE this row exists — in that case the
    # check silently fails on the "paid intent" gate. Run it again here so a
    # premium purchase with a freshly saved card reliably dispatches.
    try:
        from src.agents.events.ingestion import publish_lifecycle_event
        from src.services import wallet_engine
        eligible = wallet_engine.accelerated_push_eligible(subscriber_id, db)
        if eligible:
            try:
                wallet_engine.ensure_offer_row(subscriber_id, eligible, db)
            except Exception as exc_offer:
                logger.warning("ensure_offer_row failed sub=%s: %s", subscriber_id, exc_offer)
            try:
                from src.services.business_events import log_business_event
                log_business_event(
                    "ACCELERATED_WALLET_ELIGIBLE", subscriber_id=subscriber_id,
                    payload={"trigger": "_on_premium_payment", "pi": pi_id}, db=db,
                )
            except Exception:
                pass
            from src.agents.events.ingestion import publish_lifecycle_event
            publish_lifecycle_event({
                "event_type": "accelerated_wallet_push_eligible",
                "subscriber_id": subscriber_id,
                "payload": eligible,
            })
    except Exception as exc:
        logger.warning("accelerated_wallet_push from _on_premium_payment failed sub=%s: %s",
                       subscriber_id, exc)

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
            from src.agents.events.ingestion import publish_lifecycle_event
            publish_lifecycle_event({
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

    try:
        from src.services.attribution_service import record_conversion_attribution
        record_conversion_attribution(
            conversion_type="wallet_topup",
            source_table="stripe_payment_intents",
            source_event_id=pi_id or "",
            subscriber_id=subscriber_id,
            occurred_at=datetime.now(timezone.utc),
            db=db,
        )
    except Exception:
        logger.warning("Attribution recording failed sub=%s", subscriber_id, exc_info=True)


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


def _on_auto_mode_addon_purchase(session: dict, db: Session) -> None:
    """Activate Auto Mode entitlement on a successful $79–$99/mo add-on
    checkout. Identifies the subscriber by metadata.subscriber_id and
    cross-checks line items for the configured Auto Mode price id.
    Idempotent: re-running for the same subscriber is a no-op.
    """
    from src.core.models import Subscriber

    meta = session.get("metadata", {}) or {}
    subscriber_id_str = meta.get("subscriber_id")
    session_id = session.get("id")
    if not subscriber_id_str:
        logger.error("[auto_mode_addon] checkout session missing subscriber_id meta=%s", meta)
        return
    try:
        subscriber_id = int(subscriber_id_str)
    except (TypeError, ValueError):
        logger.error("[auto_mode_addon] non-int subscriber_id=%r", subscriber_id_str)
        return

    # Belt-and-braces: confirm the completed session actually purchased the
    # auto_mode price (guards against metadata being copied to a wrong session).
    expected_price_id = None
    try:
        from config.settings import settings
        expected_price_id = settings.active_stripe_price("auto_mode")
    except Exception as exc:
        logger.error("[auto_mode_addon] could not resolve auto_mode price: %s", exc)
        return
    if not expected_price_id:
        logger.error("[auto_mode_addon] STRIPE_PRICE_AUTO_MODE unconfigured")
        return

    matched_price = False
    try:
        if session.get("api_key") or not stripe.api_key:
            stripe.api_key = (settings.active_stripe_secret_key.get_secret_value()
                              if settings.active_stripe_secret_key else stripe.api_key)
        items = stripe.checkout.Session.list_line_items(session_id, limit=10)
        for li in items.data:
            li_price = li.get("price") or {}
            if li_price.get("id") == expected_price_id:
                matched_price = True
                break
    except Exception as exc:
        logger.warning(
            "[auto_mode_addon] list_line_items failed for session=%s: %s — "
            "falling back to metadata-only match",
            session_id, exc,
        )
        # Metadata says auto_mode_addon — trust it if the API call fails.
        matched_price = True

    if not matched_price:
        logger.warning(
            "[auto_mode_addon] session=%s metadata says auto_mode_addon but no "
            "matching line item — skipping",
            session_id,
        )
        return

    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        logger.error("[auto_mode_addon] subscriber=%d not found", subscriber_id)
        return
    if sub.auto_mode_enabled:
        logger.info(
            "[auto_mode_addon] subscriber=%d already enabled — idempotent no-op",
            subscriber_id,
        )
        return

    sub.auto_mode_enabled = True
    db.flush()
    logger.info(
        "[auto_mode_addon] enabled via add-on purchase: subscriber=%d session=%s",
        subscriber_id, session_id,
    )


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

    # Growth/Power wallets include Auto Mode by spec — set the flag explicitly
    # so dashboards don't have to compute eligibility client-side. is_eligible()
    # would already return True via wallet_tier, this just makes state explicit.
    if tier in ("growth", "power") and not sub.auto_mode_enabled:
        sub.auto_mode_enabled = True
        db.flush()
        logger.info(
            "[WalletSub] auto-enabled Auto Mode for %s tier subscriber=%d",
            tier, subscriber_id,
        )

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

    # Transactional confirmation SMS (bypasses Lifecycle — not marketing)
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

    from src.services.segmentation_engine import reclassify_safe
    from src.services.revenue_signal import ACTION_WALLET_SUBSCRIPTION_RENEWED
    reclassify_safe(subscriber_id, db, action_type=ACTION_WALLET_SUBSCRIPTION_RENEWED)

    try:
        from src.services.attribution_service import record_conversion_attribution
        record_conversion_attribution(
            conversion_type="wallet_activation",
            source_table="stripe_invoices",
            source_event_id=invoice.get("id", ""),
            subscriber_id=subscriber_id,
            occurred_at=datetime.now(timezone.utc),
            wallet_tier=tier,
            db=db,
        )
    except Exception:
        logger.warning("Attribution recording failed sub=%s", subscriber_id, exc_info=True)

    # Task 4.1 frozen control holdout — wallet activation is the conversion
    # event for the accelerated_wallet_push sequence. Test name matches the
    # key in config/lifecycle_holdout_tests.yaml; no-op for subscribers never
    # assigned an arm.
    from src.services.ab_engine import record_holdout_conversion
    record_holdout_conversion(subscriber_id, "wallet_push_holdout", db)


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


def _resolve_invoice_payment_intent(invoice_id: str):
    """Best-effort: fetch an invoice's PaymentIntent id. Needed only on Stripe
    API versions where the invoice.payment_succeeded payload omits it
    (2026-02-25+ exposes it via the expanded `payments` sub-resource)."""
    if not invoice_id:
        return None
    try:
        from config.settings import settings
        import stripe as _stripe
        key = settings.active_stripe_secret_key
        if not key:
            return None
        _stripe.api_key = key.get_secret_value()
        # json round-trip → plain dict (StripeObject lacks a public .get/.to_dict
        # in this SDK version)
        inv = json.loads(str(_stripe.Invoice.retrieve(invoice_id, expand=["payments"])))
        pays = (inv.get("payments") or {}).get("data") or []
        if pays:
            return (pays[0].get("payment") or {}).get("payment_intent")
    except Exception:
        logger.warning("Could not resolve payment_intent for invoice=%s", invoice_id, exc_info=True)
    return None


def _affiliate_reverse_from_charge(charge, reason: str, db: Session) -> None:
    """Mark a captured subscription invoice reversed so the monthly affiliate run
    writes a Commission Clawback. Version-robust: uses charge.invoice when present
    (older API), else matches by charge.payment_intent (2026-02-25+, where
    charge.invoice is null). No-op if the charge isn't a captured subscription invoice.
    """
    try:
        from src.services.affiliate_engine import (
            mark_invoice_reversed,
            mark_invoice_reversed_by_payment_intent,
        )
        invoice_id = charge.get("invoice") if hasattr(charge, "get") else None
        if invoice_id:
            mark_invoice_reversed(db, invoice_id, reason)
            return
        pi = charge.get("payment_intent") if hasattr(charge, "get") else None
        if pi:
            mark_invoice_reversed_by_payment_intent(db, pi, reason)
    except Exception:
        logger.warning("Affiliate invoice reversal failed reason=%s", reason, exc_info=True)


def _on_charge_refunded(charge: dict, db: Session) -> None:
    """charge.refunded — flip purchase status, optionally clawback credits.

    Policy:
      - Card-paid purchases: status → refunded, log refund_amount_cents.
      - Credit-paid purchases of artifact SKUs (report/brief): credit-back the
        wallet via wallet_engine.refund_credits() so the user isn't double-charged.
      - Credit-paid purchases of data-surrendered SKUs (transfer/byol): no
        credit clawback — the underlying cost (BatchData lookup) was paid and
        the data can't be unsent. Log the loss; ops can manually adjust.
      - Lead Pack refunds: handled via payment_intent.succeeded -> lead_pack path.
    """
    from src.core.models import PremiumPurchase, LeadPackPurchase, SentLead
    charge_id = charge.get("id")
    pi_id = charge.get("payment_intent")

    _affiliate_reverse_from_charge(charge, "refund", db)

    # Try PremiumPurchase first
    purchase = _resolve_premium_purchase_from_charge(charge, db)
    
    # If not a premium purchase, check if it's a lead pack
    if purchase is None and pi_id:
        purchase = db.execute(
            select(LeadPackPurchase).where(
                LeadPackPurchase.stripe_payment_intent_id == pi_id
            )
        ).scalar_one_or_none()
        if purchase:
            if purchase.refunded_at is not None:
                logger.info("[Refund] LeadPack purchase %d already refunded — skipping", purchase.id)
                return
            
            purchase.status = "refunded"
            purchase.refunded_at = datetime.now(timezone.utc)
            purchase.refund_reason = (charge.get("reason") or "unspecified")[:100]
            db.flush()

            # Revenue was split across up to 5 sent_leads rows at delivery
            # time (one ledger row per delivered lead, see
            # lead_pack_fulfillment_sweep.py) — never one row for the
            # purchase itself — so every sent_leads row tied to this
            # purchase's payment intent must be marked refunded individually.
            from src.services.revenue_ledger import mark_ledger_refunded
            sent_lead_ids = db.execute(
                select(SentLead.id).where(
                    SentLead.stripe_payment_intent_id == pi_id,
                    SentLead.source == "lead_pack",
                )
            ).scalars().all()

            # A partial refund of the overall purchase must prorate across
            # each lead's own ledger share, not zero every row out — pull
            # each row's actual original amount (never assume a fixed split
            # order) and allocate proportionally, giving the remainder to
            # the smallest share so the parts sum exactly to the refund.
            per_row_refund_cents: dict[int, int] = {}
            if sent_lead_ids:
                ledger_amounts = dict(db.execute(text("""
                    SELECT source_id, amount_cents FROM platform_revenue_ledger
                    WHERE source_table = 'sent_leads' AND source_id = ANY(:ids)
                """), {"ids": list(sent_lead_ids)}).all())
                total_original = sum(ledger_amounts.values())
                total_refund = charge.get("amount_refunded") or 0
                if total_original and 0 < total_refund < total_original:
                    ordered_ids = sorted(sent_lead_ids, key=lambda sid: -ledger_amounts.get(sid, 0))
                    allocated = 0
                    for idx, sid in enumerate(ordered_ids):
                        if idx == len(ordered_ids) - 1:
                            share = total_refund - allocated
                        else:
                            share = round(ledger_amounts.get(sid, 0) * total_refund / total_original)
                        allocated += share
                        per_row_refund_cents[sid] = share

            for sent_lead_id in sent_lead_ids:
                mark_ledger_refunded(
                    db, source_table="sent_leads", source_id=sent_lead_id,
                    refunded_at=purchase.refunded_at,
                    refunded_amount_cents=per_row_refund_cents.get(sent_lead_id),
                )

            # Clear exclusivity rows so the properties immediately become
            # available again for other trades/subscribers.
            try:
                from src.services.lead_exclusivity import clear_exclusivity_for_purchase
                cleared = clear_exclusivity_for_purchase(db, purchase.id, source="lead_pack")
                if cleared:
                    logger.info("[Refund] Cleared %d exclusivity rows for LeadPack purchase=%d", cleared, purchase.id)
            except Exception as exc:
                logger.error("[Refund] Failed to clear exclusivity rows for purchase=%d: %s", purchase.id, exc)
            
            logger.info(
                "[Refund] LeadPack purchase=%d amount_cents=%d reason=%s",
                purchase.id, charge.get("amount_refunded", 0), purchase.refund_reason,
            )
            return
    
    if purchase is None:
        # Not one of our premium charges (could be a wallet topup, lead pack, etc.)
        # Lead pack is handled above.
        logger.debug("[Refund] no PremiumPurchase for charge=%s", charge_id)
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

    from src.services.revenue_ledger import mark_ledger_refunded
    mark_ledger_refunded(
        db, source_table="premium_purchases", source_id=purchase.id,
        refunded_at=purchase.refunded_at, refunded_amount_cents=refund_amount,
    )

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

    # Disputes claw back the same as refunds. Version-robust: invoice id when
    # present, else matched by charge.payment_intent.
    _affiliate_reverse_from_charge(charge_obj, "dispute", db)

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
    
    Uses database-backed cross-trade exclusivity (lead_exclusivity table).
    """
    meta = _attr(payment_intent, "metadata") or {}
    if _attr(meta, "product") != "lead_pack":
        return

    stripe_payment_intent_id = _attr(payment_intent, "id")
    feed_uuid  = _attr(meta, "feed_uuid")
    zip_code   = _attr(meta, "zip_code")
    vertical   = _attr(meta, "vertical")
    county_id  = _attr(meta, "county_id")

    if not all([stripe_payment_intent_id, feed_uuid, zip_code, vertical, county_id]):
        logger.error(
            "[LeadPack] payment_intent.succeeded missing required metadata: %s", meta
        )
        return

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

    subscriber = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if subscriber is None:
        logger.error("[LeadPack] No subscriber for feed_uuid %s", feed_uuid)
        return

    # Close any open abandoned-checkout recovery for this lead pack (Task 7).
    if subscriber.email:
        try:
            from src.services import checkout_recovery
            checkout_recovery.mark_recovered(db, subscriber.email)
        except Exception:
            logger.warning("[LeadPack] checkout_recovery mark_recovered failed sub=%s", subscriber.id, exc_info=True)

    now = datetime.now(timezone.utc)
    exclusive_until = now + timedelta(hours=72)

    purchase = LeadPackPurchase(
        subscriber_id=subscriber.id,
        zip_code=zip_code,
        vertical=vertical,
        county_id=county_id,
        stripe_payment_intent_id=stripe_payment_intent_id,
        status="pending",
        purchased_at=now,
        exclusive_until=exclusive_until,
        amount_cents=_attr(payment_intent, "amount_received") or _attr(payment_intent, "amount"),
    )
    db.add(purchase)
    db.flush()

    # Defense-in-depth (ADR 0002): the checkout gate already blocks unlaunched
    # counties, but a PaymentIntent could be created via a stale client or an
    # API bypass. The charge succeeded, so an unlaunched county is refunded,
    # not delivered.
    from src.utils.county_config import is_county_launched
    if not is_county_launched(county_id, db):
        purchase.status = "refunded"
        purchase.refunded_at = now
        purchase.refund_reason = "county_not_launched"
        db.flush()
        try:
            import stripe
            stripe.api_key = settings.active_stripe_secret_key.get_secret_value()
            stripe_refund = stripe.Refund.create(
                payment_intent=stripe_payment_intent_id,
                idempotency_key=f"leadpack-refund-{stripe_payment_intent_id}",
            )
            purchase.stripe_refund_id = stripe_refund.get("id")
        except Exception as e:
            logger.error("[LeadPack] county_not_launched refund failed for %s: %s", stripe_payment_intent_id, e)
        logger.warning(
            "[LeadPack] county %s not launched — refunded purchase %s", county_id, purchase.id
        )
        return

    # Same sellability predicate as the availability feed card and the checkout
    # gate (ADR 0032 D5): qualified, non-guess (A2), contactable.
    from src.services.lead_pool_service import apply_segment_filter, sellable_lead_filters
    lead_filter = sellable_lead_filters(settings)
    lead_filter.append(Property.zip == zip_code)
    lead_filter.append(Property.county_id == county_id)

    try:
        score_col = DistressScore.vertical_scores[vertical].as_float()
    except KeyError:
        logger.error("[LeadPack] Unknown vertical '%s' for purchase %s", vertical, purchase.id)
        purchase.status = "expired"
        return

    from src.core.models import Owner
    from src.utils.lead_filters import phone_priority_order

    apply_segment_filter(lead_filter, _attr(meta, "segment"), now)

    try:
        from src.services.lead_exclusivity import (
            acquire_zip_lock,
            get_exclusive_property_ids,
            record_exclusivity,
            clear_exclusivity_for_purchase,
        )

        acquire_zip_lock(db, zip_code, county_id)

        excl = get_exclusive_property_ids(db, county_id, now, zip_code=zip_code)
        if excl:
            lead_filter.append(Property.id.not_in(excl))

        top_leads = db.execute(
            select(Property, DistressScore)
            .join(DistressScore, DistressScore.property_id == Property.id)
            .outerjoin(Owner, Owner.property_id == Property.id)
            .where(and_(*lead_filter))
            .order_by(*phone_priority_order(score_col))
            .limit(5)
        ).all()

        if len(top_leads) < 5:
            # Set refunded_at BEFORE calling Stripe — the charge.refunded webhook
            # may fire synchronously/in-parallel, and we need the guard to be visible.
            purchase.status = "refunded"
            purchase.refunded_at = now
            purchase.refund_reason = f"short_pack_{len(top_leads)}_of_5"
            db.flush()
            
            # All-or-nothing: no exclusivity rows written for a short pack.
            try:
                import stripe
                stripe.api_key = settings.active_stripe_secret_key.get_secret_value()
                stripe_refund = stripe.Refund.create(
                    payment_intent=stripe_payment_intent_id,
                    idempotency_key=f"leadpack-refund-{stripe_payment_intent_id}",
                )
                purchase.stripe_refund_id = stripe_refund.get("id")
            except Exception as e:
                logger.error("[LeadPack] refund failed: %s", e)
            
            logger.info(
                "[LeadPack] Short pack (%d/5) — refunded purchase %s",
                len(top_leads), purchase.id,
            )
            return

        # ADR 0018 — RESERVE, don't deliver. The 5 leads are claimed now (under
        # the advisory lock) so no concurrent buyer can grab them, and their
        # Cross-Trade Exclusivity is written immediately (72h from payment). The
        # leads are NOT yet delivered: lead_pack_fulfillment_sweep runs Tracerfy
        # Hot-Enrichment, enforces the 100% Quality Floor, then delivers (with the
        # SentLead rows + email) or refunds (releasing this reservation).
        purchase.lead_ids = [prop.id for prop, _ in top_leads]
        purchase.status = "enriching"
        db.flush()

        record_exclusivity(
            db=db,
            property_ids=purchase.lead_ids,
            zip_code=zip_code,
            county_id=county_id,
            trade=vertical,
            source="lead_pack",
            source_id=purchase.id,
            exclusive_until=exclusive_until,
        )

        logger.info(
            "[LeadPack] Reserved purchase %s — %d leads for %s/%s/%s to subscriber %s; awaiting hot-enrichment",
            purchase.id, len(top_leads), zip_code, vertical, county_id, subscriber.id,
        )

        # Ring the bell: fire an event so the always-on listener fulfills NOW
        # rather than waiting for the next cron tick. Best-effort — if the bus is
        # down the lead_pack_fulfillment_sweep cron picks it up within ~2 min.
        try:
            from src.agents.events.ingestion import publish_lifecycle_event
            publish_lifecycle_event({
                "event_type": "lead_pack_reserved",
                "subscriber_id": subscriber.id,
                "payload": {"purchase_id": purchase.id},
                "idempotency_key": f"leadpack-reserved-{stripe_payment_intent_id}",
            })
        except Exception as pub_exc:
            logger.warning(
                "[LeadPack] publish lead_pack_reserved failed for purchase %s "
                "(cron sweep is backstop): %s", purchase.id, pub_exc,
            )

        # ── Revenue attribution + Meta CAPI (S2) ─────────────────────────────
        # Reaches here only on a confirmed, non-refunded reservation (both refund
        # branches above already returned). Records revenue in the attribution
        # ledger for ROAS and reports the Purchase to Meta. Wrapped in its own
        # try/except so a failure here can NEVER trigger the outer reservation
        # rollback/raise — CAPI is an observer and money is already captured.
        try:
            _amount_cents = _attr(payment_intent, "amount_received") or _attr(payment_intent, "amount") or 0
            _amount_dollars = round(_amount_cents / 100, 2)

            _stamp_campaign_fields(subscriber, meta if isinstance(meta, dict) else {}, db)

            try:
                from src.services.attribution_service import record_conversion_attribution
                record_conversion_attribution(
                    conversion_type="lead_pack_purchase",
                    source_table="lead_pack_purchases",
                    source_event_id=stripe_payment_intent_id,
                    subscriber_id=subscriber.id,
                    occurred_at=now,
                    zip_code=zip_code,
                    revenue_amount=_amount_dollars,
                    db=db,
                )
            except Exception:
                logger.warning(
                    "[LeadPack] attribution recording failed purchase=%s — non-fatal",
                    purchase.id, exc_info=True,
                )

            from src.services.meta_capi_service import fire_purchase_event
            fire_purchase_event(
                subscriber=subscriber,
                amount=_amount_dollars,
                source="lead_pack",
                request_context={
                    "buyer_ip": _attr(meta, "buyer_ip"),
                    "buyer_user_agent": _attr(meta, "buyer_user_agent"),
                    "fbclid": _attr(meta, "fbclid"),
                    "utm_campaign": _attr(meta, "utm_campaign"),
                    "campaign_id": _attr(meta, "campaign_id"),
                    "currency": (_attr(payment_intent, "currency") or "usd").upper(),
                },
                event_id=f"leadpack_{stripe_payment_intent_id}",
            )
        except Exception:
            logger.warning(
                "[LeadPack] Meta CAPI / attribution block failed purchase=%s — non-fatal",
                purchase.id, exc_info=True,
            )

    except Exception as e:
        db.rollback()
        logger.error("[LeadPack] Reservation failed: %s", e, exc_info=True)
        raise


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


# ---------------------------------------------------------------------------
# checkout.session.expired — abandonment signal for hot lead unlock
# ---------------------------------------------------------------------------

def _on_checkout_expired(session: dict, db: Session) -> None:
    """
    Fires when a Stripe checkout session expires without payment.
    For hot_lead_unlock sessions opened by free-tier subscribers, publish
    abandonment_click_no_complete to Lifecycle so the retention flow can trigger.
    For every other expired session with a captured email, start the
    abandoned-checkout recovery sequence (Task 7). Recovery holds the contact
    out of the slower non-buyer nurture drip until it fails, so the two never
    double-contact — this is the capture point for the session-expiry path.
    """
    meta = session.get("metadata") or {}
    if meta.get("product") != "hot_lead_unlock":
        _start_recovery_for_expired_checkout(session, db)
        return

    stripe_customer_id = session.get("customer")
    if not stripe_customer_id:
        return

    from sqlalchemy import text
    row = db.execute(
        text("SELECT id, tier, vertical FROM subscribers WHERE stripe_customer_id = :cid LIMIT 1"),
        {"cid": stripe_customer_id},
    ).fetchone()
    if not row or row[1] != "free":
        return

    subscriber_id, _, vertical = row
    lead_id = meta.get("lead_id", "")

    try:
        from src.agents.events.ingestion import publish_lifecycle_event
        publish_lifecycle_event({
            "event_type": "abandonment_click_no_complete",
            "subscriber_id": subscriber_id,
            "payload": {
                "lead_id": lead_id,
                "vertical": vertical or "",
            },
        })
    except Exception:
        logger.warning(
            "abandonment_click_no_complete publish failed: subscriber=%s", subscriber_id,
        )


def _start_recovery_for_expired_checkout(session: dict, db: Session) -> None:
    details = session.get("customer_details") or {}
    email = (details.get("email") or session.get("customer_email") or "").lower().strip()
    if not email:
        return
    meta = session.get("metadata") or {}
    # Rebuild enough context to mint a fresh resume-checkout link — the expired
    # session itself can't be reused.
    resume_context = {
        "tier": meta.get("tier"),
        "vertical": meta.get("vertical"),
        "county_id": meta.get("county_id"),
        "zip_codes": [z for z in (meta.get("zip_codes") or "").split(",") if z],
    }
    try:
        from src.services import checkout_recovery
        checkout_recovery.start_recovery(
            db,
            email=email,
            source="session_expired",
            phone=details.get("phone"),
            resume_context=resume_context,
        )
    except Exception:
        logger.warning("checkout_recovery capture failed for expired checkout email=%s", email, exc_info=True)
        logger.warning("non_buyer_nurture capture failed for expired checkout session %s", session.get("id"), exc_info=True)


def _send_lead_pack_refund_email(
    subscriber: "Subscriber",
    purchase: LeadPackPurchase,
) -> None:
    """
    Notify a buyer that their Lead Pack could not clear the Quality Floor and has
    been fully refunded (ADR 0018). Sent by lead_pack_fulfillment_sweep.
    """
    from src.services.email import send_email

    if not subscriber.email:
        return

    send_email(
        to=subscriber.email,
        subject="Your Forced Action Lead Pack — Refunded",
        body_text=(
            f"Hi {subscriber.name or 'there'},\n\n"
            f"We weren't able to confirm fresh contact details for all 5 leads in "
            f"your pack for ZIP {purchase.zip_code} ({purchase.vertical.title()}), "
            f"so we did not deliver a partial pack.\n\n"
            f"Your $99 has been fully refunded — it should appear on your statement "
            f"within 5–10 business days. No leads were locked to your account.\n\n"
            f"You're welcome to try again shortly, or reach us at "
            f"support@forcedaction.io if you'd like help.\n\n"
            f"— Forced Action Team"
        ),
    )
