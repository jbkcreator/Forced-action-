"""
Stripe integration — M1-B.

Handles:
  - Founding vs regular price selection (atomic SELECT FOR UPDATE)
  - Checkout session creation
  - One-time payments (lead pack, hot lead unlock)

Pricing table (live rates — founding rate = current live rate for all tiers):
  Tier        | Live/Founding | Regular (future)
  ------------|---------------|------------------
  starter     | $299/mo       | TBD
  pro         | $499/mo       | TBD
  founder     | $1,100/mo     | —

Every product has TWO Stripe price objects: founding_price_id + regular_price_id.
Founding rate is selected atomically at checkout and locked forever.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import stripe
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import County, FoundingSubscriberCount, Plan

logger = logging.getLogger(__name__)


def _log_checkout_started(
    db: Optional[Session],
    session_id: str,
    product: str,
    metadata: dict,
) -> None:
    """
    Audit row for a created Stripe Checkout Session — the only place
    "checkouts started" becomes queryable (Stripe's own SDK call only logs
    to stdout, no DB row). Thin wrapper over the existing unified webhook
    audit logger (best-effort, never raises; rides the caller's transaction
    if db is given, else opens its own short-lived session).
    """
    from src.services.webhook_log import log_webhook_event
    log_webhook_event(
        source="stripe",
        event_type="checkout.session.created",
        direction="outbound",
        source_event_id=session_id,
        status="processed",
        payload={"product": product, **metadata},
        payload_kind="checkout_started",
        db=db,
    )

def _founding_limit() -> int:
    """Read from env (FOUNDING_SPOT_LIMIT) — changeable without redeploy."""
    from config.settings import get_settings
    return get_settings().founding_spot_limit

# ---------------------------------------------------------------------------
# Stripe price IDs — loaded from env. Set these after creating products in
# the Stripe dashboard. Each tier needs two price objects.
# ---------------------------------------------------------------------------
# Environment variable naming convention:
#   STRIPE_PRICE_{TIER}_{VERTICAL}_FOUNDING
#   STRIPE_PRICE_{TIER}_{VERTICAL}_REGULAR
#
# Because verticals share the same tier pricing, we use a single price per
# tier (not per vertical) — vertical is tracked in our DB, not Stripe.
#
# Required env vars:
#   STRIPE_PRICE_STARTER_FOUNDING      = price_xxx
#   STRIPE_PRICE_STARTER_REGULAR       = price_xxx
#   STRIPE_PRICE_PRO_FOUNDING          = price_xxx
#   STRIPE_PRICE_PRO_REGULAR           = price_xxx
#   STRIPE_PRICE_DOMINATOR_FOUNDING    = price_xxx
#   STRIPE_PRICE_DOMINATOR_REGULAR     = price_xxx
#   STRIPE_PRICE_LEAD_PACK             = price_xxx   (one-time $99)
#   STRIPE_PRICE_HOT_LEAD_UNLOCK       = price_xxx   (one-time $150)

def _price_ids():
    """Read price IDs from settings at call time (not at import time)."""
    return {
        "starter": {
            "founding": settings.active_stripe_price("starter_founding"),
            "regular":  settings.active_stripe_price("starter_regular"),
        },
        "pro": {
            "founding": settings.active_stripe_price("pro_founding"),
            "regular":  settings.active_stripe_price("pro_regular"),
        },
        "partner": {
            "founding": settings.active_stripe_price("partner"),
            "regular":  settings.active_stripe_price("partner"),
        },
        # Flat-rate — no founding/regular split, both keys point at the same price.
        "annual_lock": {
            "founding": settings.active_stripe_price("annual_lock"),
            "regular":  settings.active_stripe_price("annual_lock"),
        },
    }


def _init_stripe() -> bool:
    """Set Stripe API key. Returns False if not configured."""
    key = settings.active_stripe_secret_key
    if not key:
        logger.debug("Stripe secret key not set — Stripe disabled")
        return False
    stripe.api_key = key.get_secret_value()
    return True


def get_price_id_for_checkout(
    db: Session,
    tier: str,
    vertical: str,
    county_id: str,
    interval: str = "monthly",
) -> tuple[str, bool]:
    """
    Atomically check founding subscriber count and return the correct price_id.

    Uses SELECT FOR UPDATE to prevent race conditions at the 10th subscriber.
    Returns (price_id, is_founding).

    Must be called inside an active DB transaction.
    Raises ValueError for unknown tier or missing price config.
    Raises OperationalError on DB failure (propagated to caller).
    """
    prices = _price_ids()

    # Founder is a flat-rate tier with a monthly/annual split (no founding
    # mechanic, no ZIP-count pricing). Its two prices live on the seeded
    # `plans` rows (founder_monthly / founder_annual), so resolve the Stripe
    # price straight from the catalog by interval rather than from _price_ids.
    if tier == "founder":
        plan_id = f"founder_{'annual' if interval == 'annual' else 'monthly'}"
        price_id = db.execute(
            select(Plan.stripe_price_id).where(Plan.plan_id == plan_id, Plan.is_active.is_(True))
        ).scalar_one_or_none()
        if not price_id:
            raise ValueError(
                f"Stripe price_id not configured for {plan_id}. Seed the plan and set "
                f"STRIPE_PRICE_FOUNDER_{'ANNUAL' if interval == 'annual' else 'MONTHLY'}."
            )
        logger.info("Checkout price selected: tier=founder interval=%s (flat rate)", interval)
        return price_id, False

    # Annual billing for starter/pro — flat-rate, no founding mechanic.
    if interval == "annual" and tier in ("starter", "pro"):
        price_id = settings.active_stripe_price(f"{tier}_annual")
        if not price_id:
            raise ValueError(
                f"Stripe price_id not configured for {tier}_annual. "
                f"Set STRIPE_PRICE_{tier.upper()}_ANNUAL in env."
            )
        logger.info("Checkout price selected: tier=%s interval=annual (flat rate)", tier)
        return price_id, False

    if tier not in prices:
        raise ValueError(
            f"Unknown tier '{tier}'. Valid tiers: {list(prices.keys())}"
        )

    # Partner is a flat-rate tier — no founding mechanic, skip the founding count table
    # (check_founding_tier constraint blocks inserting 'partner' into that table)
    if tier == "partner":
        price_id = prices["partner"]["regular"]
        if not price_id:
            raise ValueError("Stripe price_id not configured for partner. Set STRIPE_PRICE_PARTNER in env.")
        logger.info("Checkout price selected: tier=partner vertical=%s county=%s (flat rate)", vertical, county_id)
        return price_id, False

    # Annual Lock is also flat-rate — no founding mechanic, skip the founding count table
    # (check_founding_tier constraint blocks inserting 'annual_lock' into that table)
    if tier == "annual_lock":
        price_id = prices["annual_lock"]["regular"]
        if not price_id:
            raise ValueError("Stripe price_id not configured for annual_lock. Set STRIPE_PRICE_ANNUAL_LOCK in env.")
        logger.info("Checkout price selected: tier=annual_lock vertical=%s county=%s (flat rate)", vertical, county_id)
        return price_id, False

    # Lock the row for this tier/vertical/county
    stmt = (
        select(FoundingSubscriberCount)
        .where(
            FoundingSubscriberCount.tier == tier,
            FoundingSubscriberCount.vertical == vertical,
            FoundingSubscriberCount.county_id == county_id,
        )
        .with_for_update()
    )
    try:
        row = db.execute(stmt).scalar_one_or_none()
    except OperationalError:
        logger.error(
            "DB error reading founding count for tier=%s vertical=%s county=%s",
            tier, vertical, county_id, exc_info=True,
        )
        raise

    if row is None:
        # First ever subscriber for this combo — create the row
        row = FoundingSubscriberCount(
            tier=tier,
            vertical=vertical,
            county_id=county_id,
            count=0,
        )
        db.add(row)
        try:
            db.flush()
        except OperationalError:
            logger.error(
                "DB error creating founding count row for tier=%s vertical=%s county=%s",
                tier, vertical, county_id, exc_info=True,
            )
            raise

    deadline_at = db.execute(
        select(County.founding_price_deadline_at).where(County.county_id == county_id)
    ).scalar_one_or_none()
    is_founding = row.count < _founding_limit() and (
        deadline_at is None or deadline_at > datetime.now(timezone.utc)
    )
    price_key = "founding" if is_founding else "regular"
    price_id = prices[tier][price_key]

    if not price_id:
        raise ValueError(
            f"Stripe price_id not configured for tier='{tier}' type='{price_key}'. "
            f"Set STRIPE_PRICE_{tier.upper()}_{price_key.upper()} in env."
        )

    logger.info(
        "Checkout price selected: tier=%s vertical=%s county=%s founding=%s count=%d/%d deadline_at=%s",
        tier, vertical, county_id, is_founding, row.count, _founding_limit(), deadline_at,
    )
    return price_id, is_founding


def get_price_id_for_preview(
    db: Session,
    tier: str,
    vertical: str,
    county_id: str,
) -> tuple[str, bool]:
    """
    Non-locking read of the founding price for display/preview purposes only.

    Intentionally does NOT use SELECT FOR UPDATE — this is a read-only
    informational query that must never block concurrent checkout or webhook
    processing.  The returned price is a snapshot of the current founding count
    and is NOT guaranteed: if founding slots fill between this call and the
    subsequent /api/checkout, the checkout will atomically select the (higher)
    regular price.

    Callers should surface ``price_guaranteed: False`` in their response so the
    front-end can set appropriate expectations.

    Returns (price_id, is_founding).
    Raises ValueError for unknown tier or missing price config.
    Raises OperationalError on DB failure (propagated to caller).
    """
    prices = _price_ids()

    # Founder is flat-rate; preview shows the monthly price (interval selection
    # happens at checkout). Resolve from the seeded plans catalog.
    if tier == "founder":
        price_id = db.execute(
            select(Plan.stripe_price_id).where(
                Plan.plan_id == "founder_monthly", Plan.is_active.is_(True)
            )
        ).scalar_one_or_none()
        if not price_id:
            raise ValueError("Stripe price_id not configured for founder_monthly.")
        return price_id, False

    if tier not in prices:
        raise ValueError(
            f"Unknown tier '{tier}'. Valid tiers: {list(prices.keys())}"
        )

    # Partner is flat-rate — no founding mechanic.
    if tier == "partner":
        price_id = prices["partner"]["regular"]
        if not price_id:
            raise ValueError(
                "Stripe price_id not configured for partner. "
                "Set STRIPE_PRICE_PARTNER in env."
            )
        return price_id, False

    # Plain SELECT — no FOR UPDATE — so we never hold a row lock during the
    # subsequent Stripe network call in the offer endpoint.
    try:
        row = db.execute(
            select(FoundingSubscriberCount).where(
                FoundingSubscriberCount.tier == tier,
                FoundingSubscriberCount.vertical == vertical,
                FoundingSubscriberCount.county_id == county_id,
            )
        ).scalar_one_or_none()
    except OperationalError:
        logger.error(
            "DB error reading founding count (preview) tier=%s vertical=%s county=%s",
            tier, vertical, county_id, exc_info=True,
        )
        raise

    # If no row exists yet, no founding subscribers have signed up → founding slots open.
    count = row.count if row is not None else 0
    is_founding = count < _founding_limit()
    price_key = "founding" if is_founding else "regular"
    price_id = prices[tier][price_key]

    if not price_id:
        raise ValueError(
            f"Stripe price_id not configured for tier='{tier}' type='{price_key}'. "
            f"Set STRIPE_PRICE_{tier.upper()}_{price_key.upper()} in env."
        )

    logger.debug(
        "Preview price selected (non-locking): tier=%s vertical=%s county=%s "
        "founding=%s count=%d/%d",
        tier, vertical, county_id, is_founding, count, _founding_limit(),
    )
    return price_id, is_founding


def create_subscription_checkout(
    db: Session,
    tier: str,
    vertical: str,
    county_id: str,
    zip_codes: list[str],
    success_url: str,
    cancel_url: str,
    customer_email: Optional[str] = None,
) -> dict:
    """
    Create a Stripe Checkout Session for a subscription.

    Atomically selects founding vs regular price inside this call.
    Returns the session dict with url and session_id.
    Raises RuntimeError if Stripe is not configured.
    Raises ValueError for config/price issues.
    Raises stripe.error.StripeError on Stripe API failure.
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    price_id, is_founding = get_price_id_for_checkout(db, tier, vertical, county_id)

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            customer_email=customer_email,
            # Collect a phone number on the Stripe payment page so we have it
            # on file for SMS features (Lifecycle SMS, accelerated wallet push,
            # etc.). Stripe validates the number and exposes it in
            # session.customer_details.phone on checkout.session.completed.
            phone_number_collection={"enabled": True},
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "tier": tier,
                "vertical": vertical,
                "county_id": county_id,
                "zip_codes": ",".join(zip_codes),
                "is_founding": str(is_founding),
                "founding_price_id": price_id if is_founding else "",
            },
            subscription_data={
                "metadata": {
                    "tier": tier,
                    "vertical": vertical,
                    "county_id": county_id,
                    "is_founding": str(is_founding),
                }
            },
        )
    except stripe.error.InvalidRequestError as exc:
        logger.warning("Stripe invalid request creating subscription checkout: %s", exc)
        raise
    except stripe.error.AuthenticationError:
        logger.error("Stripe authentication failed — check STRIPE_SECRET_KEY", exc_info=True)
        raise
    except stripe.error.RateLimitError:
        logger.warning("Stripe rate limit hit creating subscription checkout")
        raise
    except stripe.error.StripeError:
        logger.error("Stripe error creating subscription checkout", exc_info=True)
        raise

    logger.info(
        "Stripe checkout session created: %s tier=%s founding=%s",
        session.id, tier, is_founding,
    )
    _log_checkout_started(db, session.id, "subscription", {
        "tier": tier, "vertical": vertical, "county_id": county_id, "is_founding": is_founding,
    })
    return {
        "session_id": session.id,
        "url": session.url,
        "price_id": price_id,
        "is_founding": is_founding,
    }


def create_lead_pack_checkout(
    success_url: str,
    cancel_url: str,
    subscriber_stripe_customer_id: str,
    zip_code: str,
    db: Optional[Session] = None,
) -> dict:
    """
    One-time $99 lead pack — 5 leads, 72hr exclusivity, 15min delivery.
    Raises RuntimeError if Stripe is not configured.
    Raises ValueError if STRIPE_PRICE_LEAD_PACK is not set.
    Raises stripe.error.StripeError on Stripe API failure.

    db is optional and only used to log a "checkouts started" audit row —
    pass the caller's session if one is available; omitting it just skips
    that logging, no other behavior changes.
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    price_lead_pack = settings.stripe_price_lead_pack
    if not price_lead_pack:
        raise ValueError("STRIPE_PRICE_LEAD_PACK not set in env")

    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            customer=subscriber_stripe_customer_id,
            line_items=[{"price": price_lead_pack, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "product": "lead_pack",
                "zip_code": zip_code,
            },
        )
    except stripe.error.InvalidRequestError as exc:
        logger.warning(
            "Stripe invalid request creating lead pack checkout for customer %s: %s",
            subscriber_stripe_customer_id, exc,
        )
        raise
    except stripe.error.StripeError:
        logger.error(
            "Stripe error creating lead pack checkout for customer %s",
            subscriber_stripe_customer_id, exc_info=True,
        )
        raise

    _log_checkout_started(db, session.id, "lead_pack", {"zip_code": zip_code})
    return {"session_id": session.id, "url": session.url}


def create_hot_lead_unlock_link(
    subscriber_stripe_customer_id: str,
    lead_id: str,
    reduced: bool = False,
    customer_email: Optional[str] = None,
    db: Optional[Session] = None,
    feed_uuid: Optional[str] = None,
) -> dict:
    """
    Dynamic one-time Stripe payment link for hot lead unlock.
    $150 standard. Drops to $99 if unlock rate is low (reduced=True).
    Expires 23hr after creation (Stripe hard-caps checkout expires_at at 24hr).
    Raises RuntimeError if Stripe is not configured.
    Raises ValueError if required price env vars are not set.
    Raises stripe.error.StripeError on Stripe API failure.

    db is optional and only used to log a "checkouts started" audit row —
    pass the caller's session if one is available; omitting it just skips
    that logging, no other behavior changes.
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    price_hot_lead_unlock = settings.active_stripe_price("hot_lead_unlock")
    if not price_hot_lead_unlock:
        raise ValueError("STRIPE_PRICE_HOT_LEAD_UNLOCK not set in env")

    if reduced and not settings.stripe_price_lead_pack:
        raise ValueError("STRIPE_PRICE_LEAD_PACK not set in env (required for reduced rate)")

    # Use lead pack price ($99) as the reduced rate
    price = settings.stripe_price_lead_pack if reduced else price_hot_lead_unlock

    # There is no standalone /leads/:id route in the SPA — send the visitor
    # back to their dashboard (if known) so the completed unlock actually
    # lands somewhere real and can fire its GA4 completion event there.
    unlock_amount = 99 if reduced else 150
    success_url = (
        f"{settings.app_base_url}/dashboard/{feed_uuid}"
        f"?hot_unlock=true&lead_id={lead_id}&price={unlock_amount}"
        if feed_uuid
        else f"{settings.app_base_url}/leads/{lead_id}?unlocked=true"
    )
    cancel_url = (
        f"{settings.app_base_url}/dashboard/{feed_uuid}"
        if feed_uuid
        else f"{settings.app_base_url}/leads/{lead_id}"
    )

    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            customer=subscriber_stripe_customer_id,
            line_items=[{"price": price, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            expires_at=int(time.time()) + 23 * 3600,  # Stripe caps expires_at at 24hr from creation
            metadata={
                "product": "hot_lead_unlock",
                "lead_id": lead_id,
                "reduced_rate": str(reduced),
            },
            # Session metadata is NOT copied to the PaymentIntent by Stripe.
            # Fulfillment (_on_payment_intent_succeeded) routes by PI metadata,
            # so it must be set explicitly here or the payment is treated as a
            # bare card-save and no delivery record is created.
            payment_intent_data={
                "metadata": {
                    "product": "hot_lead_unlock",
                    "property_id": lead_id,
                    "reduced_rate": str(reduced),
                },
                # Stripe emails an itemized receipt on success.
                **({"receipt_email": customer_email} if customer_email else {}),
            },
        )
    except stripe.error.InvalidRequestError as exc:
        logger.warning(
            "Stripe invalid request creating hot lead unlock for lead %s: %s",
            lead_id, exc,
        )
        raise
    except stripe.error.StripeError:
        logger.error(
            "Stripe error creating hot lead unlock for lead %s",
            lead_id, exc_info=True,
        )
        raise

    _log_checkout_started(db, session.id, "hot_lead_unlock", {
        "lead_id": lead_id, "reduced_rate": reduced,
    })
    return {"session_id": session.id, "url": session.url}


# Subscriber.status values that block any plan switch. The user must clear
# the underlying state first (update card, end pause, exit dispute) before
# we'll send a proration request to Stripe — otherwise we get a confusing
# 502 cascade through the failed-payment recovery path.
_BLOCKED_SWITCH_STATUSES = frozenset({"grace", "churned", "cancelled", "paused", "disputed"})


def can_switch_subscription(subscriber) -> tuple[bool, str | None]:
    """Pre-flight gate for any plan change (annual lock, AP Pro upgrade, data-only).

    Returns (True, None) if the switch can proceed, otherwise (False, reason)
    where reason is the subscriber's current status. Callers should surface
    a 409 response that points the user to the billing portal so they can
    clear the underlying state before retrying.
    """
    if subscriber is None:
        return False, "missing"
    status = (getattr(subscriber, "status", None) or "").lower()
    if status in _BLOCKED_SWITCH_STATUSES:
        return False, status
    return True, None


def switch_subscription_plan(
    subscription_id: str,
    new_price_id: str,
    prorate: bool = True,
) -> dict:
    """
    Switch an existing Stripe subscription to a new price (annual lock, data-only, etc.).

    Uses proration by default so the subscriber is charged/credited the difference
    immediately. Set prorate=False for end-of-period switches.

    Returns the updated Stripe subscription object dict.
    Raises RuntimeError if Stripe is not configured.
    Raises stripe.error.StripeError on API failure.
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    try:
        subscription = stripe.Subscription.retrieve(subscription_id)
    except stripe.error.InvalidRequestError as exc:
        logger.error("switch_subscription_plan: subscription %s not found: %s", subscription_id, exc)
        raise

    # Use bracket access — subscription["items"] avoids the dict.items() builtin collision
    # in Stripe SDK v11+ where StripeObject inherits from dict.
    items_obj = subscription.get("items")
    items = list(items_obj.data) if items_obj else []
    if not items:
        raise ValueError(f"Subscription {subscription_id} has no items")

    item_id = items[0].id

    proration_behavior = "create_prorations" if prorate else "none"

    try:
        updated = stripe.Subscription.modify(
            subscription_id,
            items=[{"id": item_id, "price": new_price_id}],
            proration_behavior=proration_behavior,
        )
    except stripe.error.InvalidRequestError as exc:
        logger.error(
            "switch_subscription_plan: invalid request for %s → %s: %s",
            subscription_id, new_price_id, exc,
        )
        raise
    except stripe.error.StripeError:
        logger.error(
            "switch_subscription_plan: Stripe error for %s → %s",
            subscription_id, new_price_id, exc_info=True,
        )
        raise

    logger.info(
        "Subscription %s switched to price %s (prorate=%s)",
        subscription_id, new_price_id, prorate,
    )
    return updated.to_dict()


def create_subscription_off_saved_pm(
    subscriber,
    tier: str,
    offer_id: int,
) -> dict:
    """Create a wallet Subscription off the subscriber's saved payment method,
    off-session. Wallet credits land on the webhook (invoice.payment_succeeded);
    do NOT credit here.

    `payment_behavior="default_incomplete"` ensures we don't recognise revenue
    until the first invoice clears. If 3DS / authentication is needed, the
    returned `client_secret` and `requires_action=True` let the caller send a
    fallback SMS with a Checkout URL.

    Used by:
      - `wallet_engine.activate_via_saved_card` (SMS reply WALLET / YES / TOPUP)
      - `POST /api/wallet/accept-accelerated-offer/{uuid}` (in-app modal)
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    if not subscriber.stripe_customer_id or not subscriber.stripe_payment_method_id:
        raise ValueError("subscriber missing stripe_customer_id or payment_method_id")

    # Wallet price IDs are keyed `wallet_{starter|growth|power}`; tier name in
    # config.revenue_ladder is `starter_wallet|growth|power`. Map both forms.
    suffix = tier.replace("_wallet", "")  # 'starter_wallet'→'starter'; 'growth'→'growth'
    price_name = f"wallet_{suffix}"
    price_id = settings.active_stripe_price(price_name)
    if not price_id:
        raise ValueError(
            f"Stripe price not configured for tier '{tier}' "
            f"(expected env STRIPE_PRICE_{price_name.upper()})"
        )

    try:
        # `allow_incomplete` = Stripe attempts the off-session charge against
        # the saved PM immediately. If it succeeds, invoice.payment_succeeded
        # fires and the webhook activates the wallet. If 3DS / authentication
        # is required, the subscription lands in `incomplete` and we surface
        # `requires_action=True` so the caller can SMS a fallback link.
        sub = stripe.Subscription.create(
            customer=subscriber.stripe_customer_id,
            items=[{"price": price_id, "quantity": 1}],
            default_payment_method=subscriber.stripe_payment_method_id,
            off_session=True,
            payment_behavior="allow_incomplete",
            metadata={
                "product": "wallet_subscription",
                "subscriber_id": str(subscriber.id),
                "wallet_offer_id": str(offer_id),
                "tier": tier,
            },
        )
    except stripe.error.CardError as exc:
        logger.warning(
            "Wallet subscription card error for sub=%s offer=%s: %s",
            subscriber.id, offer_id, exc,
        )
        return {
            "subscription_id": None,
            "status": "failed",
            "requires_action": True,
            "error_code": getattr(exc, "code", "card_error"),
            "error_message": str(exc),
        }
    except stripe.error.StripeError:
        logger.error(
            "Stripe error creating wallet subscription sub=%s offer=%s",
            subscriber.id, offer_id, exc_info=True,
        )
        raise

    # `incomplete` subscription status means the off-session charge couldn't
    # complete without further action (3DS / requires_action). The webhook
    # will not fire `invoice.payment_succeeded` until the customer authenticates,
    # so we tell the caller to send a fallback link.
    requires_action = sub.status == "incomplete"

    logger.info(
        "Wallet subscription created sub=%s offer=%s id=%s status=%s requires_action=%s",
        subscriber.id, offer_id, sub.id, sub.status, requires_action,
    )

    return {
        "subscription_id": sub.id,
        "status": sub.status,
        "requires_action": requires_action,
    }


def get_founding_spots_remaining(
    db: Session,
    tier: str,
    vertical: str,
    county_id: str,
) -> int:
    """
    Returns how many founding spots remain for a tier/vertical/county.
    Used by the landing page /api/founding-spots endpoint.
    Raises OperationalError on DB failure (propagated to caller).
    """
    try:
        row = db.execute(
            select(FoundingSubscriberCount).where(
                FoundingSubscriberCount.tier == tier,
                FoundingSubscriberCount.vertical == vertical,
                FoundingSubscriberCount.county_id == county_id,
            )
        ).scalar_one_or_none()
    except OperationalError:
        logger.error(
            "DB error reading founding spots for tier=%s vertical=%s county=%s",
            tier, vertical, county_id, exc_info=True,
        )
        raise

    if row is None:
        return _founding_limit()

    return max(0, _founding_limit() - row.count)


def issue_guarantee_credit(
    stripe_customer_id: str, credit_cents: int, *, description: str, idempotency_key: str,
) -> str:
    """
    Apply a tiered-volume-guarantee shortfall credit to a customer's Stripe
    balance (negative balance transaction = owed less on the next invoice).

    idempotency_key must be deterministic per (subscriber, period) — callers
    retrying a crashed or failed attempt for the same cycle must pass the same
    key so Stripe returns the original transaction instead of crediting twice.

    Raises RuntimeError if Stripe isn't configured, or stripe.StripeError on
    an API failure — callers decide how to record/report a failed credit.
    Returns the balance transaction id.
    """
    if not _init_stripe():
        raise RuntimeError("Stripe secret key not configured — cannot issue guarantee credit")

    txn = stripe.Customer.create_balance_transaction(
        stripe_customer_id,
        amount=-abs(credit_cents),
        currency="usd",
        description=description,
        idempotency_key=idempotency_key,
    )
    return txn["id"]
