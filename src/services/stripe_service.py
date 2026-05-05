"""
Stripe integration — M1-B.

Handles:
  - Founding vs regular price selection (atomic SELECT FOR UPDATE)
  - Checkout session creation
  - One-time payments (lead pack, hot lead unlock)

Pricing table:
  Tier        | Founding | Regular | Future (6mo)
  ------------|----------|---------|-------------
  starter     | $600/mo  | $800/mo | $1,100/mo
  pro         | $1,100   | $1,500  | $1,900
  dominator   | $2,000   | $2,800  | $3,500

Every product has TWO Stripe price objects: founding_price_id + regular_price_id.
Founding rate is selected atomically at checkout and locked forever.
"""

import logging
import time
from typing import Optional

import stripe
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import FoundingSubscriberCount

logger = logging.getLogger(__name__)

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
        "dominator": {
            "founding": settings.active_stripe_price("dominator_founding"),
            "regular":  settings.active_stripe_price("dominator_regular"),
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
    if tier not in prices:
        raise ValueError(
            f"Unknown tier '{tier}'. Valid tiers: {list(prices.keys())}"
        )

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

    is_founding = row.count < _founding_limit()
    price_key = "founding" if is_founding else "regular"
    price_id = prices[tier][price_key]

    if not price_id:
        raise ValueError(
            f"Stripe price_id not configured for tier='{tier}' type='{price_key}'. "
            f"Set STRIPE_PRICE_{tier.upper()}_{price_key.upper()} in env."
        )

    logger.info(
        "Checkout price selected: tier=%s vertical=%s county=%s founding=%s count=%d/%d",
        tier, vertical, county_id, is_founding, row.count, _founding_limit(),
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
) -> dict:
    """
    One-time $99 lead pack — 5 leads, 72hr exclusivity, 15min delivery.
    Raises RuntimeError if Stripe is not configured.
    Raises ValueError if STRIPE_PRICE_LEAD_PACK is not set.
    Raises stripe.error.StripeError on Stripe API failure.
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

    return {"session_id": session.id, "url": session.url}


def create_hot_lead_unlock_link(
    subscriber_stripe_customer_id: str,
    lead_id: str,
    reduced: bool = False,
) -> dict:
    """
    Dynamic one-time Stripe payment link for hot lead unlock.
    $150 standard. Drops to $99 if unlock rate is low (reduced=True).
    Expires 48hr after creation.
    Raises RuntimeError if Stripe is not configured.
    Raises ValueError if required price env vars are not set.
    Raises stripe.error.StripeError on Stripe API failure.
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

    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            customer=subscriber_stripe_customer_id,
            line_items=[{"price": price, "quantity": 1}],
            success_url=f"{settings.app_base_url}/leads/{lead_id}?unlocked=true",
            cancel_url=f"{settings.app_base_url}/leads/{lead_id}",
            expires_at=int(time.time()) + 48 * 3600,  # 48hr expiry
            metadata={
                "product": "hot_lead_unlock",
                "lead_id": lead_id,
                "reduced_rate": str(reduced),
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

    # Find the current subscription item ID
    items = subscription.get("items", {}).get("data", [])
    if not items:
        raise ValueError(f"Subscription {subscription_id} has no items")

    item_id = items[0]["id"]

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
    return dict(updated)


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
