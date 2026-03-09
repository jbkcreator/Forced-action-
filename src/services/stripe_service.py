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
import uuid
from typing import Optional

import stripe
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import FoundingSubscriberCount

logger = logging.getLogger(__name__)

FOUNDING_LIMIT = 10  # founding rate available for first 10 subscribers per tier/vertical/county

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
            "founding": settings.stripe_price_starter_founding,
            "regular":  settings.stripe_price_starter_regular,
        },
        "pro": {
            "founding": settings.stripe_price_pro_founding,
            "regular":  settings.stripe_price_pro_regular,
        },
        "dominator": {
            "founding": settings.stripe_price_dominator_founding,
            "regular":  settings.stripe_price_dominator_regular,
        },
    }


def _init_stripe() -> bool:
    """Set Stripe API key. Returns False if not configured."""
    key = settings.stripe_secret_key
    if not key:
        logger.debug("STRIPE_SECRET_KEY not set — Stripe disabled")
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
    """
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
    row = db.execute(stmt).scalar_one_or_none()

    if row is None:
        # First ever subscriber for this combo — create the row
        row = FoundingSubscriberCount(
            tier=tier,
            vertical=vertical,
            county_id=county_id,
            count=0,
        )
        db.add(row)
        db.flush()

    is_founding = row.count < FOUNDING_LIMIT
    price_key = "founding" if is_founding else "regular"
    price_id = _price_ids()[tier][price_key]

    if not price_id:
        raise ValueError(
            f"Stripe price_id not configured for tier='{tier}' type='{price_key}'. "
            f"Set STRIPE_PRICE_{tier.upper()}_{price_key.upper()} in env."
        )

    logger.info(
        f"Checkout price selected: tier={tier} vertical={vertical} county={county_id} "
        f"founding={is_founding} count={row.count}/{FOUNDING_LIMIT}"
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
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    price_id, is_founding = get_price_id_for_checkout(db, tier, vertical, county_id)

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

    logger.info(f"Stripe checkout session created: {session.id} tier={tier} founding={is_founding}")
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
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    price_lead_pack = settings.stripe_price_lead_pack
    if not price_lead_pack:
        raise ValueError("STRIPE_PRICE_LEAD_PACK not set in env")

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
    """
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")

    price_hot_lead_unlock = settings.stripe_price_hot_lead_unlock
    if not price_hot_lead_unlock:
        raise ValueError("STRIPE_PRICE_HOT_LEAD_UNLOCK not set in env")

    # Use lead pack price ($99) as the reduced rate
    price = settings.stripe_price_lead_pack if reduced else price_hot_lead_unlock

    session = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        customer=subscriber_stripe_customer_id,
        line_items=[{"price": price, "quantity": 1}],
        success_url=f"https://app.forcedaction.io/leads/{lead_id}?unlocked=true",
        cancel_url=f"https://app.forcedaction.io/leads/{lead_id}",
        expires_at=int(__import__("time").time()) + 48 * 3600,  # 48hr expiry
        metadata={
            "product": "hot_lead_unlock",
            "lead_id": lead_id,
            "reduced_rate": str(reduced),
        },
    )

    return {"session_id": session.id, "url": session.url}


def get_founding_spots_remaining(
    db: Session,
    tier: str,
    vertical: str,
    county_id: str,
) -> int:
    """
    Returns how many founding spots remain for a tier/vertical/county.
    Used by the landing page /api/founding-spots endpoint.
    """
    row = db.execute(
        select(FoundingSubscriberCount).where(
            FoundingSubscriberCount.tier == tier,
            FoundingSubscriberCount.vertical == vertical,
            FoundingSubscriberCount.county_id == county_id,
        )
    ).scalar_one_or_none()

    if row is None:
        return FOUNDING_LIMIT

    return max(0, FOUNDING_LIMIT - row.count)
