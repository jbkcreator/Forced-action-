"""
Payment Sheet service — Item 26 (backend).

Creates Stripe PaymentIntents for the Payment Sheet SDK (micro-payments).
When save_card=True, sets setup_future_usage='off_session' which triggers the
payment_intent.succeeded webhook → _on_card_saved in stripe_webhooks.py.

Used for:
  - Wallet top-ups
  - Bundle purchases
  - Lead unlocks
  - Any one-time micro-payment from the mobile/web UI
"""
import logging
from typing import Optional

import stripe
from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import settings

logger = logging.getLogger(__name__)


def create_payment_intent(
    subscriber_id: int,
    amount_cents: int,
    description: str,
    save_card: bool,
    db: Session,
    metadata: Optional[dict] = None,
) -> dict:
    """
    Create a Stripe PaymentIntent for the Payment Sheet.

    Returns:
        {
            "client_secret": str,    # passed to Stripe SDK on frontend
            "payment_intent_id": str,
            "amount": int,           # in cents
            "save_card": bool,
            "publishable_key": str,  # convenience — frontend needs this too
        }

    Raises RuntimeError if Stripe is not configured.
    Raises ValueError if subscriber not found.
    Raises stripe.error.StripeError on API failure.
    """
    from src.core.models import Subscriber

    key = settings.active_stripe_secret_key
    if not key:
        raise RuntimeError("Stripe not configured — set STRIPE_SECRET_KEY or STRIPE_TEST_SECRET_KEY")
    stripe.api_key = key.get_secret_value()

    sub = db.execute(
        select(Subscriber).where(Subscriber.id == subscriber_id)
    ).scalar_one_or_none()
    if sub is None:
        raise ValueError(f"Subscriber {subscriber_id} not found")

    params: dict = {
        "amount": amount_cents,
        "currency": "usd",
        "description": description,
        "metadata": {
            "subscriber_id": str(subscriber_id),
            **(metadata or {}),
        },
    }

    if sub.stripe_customer_id:
        params["customer"] = sub.stripe_customer_id

    if save_card:
        params["setup_future_usage"] = "off_session"
        if sub.stripe_payment_method_id:
            params["payment_method"] = sub.stripe_payment_method_id

    try:
        pi = stripe.PaymentIntent.create(**params)
    except stripe.error.StripeError:
        logger.error(
            "PaymentIntent creation failed: subscriber=%d amount=%d",
            subscriber_id, amount_cents, exc_info=True,
        )
        raise

    logger.info(
        "PaymentIntent created: id=%s amount=%d subscriber=%d save_card=%s",
        pi.id, amount_cents, subscriber_id, save_card,
    )
    return {
        "client_secret": pi.client_secret,
        "payment_intent_id": pi.id,
        "amount": amount_cents,
        "save_card": save_card,
        "publishable_key": settings.active_stripe_publishable_key,
    }


def get_publishable_key() -> Optional[str]:
    """Return the mode-aware Stripe publishable key for frontend initialisation."""
    return settings.active_stripe_publishable_key
