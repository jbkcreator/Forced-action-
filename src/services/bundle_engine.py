"""
Bundle engine — weekend/storm/zip_booster/monthly_reload one-time purchases.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import stripe
from sqlalchemy import and_, desc, select
from sqlalchemy.orm import Session

from config.revenue_ladder import BUNDLES
from config.settings import settings
from src.core.models import BundlePurchase, DistressScore, Property, Subscriber

logger = logging.getLogger(__name__)


def is_available(bundle_type: str, subscriber_id: int, db: Session) -> bool:
    if bundle_type == "weekend":
        return datetime.now(timezone.utc).weekday() >= 4  # Fri=4, Sat=5, Sun=6
    if bundle_type == "storm":
        sub = db.get(Subscriber, subscriber_id)
        if not sub:
            return False
        zip_codes = _get_subscriber_zips(sub, db)
        from src.core.redis_client import redis_available, rget
        if not redis_available():
            return False
        return any(rget(f"storm_active:{z}") for z in zip_codes)
    if bundle_type in ("zip_booster", "monthly_reload"):
        return True
    return False


def create_payment_intent(
    bundle_type: str,
    subscriber_id: int,
    zip_code: str,
    vertical: str,
    db: Session,
) -> dict:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        raise ValueError(f"Subscriber {subscriber_id} not found")

    bundle_config = BUNDLES.get(bundle_type)
    if not bundle_config:
        raise ValueError(f"Unknown bundle type: {bundle_type}")

    key = settings.active_stripe_secret_key
    if not key:
        raise RuntimeError("Stripe not configured")
    stripe.api_key = key.get_secret_value()

    pi = stripe.PaymentIntent.create(
        amount=bundle_config["price_cents"],
        currency="usd",
        customer=sub.stripe_customer_id,
        setup_future_usage="off_session",
        metadata={
            "product": "bundle",
            "bundle_type": bundle_type,
            "subscriber_id": str(subscriber_id),
            "zip_code": zip_code,
            "vertical": vertical,
        },
    )
    return {
        "client_secret": pi.client_secret,
        "amount": bundle_config["price_cents"],
        "bundle_type": bundle_type,
    }


def deliver(bundle_purchase_id: int, db: Session) -> BundlePurchase:
    purchase = db.get(BundlePurchase, bundle_purchase_id)
    if not purchase:
        raise ValueError(f"BundlePurchase {bundle_purchase_id} not found")

    now = datetime.now(timezone.utc)
    bundle_config = BUNDLES.get(purchase.bundle_type, {})

    if purchase.bundle_type in ("storm", "zip_booster"):
        leads = _select_top_leads(purchase.zip_code, purchase.vertical, purchase.county_id, db)
        purchase.lead_ids = [p.id for p in leads]
        duration_hours = bundle_config.get("duration_hours", 72)
        purchase.expires_at = now + timedelta(hours=duration_hours)
    elif purchase.bundle_type == "monthly_reload":
        credits = bundle_config.get("credits", 30)
        from src.services.wallet_engine import credit
        credit(
            purchase.subscriber_id,
            credits,
            f"bundle_monthly_reload",
            db,
            stripe_charge_id=purchase.stripe_payment_intent_id,
        )
        purchase.credits_awarded = credits
        purchase.expires_at = now + timedelta(days=30)
    elif purchase.bundle_type == "weekend":
        leads_count = bundle_config.get("leads", 5)
        leads = _select_top_leads(purchase.zip_code, purchase.vertical, purchase.county_id, db, limit=leads_count)
        purchase.lead_ids = [p.id for p in leads]
        # Weekend bundle expires Sunday midnight
        days_until_sunday = (6 - now.weekday()) % 7 or 7
        purchase.expires_at = now + timedelta(days=days_until_sunday)

    purchase.status = "active"
    db.flush()
    logger.info("Bundle delivered: purchase=%d type=%s leads=%s", purchase.id, purchase.bundle_type, purchase.lead_ids)
    return purchase


def expire_stale(db: Session) -> int:
    from sqlalchemy import update
    now = datetime.now(timezone.utc)
    result = db.execute(
        select(BundlePurchase).where(
            BundlePurchase.expires_at < now,
            BundlePurchase.status == "active",
        )
    ).scalars().all()
    for p in result:
        p.status = "expired"
    db.flush()
    logger.info("Expired %d stale bundle purchases", len(result))
    return len(result)


def _select_top_leads(
    zip_code: Optional[str],
    vertical: Optional[str],
    county_id: str,
    db: Session,
    limit: int = 10,
) -> list:
    if not zip_code or not vertical:
        return []
    try:
        score_col = DistressScore.vertical_scores[vertical].as_float()
    except Exception:
        return []
    rows = db.execute(
        select(Property)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .where(
            and_(
                Property.zip == zip_code,
                Property.county_id == county_id,
                DistressScore.qualified == True,  # noqa: E712
            )
        )
        .order_by(desc(score_col))
        .limit(limit)
    ).scalars().all()
    return list(rows)


def _get_subscriber_zips(sub: Subscriber, db: Session) -> list[str]:
    from src.core.models import ZipTerritory
    territories = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.subscriber_id == sub.id,
            ZipTerritory.status == "locked",
        )
    ).scalars().all()
    return [t.zip_code for t in territories]
