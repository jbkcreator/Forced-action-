"""
Idempotent milestone grant handlers for the Referral Core Loop.

Three public functions, each safe to call multiple times:
- grant_per_referral_credits  — 5 credits per confirmed referral
- grant_free_month            — Stripe coupon on 3rd referral milestone
- grant_lock_slot             — +1 bonus ZIP slot on 5th referral milestone
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import stripe
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.core.models import ReferralMilestoneAward, Subscriber
from src.services.wallet_engine import credit

logger = logging.getLogger(__name__)

PER_REFERRAL_CREDITS = 5


def grant_per_referral_credits(
    referrer_id: int,
    referral_event_id: int,
    db: Session,
) -> None:
    """
    Credit 5 credits to referrer for a confirmed referral.
    Idempotent via wallet ledger — the same event_id produces a distinct
    description that wallet_engine dedupes by not re-running if called again
    within the same DB transaction (the status transition guards it upstream).
    """
    credit(
        referrer_id,
        PER_REFERRAL_CREDITS,
        f"referral_reward:event:{referral_event_id}",
        db,
    )
    logger.info("[ReferralGrants] credited %d credits to subscriber=%d event=%d",
                PER_REFERRAL_CREDITS, referrer_id, referral_event_id)


def grant_free_month(
    referrer_id: int,
    triggering_event_id: int,
    db: Session,
) -> ReferralMilestoneAward:
    """
    Apply a one-time Stripe free-month coupon to the referrer's subscription.

    Idempotent: UNIQUE(referrer_subscriber_id, milestone) prevents double-grant.
    On IntegrityError (concurrent duplicate insert), returns the existing row.

    Requires settings.referral_free_month_coupon_id to be set.
    """
    settings = get_settings()
    coupon_id = getattr(settings, "referral_free_month_coupon_id", None)

    sp = db.begin_nested()
    try:
        award = ReferralMilestoneAward(
            referrer_subscriber_id=referrer_id,
            milestone="free_month_3",
            awarded_at=datetime.now(timezone.utc),
            triggering_referral_event_id=triggering_event_id,
        )
        db.add(award)
        db.flush()
        sp.commit()
    except IntegrityError:
        sp.rollback()
        from sqlalchemy import select
        award = db.execute(
            select(ReferralMilestoneAward).where(
                ReferralMilestoneAward.referrer_subscriber_id == referrer_id,
                ReferralMilestoneAward.milestone == "free_month_3",
            )
        ).scalar_one()
        logger.info("[ReferralGrants] free_month_3 already awarded to subscriber=%d", referrer_id)
        return award

    # Apply Stripe coupon — runs outside the savepoint so a Stripe failure
    # does NOT roll back the milestone-award row (we log and surface the error).
    if coupon_id:
        grant_ref = _apply_stripe_coupon(referrer_id, coupon_id, db)
        award.grant_ref = grant_ref
        db.flush()
    else:
        logger.warning("[ReferralGrants] referral_free_month_coupon_id not configured; "
                       "skipping Stripe coupon for subscriber=%d", referrer_id)

    logger.info("[ReferralGrants] free_month_3 granted to subscriber=%d grant_ref=%s",
                referrer_id, award.grant_ref)
    return award


def grant_lock_slot(
    referrer_id: int,
    triggering_event_id: int,
    db: Session,
) -> ReferralMilestoneAward:
    """
    Increment bonus_zip_slots by 1 for the referrer.

    Idempotent: UNIQUE(referrer_subscriber_id, milestone) prevents double-grant.
    On IntegrityError (concurrent duplicate insert), returns the existing row.
    """
    sp = db.begin_nested()
    try:
        award = ReferralMilestoneAward(
            referrer_subscriber_id=referrer_id,
            milestone="lock_slot_5",
            awarded_at=datetime.now(timezone.utc),
            triggering_referral_event_id=triggering_event_id,
        )
        db.add(award)
        db.flush()
        sp.commit()
    except IntegrityError:
        sp.rollback()
        from sqlalchemy import select
        award = db.execute(
            select(ReferralMilestoneAward).where(
                ReferralMilestoneAward.referrer_subscriber_id == referrer_id,
                ReferralMilestoneAward.milestone == "lock_slot_5",
            )
        ).scalar_one()
        logger.info("[ReferralGrants] lock_slot_5 already awarded to subscriber=%d", referrer_id)
        return award

    db.execute(
        update(Subscriber)
        .where(Subscriber.id == referrer_id)
        .values(bonus_zip_slots=Subscriber.bonus_zip_slots + 1)
    )
    db.flush()
    logger.info("[ReferralGrants] lock_slot_5 granted to subscriber=%d", referrer_id)
    return award


# ── internal helpers ──────────────────────────────────────────────────────────

def _apply_stripe_coupon(referrer_id: int, coupon_id: str, db: Session) -> str:
    """
    Apply coupon_id to the referrer's active Stripe subscription.
    Falls back to attaching the coupon to the Customer if no active subscription.
    Returns a grant_ref string describing what was done.
    """
    settings = get_settings()
    stripe.api_key = settings.active_stripe_secret_key.get_secret_value()

    sub = db.get(Subscriber, referrer_id)
    if not sub:
        raise ValueError(f"Subscriber {referrer_id} not found")

    # Stripe API 2026-04-22 (Dahlia): subscriptions on billing_mode=flexible
    # reject legacy `coupon=`. The supported path is:
    #   1. Create a PromotionCode wrapping the coupon (new shape:
    #      promotion={"type":"coupon","coupon":id}).
    #   2. Apply via discounts=[{"promotion_code": promo_id}].
    def _ensure_promo_code() -> str:
        return stripe.PromotionCode.create(
            promotion={"type": "coupon", "coupon": coupon_id},
            max_redemptions=1,
        ).id

    if sub.stripe_subscription_id:
        try:
            stripe.Subscription.modify(sub.stripe_subscription_id, coupon=coupon_id)
            return f"sub:{sub.stripe_subscription_id}:{coupon_id}"
        except stripe.InvalidRequestError as exc:
            if "coupon" in str(exc) and "not supported" in str(exc):
                promo_id = _ensure_promo_code()
                stripe.Subscription.modify(
                    sub.stripe_subscription_id,
                    discounts=[{"promotion_code": promo_id}],
                )
                return f"sub_promo:{sub.stripe_subscription_id}:{promo_id}"
            raise
    elif sub.stripe_customer_id:
        try:
            stripe.Customer.modify(sub.stripe_customer_id, coupon=coupon_id)
            return f"cus:{sub.stripe_customer_id}:{coupon_id}"
        except stripe.InvalidRequestError as exc:
            if "coupon" in str(exc) and "not supported" in str(exc):
                promo_id = _ensure_promo_code()
                stripe.Customer.modify(
                    sub.stripe_customer_id,
                    discounts=[{"promotion_code": promo_id}],
                )
                return f"cus_promo:{sub.stripe_customer_id}:{promo_id}"
            raise
    else:
        logger.warning("[ReferralGrants] subscriber=%d has no stripe ids; coupon not applied", referrer_id)
        return f"no_stripe_id:{coupon_id}"
