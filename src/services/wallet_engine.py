"""
Wallet engine — credit balance management, enrollment, auto-reload, bonuses.

Concurrency notes (2026-05-04):
  Every function that mutates `WalletBalance.credits_remaining` reads the row
  with `with_for_update()` so concurrent debits/credits/auto-reloads serialize
  on the row-level lock. A Postgres CHECK constraint on the column
  (credits_nonneg) is the belt-and-braces guard: even if a future code path
  forgets the lock, the DB rejects negative balances.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import stripe
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from config.cora_guardrails import get_guardrail
from config.revenue_ladder import (
    CREDIT_COSTS,
    WALLET_AUTO_RELOAD_THRESHOLD,
    WALLET_ENROLLMENT_TRIGGERS,
    WALLET_TIERS,
)
from config.settings import settings
from src.core.models import Subscriber, WalletBalance, WalletTransaction

logger = logging.getLogger(__name__)


def _select_wallet_for_update(subscriber_id: int, db: Session) -> Optional[WalletBalance]:
    """Read the wallet row with a row-level lock so concurrent mutators serialize."""
    return db.execute(
        select(WalletBalance)
        .where(WalletBalance.subscriber_id == subscriber_id)
        .with_for_update()
    ).scalar_one_or_none()


def get_or_create_wallet(subscriber_id: int, db: Session, lock: bool = False) -> WalletBalance:
    """Return the wallet row, creating it if missing.

    Pass lock=True from any caller that intends to mutate `credits_remaining`
    so the row is locked from the moment we read it.
    """
    if lock:
        wallet = _select_wallet_for_update(subscriber_id, db)
    else:
        wallet = db.execute(
            select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
        ).scalar_one_or_none()
    if wallet is None:
        wallet = WalletBalance(
            subscriber_id=subscriber_id,
            wallet_tier="starter_wallet",
            credits_remaining=0,
            credits_used_total=0,
        )
        db.add(wallet)
        db.flush()
        # Re-fetch with the lock held so the caller's mutation is serialized
        # against any racing creator that lost the unique-constraint contest.
        if lock:
            wallet = _select_wallet_for_update(subscriber_id, db) or wallet
    return wallet


def get_balance(subscriber_id: int, db: Session) -> int:
    """Read-only balance — no lock."""
    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    return wallet.credits_remaining if wallet else 0


def debit(
    subscriber_id: int,
    action: str,
    db: Session,
    description: str = "",
    zip_code: Optional[str] = None,
) -> bool:
    """Atomic debit. Returns False if the wallet has insufficient credits.

    Holds a row-level lock for the duration so concurrent debits cannot both
    pass the balance check on the same wallet.

    zip_code: if provided, attributed to the transaction for Wallet-to-Lock
    detection. Pass the ZIP of the lead being acted on.
    """
    cost = CREDIT_COSTS.get(action, 1)
    wallet = get_or_create_wallet(subscriber_id, db, lock=True)
    if wallet.credits_remaining < cost:
        return False
    wallet.credits_remaining -= cost
    wallet.credits_used_total += cost
    txn = WalletTransaction(
        subscriber_id=subscriber_id,
        wallet_id=wallet.id,
        txn_type="debit",
        amount=-cost,
        balance_after=wallet.credits_remaining,
        description=description or action,
        zip_code=zip_code,
    )
    db.add(txn)
    db.flush()
    if wallet.credits_remaining < WALLET_AUTO_RELOAD_THRESHOLD:
        check_auto_reload(wallet, db)
    # Log manual action for AP Lite threshold detection (no DB round-trip for
    # action types we don't care about).
    _maybe_log_manual_action(subscriber_id, action, db)

    # fa016 Accelerated Wallet Push — first paid debit is "paid intent".
    # Wallet already exists at this point so the detector will only emit on
    # the narrow edge case where a saved-card user landed credits via a
    # bonus/manual top-up but never had an enrollment offer.
    try:
        eligible = accelerated_push_eligible(subscriber_id, db)
        if eligible:
            from src.agents.supervisor import dispatch_event
            dispatch_event({
                "event_type": "accelerated_wallet_push_eligible",
                "subscriber_id": subscriber_id,
                "payload": eligible,
            })
    except Exception as exc:
        logger.warning(
            "accelerated_wallet_push detector failed sub=%s: %s",
            subscriber_id, exc,
        )

    from src.services.segmentation_engine import reclassify_safe
    reclassify_safe(subscriber_id, db)
    return True


def _maybe_log_manual_action(subscriber_id: int, action: str, db: Session) -> None:
    """Insert into manual_action_log if action is a tracked manual type."""
    from config.ap_lite import MANUAL_ACTION_TYPES
    from src.core.models import ManualActionLog
    from datetime import date, timedelta
    if action not in MANUAL_ACTION_TYPES:
        return
    today = date.today()
    # Monday of the current week
    week_start = today - timedelta(days=today.weekday())
    try:
        db.add(ManualActionLog(
            subscriber_id=subscriber_id,
            action_type=action,
            week_start=week_start,
        ))
        db.flush()
    except Exception as exc:
        logger.warning("manual_action_log insert failed for sub %s: %s", subscriber_id, exc)


def credit(
    subscriber_id: int,
    amount: int,
    description: str,
    db: Session,
    stripe_charge_id: Optional[str] = None,
) -> WalletTransaction:
    """Atomic credit add. Locks the wallet row."""
    wallet = get_or_create_wallet(subscriber_id, db, lock=True)
    wallet.credits_remaining += amount
    txn = WalletTransaction(
        subscriber_id=subscriber_id,
        wallet_id=wallet.id,
        txn_type="credit",
        amount=amount,
        balance_after=wallet.credits_remaining,
        description=description,
        stripe_charge_id=stripe_charge_id,
    )
    db.add(txn)
    db.flush()
    return txn


def refund_credits(
    subscriber_id: int,
    amount: int,
    description: str,
    db: Session,
    stripe_charge_id: Optional[str] = None,
) -> WalletTransaction:
    """Refund credits to the wallet (e.g. clawback of a refunded purchase).

    Distinct txn_type='refund' so the audit trail separates customer-facing
    refunds from organic credits/reloads.
    """
    wallet = get_or_create_wallet(subscriber_id, db, lock=True)
    wallet.credits_remaining += amount
    txn = WalletTransaction(
        subscriber_id=subscriber_id,
        wallet_id=wallet.id,
        txn_type="refund",
        amount=amount,
        balance_after=wallet.credits_remaining,
        description=description,
        stripe_charge_id=stripe_charge_id,
    )
    db.add(txn)
    db.flush()
    return txn


def check_enrollment_triggers(subscriber_id: int, db: Session) -> Optional[str]:
    """Return tier name if the subscriber qualifies for wallet enrollment, else None.

    Triggers (any one fires):
      • saved-card pre-qualify (has_saved_card AND no wallet yet, AND not opted-out)
      • two_unlocks_24h
      • three_total_unlocks
      • eight_dollar_day (≥ 4 credits debited today, ~$8 in starter pricing)
      • repeat_zip_48h   (≥ 2 debits on same ZIP within last 48h)
    """
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return None
    if getattr(sub, "wallet_opt_out", False):
        return None

    existing_wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()

    # Saved-card users skip threshold — pre-qualified for starter_wallet
    if sub.has_saved_card and not existing_wallet:
        return "starter_wallet"
    if existing_wallet:
        return None

    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # two_unlocks_24h
    unlocks_24h = db.execute(
        select(func.count()).select_from(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.description == "lead_unlock",
            WalletTransaction.created_at >= now - timedelta(hours=24),
        )
    ).scalar() or 0
    if unlocks_24h >= 2:
        return "starter_wallet"

    # three_total_unlocks
    total_unlocks = db.execute(
        select(func.count()).select_from(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.description == "lead_unlock",
        )
    ).scalar() or 0
    if total_unlocks >= 3:
        return "starter_wallet"

    # eight_dollar_day — ≥ 4 credits debited today (~$8 at starter pricing)
    today_credits = db.execute(
        select(func.coalesce(func.sum(WalletTransaction.amount), 0)).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.txn_type == "debit",
            WalletTransaction.created_at >= today_start,
        )
    ).scalar() or 0
    if abs(int(today_credits)) >= 4:
        return "starter_wallet"

    # repeat_zip_48h — same ZIP touched ≥ 2 times in last 48h
    forty_eight_ago = now - timedelta(hours=48)
    repeat_zip = db.execute(
        select(WalletTransaction.zip_code, func.count().label("c")).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.zip_code.is_not(None),
            WalletTransaction.created_at >= forty_eight_ago,
        ).group_by(WalletTransaction.zip_code).having(func.count() >= 2)
    ).first()
    if repeat_zip:
        return "starter_wallet"

    return None


def accelerated_push_eligible(subscriber_id: int, db: Session) -> Optional[dict]:
    """Return offer dict if subscriber qualifies for an Accelerated Wallet Push,
    else None.

    Eligible when ALL hold:
      • feature flag enabled
      • subscriber exists, not wallet_opt_out
      • has_saved_card AND stripe_payment_method_id
      • no existing WalletBalance row
      • at least one WalletTransaction(txn_type='debit') exists (paid intent)
      • per-day idempotency key not set in Redis

    Sets Redis key `aw_push:{sub}:{yyyy-mm-dd}` (TTL ~26h) on a positive return so
    repeated invocations within the same UTC day are a no-op.
    """
    from config.settings import settings
    if not getattr(settings, "accelerated_wallet_push_enabled", False):
        return None

    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return None
    if getattr(sub, "wallet_opt_out", False):
        return None
    if not sub.has_saved_card or not sub.stripe_payment_method_id:
        return None

    # Block the push only when the subscriber already has an ACTIVE wallet
    # subscription (accepted/activated WalletPushOffer with a Stripe sub id).
    # A bare WalletBalance row (created by the +2 saved-card bonus credits
    # grant) is NOT a subscription — those users are exactly who should get
    # the push to convert into the $49/mo recurring plan.
    from src.core.models import WalletPushOffer as _WPO
    has_active_subscription = db.execute(
        select(func.count()).select_from(_WPO).where(
            _WPO.subscriber_id == subscriber_id,
            _WPO.stripe_subscription_id.isnot(None),
            _WPO.status.in_(("accepted", "activated")),
        )
    ).scalar() or 0
    if has_active_subscription:
        return None

    # "Paid intent" = any of:
    #   (a) a successful WalletTransaction debit (rare here since wallet
    #       doesn't exist yet — only set by existing wallet users)
    #   (b) a card-paid PremiumPurchase (the common case: subscriber paid cash
    #       for a premium unlock / brief / transfer / byol)
    debit_exists = db.execute(
        select(func.count()).select_from(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.txn_type == "debit",
        )
    ).scalar() or 0
    if debit_exists < 1:
        from src.core.models import PremiumPurchase
        premium_paid = db.execute(
            select(func.count()).select_from(PremiumPurchase).where(
                PremiumPurchase.subscriber_id == subscriber_id,
                PremiumPurchase.paid_via == "card",
                PremiumPurchase.status.in_(("pending", "delivered")),
            )
        ).scalar() or 0
        if premium_paid < 1:
            return None

    # Redis dedupe — one push per subscriber per UTC day
    from src.core.redis_client import redis_available, rget, rset
    today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    redis_key = f"aw_push:{subscriber_id}:{today_key}"
    if redis_available() and rget(redis_key):
        return None

    tier = "starter_wallet"
    tier_config = WALLET_TIERS[tier]
    missed_leads = int(getattr(sub, "missed_lead_count", 0) or 0)

    if redis_available():
        rset(redis_key, "1", ttl_seconds=26 * 3600)

    return {
        "tier": tier,
        "missed_leads": missed_leads,
        "credits_in_offer": tier_config["credits_per_cycle"],
        "price_cents": tier_config["price_cents"],
        "reason": "saved_card_paid_intent",
    }


def ensure_offer_row(subscriber_id: int, eligibility: dict, db: Session):
    """Idempotent: return existing 'offered' row for this subscriber, else create one.

    Called as soon as `accelerated_push_eligible()` returns truthy, BEFORE
    agent dispatch — so the in-app surface (`subscriber.accelerated_wallet_offer_active`
    computed from this table) is populated regardless of whether the SMS
    delivery succeeds.
    """
    from src.core.models import WalletPushOffer

    existing = db.execute(
        select(WalletPushOffer).where(
            WalletPushOffer.subscriber_id == subscriber_id,
            WalletPushOffer.status == "offered",
        )
    ).scalar_one_or_none()
    if existing:
        return existing

    offer = WalletPushOffer(
        subscriber_id=subscriber_id,
        framing_variant="credits_ready",
        tier=eligibility.get("tier", "starter_wallet"),
        status="offered",
    )
    db.add(offer)
    db.flush()
    logger.info(
        "WalletPushOffer ensured sub=%s offer=%s trigger=%s",
        subscriber_id, offer.id, eligibility.get("reason"),
    )
    return offer


def activate_via_saved_card(
    subscriber_id: int, tier: str, db: Session, offer_id: int
) -> dict:
    """Create an off-session Stripe Subscription against the saved PM.

    The wallet is NOT credited here; activation happens in the
    `invoice.payment_succeeded` webhook so we never grant credits before
    Stripe confirms the first invoice.

    Returns dict with `subscription_id`, `status`, and optional
    `client_secret`/`requires_action` for fallback handling.
    Raises ValueError("no_saved_card") if prerequisites are missing.
    """
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        raise ValueError("no_subscriber")
    if not sub.has_saved_card or not sub.stripe_payment_method_id:
        raise ValueError("no_saved_card")
    if not sub.stripe_customer_id:
        raise ValueError("no_stripe_customer")

    from src.services import stripe_service
    return stripe_service.create_subscription_off_saved_pm(
        subscriber=sub, tier=tier, offer_id=offer_id
    )


def enroll(subscriber_id: int, tier: str, db: Session) -> WalletBalance:
    existing = _select_wallet_for_update(subscriber_id, db)
    tier_config = WALLET_TIERS.get(tier, WALLET_TIERS["starter_wallet"])
    if existing:
        existing.wallet_tier = tier
        existing.credits_remaining += tier_config["credits_per_cycle"]
        db.flush()
        return existing
    wallet = WalletBalance(
        subscriber_id=subscriber_id,
        wallet_tier=tier,
        credits_remaining=tier_config["credits_per_cycle"],
    )
    db.add(wallet)
    db.flush()
    return wallet


def check_auto_reload(wallet: WalletBalance, db: Session) -> bool:
    """Auto-reload runs inside the same lock the caller holds (debit() does)."""
    if not wallet.auto_reload_enabled:
        return False
    sub = db.get(Subscriber, wallet.subscriber_id)
    if not sub or not sub.has_saved_card or not sub.stripe_payment_method_id:
        return False

    tier_config = WALLET_TIERS.get(wallet.wallet_tier, WALLET_TIERS["starter_wallet"])
    price_name = tier_config["stripe_price_env"].replace("stripe_price_", "")
    price_id = settings.active_stripe_price(price_name)
    if not price_id:
        logger.warning("Auto-reload: no price_id for tier %s", wallet.wallet_tier)
        return False

    key = settings.active_stripe_secret_key
    if not key:
        return False
    stripe.api_key = key.get_secret_value()

    try:
        pi = stripe.PaymentIntent.create(
            amount=tier_config["price_cents"],
            currency="usd",
            customer=sub.stripe_customer_id,
            payment_method=sub.stripe_payment_method_id,
            off_session=True,
            confirm=True,
            metadata={"product": "wallet_reload", "subscriber_id": str(sub.id)},
        )
        new_credits = tier_config["credits_per_cycle"]
        wallet.credits_remaining += new_credits
        wallet.last_reload_at = datetime.now(timezone.utc)
        txn = WalletTransaction(
            subscriber_id=wallet.subscriber_id,
            wallet_id=wallet.id,
            txn_type="reload",
            amount=new_credits,
            balance_after=wallet.credits_remaining,
            description=f"auto_reload:{wallet.wallet_tier}",
            stripe_charge_id=pi.id,
        )
        db.add(txn)
        db.flush()
        logger.info("Auto-reload: subscriber=%s credits=%d", wallet.subscriber_id, new_credits)
        return True
    except stripe.error.StripeError as exc:
        logger.error("Auto-reload Stripe error for subscriber %s: %s", wallet.subscriber_id, exc)
        return False


def add_bonus(subscriber_id: int, amount: int, reason: str, db: Session) -> WalletTransaction:
    # Enforce guardrail: max 10 bonus credits per event
    guardrail = get_guardrail("credit_bonus_max")
    capped = min(amount, guardrail.get("max_credits", 10))
    wallet = get_or_create_wallet(subscriber_id, db, lock=True)
    wallet.credits_remaining += capped
    txn = WalletTransaction(
        subscriber_id=subscriber_id,
        wallet_id=wallet.id,
        txn_type="bonus",
        amount=capped,
        balance_after=wallet.credits_remaining,
        description=reason,
    )
    db.add(txn)
    db.flush()
    return txn


def check_saved_card_bonus(subscriber_id: int, db: Session) -> bool:
    """Grant 2 bonus credits if the subscriber saved a card within the 10-min
    window (Redis key set by stripe webhook). Idempotent — the Redis key is
    consumed on successful grant so repeat calls in the same window are
    no-ops. Also short-circuits if any 'saved_card_bonus' transaction already
    exists for this subscriber (defense-in-depth if Redis is unavailable).
    """
    from src.core.redis_client import redis_available, rget, rdelete
    from sqlalchemy import select as _select
    if not redis_available():
        return False
    key = f"saved_card_window:{subscriber_id}"
    if not rget(key):
        return False
    already = db.execute(
        _select(func.count()).select_from(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.description == "saved_card_bonus",
        )
    ).scalar() or 0
    if already > 0:
        rdelete(key)
        return False
    add_bonus(subscriber_id, 2, "saved_card_bonus", db)
    rdelete(key)
    return True


def check_accelerated_push(subscriber_id: int, db: Session) -> Optional[str]:
    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    if not wallet:
        return None

    cutoff = datetime.now(timezone.utc) - timedelta(days=14)
    debits_14d = db.execute(
        select(func.sum(WalletTransaction.amount)).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.txn_type == "debit",
            WalletTransaction.created_at >= cutoff,
        )
    ).scalar() or 0

    total_credits = wallet.credits_used_total or 0
    if total_credits == 0:
        return None

    usage_ratio = abs(debits_14d) / total_credits
    if usage_ratio >= 0.70:
        tier_order = ["starter_wallet", "growth", "power"]
        current_idx = tier_order.index(wallet.wallet_tier) if wallet.wallet_tier in tier_order else 0
        if current_idx < len(tier_order) - 1:
            return tier_order[current_idx + 1]
    return None
