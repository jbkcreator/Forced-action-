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


def debit(subscriber_id: int, action: str, db: Session, description: str = "") -> bool:
    """Atomic debit. Returns False if the wallet has insufficient credits.

    Holds a row-level lock for the duration so concurrent debits cannot both
    pass the balance check on the same wallet.
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
    )
    db.add(txn)
    db.flush()
    if wallet.credits_remaining < WALLET_AUTO_RELOAD_THRESHOLD:
        check_auto_reload(wallet, db)
    return True


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
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return None

    # Saved-card users skip threshold — pre-qualified for starter_wallet
    if sub.has_saved_card:
        existing = db.execute(
            select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
        ).scalar_one_or_none()
        if not existing:
            return "starter_wallet"
        return None

    # Check purchase-based triggers via WalletTransaction debits
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # two_unlocks_24h: 2+ lead_unlock debits in last 24h
    unlocks_24h = db.execute(
        select(func.count()).select_from(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.description == "lead_unlock",
            WalletTransaction.created_at >= now - timedelta(hours=24),
        )
    ).scalar() or 0
    if unlocks_24h >= 2:
        return "starter_wallet"

    # three_total_unlocks: 3+ total lead_unlock debits
    total_unlocks = db.execute(
        select(func.count()).select_from(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.description == "lead_unlock",
        )
    ).scalar() or 0
    if total_unlocks >= 3:
        return "starter_wallet"

    return None


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
    from src.core.redis_client import redis_available, rget
    if not redis_available():
        return False
    key = f"saved_card_window:{subscriber_id}"
    if rget(key):
        add_bonus(subscriber_id, 2, "saved_card_bonus", db)
        return True
    return False


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
