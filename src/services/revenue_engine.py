"""B1 / M9 — Revenue Engine.

Maintains the S1 paying-customer state (customer_accounts) and the MRR ledger
(mrr_movements) off the back of Stripe webhook events. Pure revenue-recognition
rules (§12.7) live here as standalone functions so they are trivially testable;
the DB-mutating helpers are called by the existing Stripe webhook handlers.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def normalize_mrr_cents(price_cents: int, interval: str) -> int:
    """Normalize a plan price to a monthly run-rate (§12.7).

    monthly -> unchanged; annual -> /12; one_time / trial -> 0 (excluded
    from MRR — one-offs are non-recurring revenue reported separately).
    """
    if interval == "monthly":
        return price_cents
    if interval == "annual":
        return price_cents // 12
    return 0


def classify_movement(prior_mrr_cents: int, new_mrr_cents: int) -> Optional[str]:
    """Classify an MRR change for the movement ledger (§12.7).

    Returns 'new' | 'expansion' | 'contraction' | 'churn', or None when the
    run-rate did not change (no movement row should be written).
    """
    if new_mrr_cents == prior_mrr_cents:
        return None
    if new_mrr_cents == 0:
        return "churn"
    if prior_mrr_cents == 0:
        return "new"
    return "expansion" if new_mrr_cents > prior_mrr_cents else "contraction"


def _record_movement(
    db: Session,
    account,
    *,
    movement_type: str,
    delta_cents: int,
    mrr_after_cents: int,
    stripe_event_id: Optional[str],
    is_involuntary: bool = False,
    effective_at: Optional[datetime] = None,
):
    """Append one MrrMovement row, idempotent on stripe_event_id.

    Returns the new MrrMovement, or None if an event with this id already
    produced a movement (replay).
    """
    from src.core.models import MrrMovement

    if stripe_event_id:
        existing = db.execute(
            text("SELECT 1 FROM mrr_movements WHERE stripe_event_id = :eid LIMIT 1"),
            {"eid": stripe_event_id},
        ).fetchone()
        if existing:
            logger.info("mrr movement already recorded for event %s — skipping", stripe_event_id)
            return None

    mv = MrrMovement(
        account_id=account.account_id,
        movement_type=movement_type,
        delta_cents=delta_cents,
        mrr_after_cents=mrr_after_cents,
        is_involuntary=is_involuntary,
        stripe_event_id=stripe_event_id,
    )
    if effective_at is not None:
        mv.effective_at = effective_at
    db.add(mv)
    db.flush()
    return mv


def _get_plan(db: Session, plan_id: str):
    row = db.execute(
        text("SELECT plan_id, price_cents, interval, entitlements FROM plans WHERE plan_id = :pid"),
        {"pid": plan_id},
    ).fetchone()
    return row


def account_by_stripe_customer(db: Session, stripe_customer_id: str):
    """Return the mutable CustomerAccount for a Stripe customer id, or None.

    Loaded via the ORM (not text()) because callers mutate the returned instance.
    """
    from src.core.models import CustomerAccount

    return (
        db.query(CustomerAccount)
        .filter(CustomerAccount.stripe_customer_id == stripe_customer_id)
        .first()
    )


def plan_id_for_tier(db: Session, tier: Optional[str]) -> Optional[str]:
    """Resolve a Subscriber.tier to a plans.plan_id, or None if no plan exists.

    Tries plan_id == tier first (the canonical 1:1 mapping — 'starter' tier ->
    'starter' plan), then falls back to the plans.tier column. Returns None for
    unmapped tiers (e.g. legacy/founding tiers not yet in the catalog) so the
    caller can skip account activation rather than break checkout.
    """
    if not tier:
        return None
    row = db.execute(
        text("SELECT plan_id FROM plans WHERE plan_id = :t OR tier = :t ORDER BY (plan_id = :t) DESC LIMIT 1"),
        {"t": tier},
    ).fetchone()
    return row.plan_id if row else None


def plan_id_for_price(db: Session, stripe_price_id: Optional[str]) -> Optional[str]:
    """Resolve a Stripe price id to a plans.plan_id, or None if unmapped."""
    if not stripe_price_id:
        return None
    row = db.execute(
        text("SELECT plan_id FROM plans WHERE stripe_price_id = :p LIMIT 1"),
        {"p": stripe_price_id},
    ).fetchone()
    return row.plan_id if row else None


def get_or_create_account(
    db: Session,
    *,
    stripe_customer_id: str,
    subscriber_id: Optional[int] = None,
):
    """Return the CustomerAccount for a Stripe customer, creating a bridged
    free_trial shell if none exists. Backfills subscriber_id on an existing
    unbridged account so there is one reconciled billing truth per customer.
    """
    from src.core.models import CustomerAccount

    account = account_by_stripe_customer(db, stripe_customer_id)
    if account is None:
        account = CustomerAccount(
            stripe_customer_id=stripe_customer_id,
            subscriber_id=subscriber_id,
        )
        db.add(account)
        db.flush()
    elif subscriber_id is not None and account.subscriber_id is None:
        account.subscriber_id = subscriber_id
        db.flush()
    return account


def record_past_due(db: Session, account) -> None:
    """Mark an account past_due (failed invoice). MRR is unchanged — a
    retriable failure is not churn, so no movement is written here."""
    account.status = "past_due"
    db.flush()


def record_churn(
    db: Session,
    account,
    *,
    stripe_event_id: Optional[str],
    effective_at: Optional[datetime] = None,
):
    """Churn an account: status -> churned, mrr -> 0, and write a churn movement.

    is_involuntary is True when the account was past_due at cancel time
    (payment-failure churn) vs a voluntary cancel. Returns the movement, or
    None if the account had no recurring revenue to remove.
    """
    prior_mrr = account.mrr_cents or 0
    involuntary = account.status == "past_due"

    account.status = "churned"
    account.mrr_cents = 0
    db.flush()

    if prior_mrr <= 0:
        return None
    return _record_movement(
        db, account,
        movement_type="churn",
        delta_cents=-prior_mrr,
        mrr_after_cents=0,
        stripe_event_id=stripe_event_id,
        is_involuntary=involuntary,
        effective_at=effective_at,
    )


def record_recovery(db: Session, account) -> None:
    """A successful invoice cleared a past_due account — restore access.
    MRR is unchanged (the run-rate never dropped), so no movement is written."""
    if account.status == "past_due":
        account.status = "active"
        db.flush()


def record_subscription_active(
    db: Session,
    account,
    *,
    plan_id: str,
    stripe_subscription_id: Optional[str],
    current_period_end: Optional[datetime],
    stripe_event_id: Optional[str],
    now: Optional[datetime] = None,
):
    """Move an account onto an active paid plan and record the MRR movement.

    Recomputes normalized mrr_cents from the plan, classifies the movement
    (new/expansion/contraction) against the prior run-rate, stamps converted_at
    on first paid transition, and updates the account's recurring-revenue fields.
    Returns the MrrMovement written, or None if MRR did not change / replay.
    """
    now = now or datetime.now(timezone.utc)
    plan = _get_plan(db, plan_id)
    if plan is None:
        raise ValueError(f"unknown plan_id: {plan_id!r}")

    prior_mrr = account.mrr_cents or 0
    new_mrr = normalize_mrr_cents(plan.price_cents, plan.interval)
    movement_type = classify_movement(prior_mrr, new_mrr)

    if account.converted_at is None and new_mrr > 0:
        account.converted_at = now

    account.status = "active"
    account.plan_tier = plan.plan_id
    account.lead_entitlement = plan.entitlements or {}
    account.stripe_subscription_id = stripe_subscription_id
    account.current_period_end = current_period_end
    account.mrr_cents = new_mrr
    db.flush()

    if movement_type is None:
        return None
    return _record_movement(
        db, account,
        movement_type=movement_type,
        delta_cents=new_mrr - prior_mrr,
        mrr_after_cents=new_mrr,
        stripe_event_id=stripe_event_id,
        effective_at=now,
    )
