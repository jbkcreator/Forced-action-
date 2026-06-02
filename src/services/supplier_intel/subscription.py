"""
Supplier Intelligence Foundation — subscription lifecycle (fa067).

Mirrors src/services/bankruptcy_alert/subscription.py:
  - Stripe checkout creation with metadata.product=supplier_intel
  - resolve_handler() called by stripe_webhooks.handle_webhook BEFORE property handlers
  - Status lifecycle: trialing → active → past_due → canceled
  - access_token UUID auth for supplier-facing routes

Phase 1: admin-provisioned accounts only (no self-signup flow).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import stripe
from fastapi import Depends, HTTPException
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.settings import get_settings
from config.supplier_intel_config import SUPPLIER_INTEL_TIERS, TRIAL_DAYS

logger = logging.getLogger(__name__)


def _init_stripe() -> bool:
    settings = get_settings()
    key = settings.active_stripe_secret_key
    if not key:
        return False
    stripe.api_key = key.get_secret_value()
    return True


def _attr(obj, key, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        v = obj.get(key, default)
    else:
        v = getattr(obj, key, default)
    return v if v is not None else default


# ── Checkout ──────────────────────────────────────────────────────────────────

def create_checkout(
    account_id: int,
    plan_tier: str,
    *,
    success_url: str,
    cancel_url: str,
    customer_email: Optional[str] = None,
    with_trial: bool = True,
) -> dict:
    """Create a Stripe checkout session for a supplier account."""
    if not _init_stripe():
        raise RuntimeError("Stripe not configured")
    if plan_tier not in SUPPLIER_INTEL_TIERS:
        raise ValueError(f"Unknown plan_tier: {plan_tier}")

    settings = get_settings()
    price_key = f"supplier_intel_{plan_tier}"
    price_id = settings.active_stripe_price(price_key)
    if not price_id:
        raise ValueError(f"STRIPE_PRICE_{price_key.upper()} not configured")

    subscription_data: dict = {"metadata": {"product": "supplier_intel"}}
    if with_trial and TRIAL_DAYS > 0:
        subscription_data["trial_period_days"] = TRIAL_DAYS

    session = stripe.checkout.Session.create(
        mode="subscription",
        payment_method_types=["card"],
        customer_email=customer_email,
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "product": "supplier_intel",
            "account_id": str(account_id),
            "plan_tier": plan_tier,
        },
        subscription_data=subscription_data,
    )
    logger.info("[supplier-sub] checkout created account=%s tier=%s", account_id, plan_tier)
    return {"session_id": session.id, "url": session.url}


# ── Row helpers ───────────────────────────────────────────────────────────────

def _find_sub_by_stripe_id(db: Session, stripe_sub_id: str):
    return db.execute(sa_text("""
        SELECT * FROM supplier_subscriptions WHERE stripe_subscription_id = :sid LIMIT 1
    """), {"sid": stripe_sub_id}).first()


def _find_sub_by_account(db: Session, account_id: int):
    return db.execute(sa_text("""
        SELECT * FROM supplier_subscriptions WHERE account_id = :aid LIMIT 1
    """), {"aid": account_id}).first()


def _set_status(db: Session, sub_id: int, status: str, *, canceled: bool = False) -> None:
    extra = ", canceled_at = NOW()" if canceled else ""
    db.execute(sa_text(f"""
        UPDATE supplier_subscriptions
        SET status = :status, updated_at = NOW() {extra}
        WHERE id = :id
    """), {"status": status, "id": sub_id})


# ── Webhook handlers ──────────────────────────────────────────────────────────

def _on_checkout_completed(session: dict, db: Session) -> None:
    meta = _attr(session, "metadata", {}) or {}
    account_id = meta.get("account_id")
    plan_tier = meta.get("plan_tier", "foundation")
    if not account_id:
        return

    customer_id = _attr(session, "customer")
    stripe_sub_id = _attr(session, "subscription")
    status = "trialing" if TRIAL_DAYS > 0 else "active"

    # Get trial_ends_at from Stripe subscription if possible
    trial_ends_at = None
    if stripe_sub_id and TRIAL_DAYS > 0:
        try:
            sub_obj = stripe.Subscription.retrieve(stripe_sub_id)
            te = getattr(sub_obj, "trial_end", None)
            if te:
                trial_ends_at = datetime.fromtimestamp(te, tz=timezone.utc)
        except Exception:
            pass

    # Link Stripe customer to account
    if customer_id:
        db.execute(sa_text("""
            UPDATE supplier_accounts SET stripe_customer_id = :cid, updated_at = NOW()
            WHERE id = :aid AND (stripe_customer_id IS NULL OR stripe_customer_id = :cid)
        """), {"cid": customer_id, "aid": account_id})

    # Upsert subscription row
    existing = _find_sub_by_account(db, int(account_id))
    if existing:
        db.execute(sa_text("""
            UPDATE supplier_subscriptions
            SET stripe_subscription_id = :sid, status = :status,
                trial_ends_at = :trial_ends_at, canceled_at = NULL, updated_at = NOW()
            WHERE id = :id
        """), {
            "sid": stripe_sub_id, "status": status,
            "trial_ends_at": trial_ends_at, "id": existing.id,
        })
    else:
        tier_cfg = SUPPLIER_INTEL_TIERS.get(plan_tier, {})
        db.execute(sa_text("""
            INSERT INTO supplier_subscriptions
                (account_id, plan_tier, status, stripe_subscription_id,
                 price_cents, trial_ends_at, created_at, updated_at)
            VALUES
                (:aid, :tier, :status, :sid,
                 :price_cents, :trial_ends_at, NOW(), NOW())
            ON CONFLICT (stripe_subscription_id) DO NOTHING
        """), {
            "aid": account_id,
            "tier": plan_tier,
            "status": status,
            "sid": stripe_sub_id,
            "price_cents": tier_cfg.get("price_cents"),
            "trial_ends_at": trial_ends_at,
        })
    logger.info("[supplier-sub] checkout completed account=%s tier=%s status=%s", account_id, plan_tier, status)


def _on_subscription_updated(sub: dict, db: Session) -> None:
    stripe_sub_id = _attr(sub, "id")
    stripe_status = _attr(sub, "status", "")
    mapping = {
        "trialing": "trialing", "active": "active",
        "past_due": "past_due", "unpaid": "past_due",
        "canceled": "canceled", "incomplete_expired": "canceled",
    }
    our_status = mapping.get(stripe_status)
    if not our_status:
        return
    row = _find_sub_by_stripe_id(db, stripe_sub_id)
    if row:
        _set_status(db, row.id, our_status, canceled=(our_status == "canceled"))


def _on_subscription_deleted(sub: dict, db: Session) -> None:
    stripe_sub_id = _attr(sub, "id")
    row = _find_sub_by_stripe_id(db, stripe_sub_id)
    if row:
        _set_status(db, row.id, "canceled", canceled=True)


def _on_payment_failed(invoice: dict, db: Session) -> None:
    stripe_sub_id = _attr(invoice, "subscription")
    if stripe_sub_id:
        row = _find_sub_by_stripe_id(db, stripe_sub_id)
        if row:
            _set_status(db, row.id, "past_due")


def _on_payment_succeeded(invoice: dict, db: Session) -> None:
    stripe_sub_id = _attr(invoice, "subscription")
    if stripe_sub_id:
        db.execute(sa_text("""
            UPDATE supplier_subscriptions
            SET status = 'active', updated_at = NOW()
            WHERE stripe_subscription_id = :sid AND status IN ('past_due','trialing')
        """), {"sid": stripe_sub_id})


_HANDLERS = {
    "checkout.session.completed": _on_checkout_completed,
    "customer.subscription.updated": _on_subscription_updated,
    "customer.subscription.deleted": _on_subscription_deleted,
    "invoice.payment_failed": _on_payment_failed,
    "invoice.payment_succeeded": _on_payment_succeeded,
}


def _owns_subscription(db: Session, stripe_sub_id: Optional[str]) -> bool:
    if not stripe_sub_id:
        return False
    row = db.execute(sa_text("""
        SELECT 1 FROM supplier_subscriptions WHERE stripe_subscription_id = :sid LIMIT 1
    """), {"sid": stripe_sub_id}).first()
    return row is not None


def resolve_handler(event_type: str, data: dict, db: Session):
    """Return the supplier handler for this event, or None if not ours.

    Called by stripe_webhooks.handle_webhook before property handlers.
    """
    handler = _HANDLERS.get(event_type)
    if handler is None:
        return None

    if event_type in ("checkout.session.completed",
                      "customer.subscription.updated",
                      "customer.subscription.deleted"):
        meta = _attr(data, "metadata", {}) or {}
        return handler if _attr(meta, "product") == "supplier_intel" else None

    if event_type in ("invoice.payment_failed", "invoice.payment_succeeded"):
        return handler if _owns_subscription(db, _attr(data, "subscription")) else None

    return None


# ── FastAPI access_token auth ─────────────────────────────────────────────────

def _get_db():
    from src.core.database import get_db_context
    with get_db_context() as db:
        yield db


def get_current_supplier(
    access_token: str,
    db: Session = Depends(_get_db),
):
    """Dependency: resolve supplier account by access_token. 401 if invalid."""
    row = db.execute(sa_text("""
        SELECT sa.*, ss.plan_tier, ss.status AS sub_status,
               ss.trial_ends_at, ss.canceled_at
        FROM supplier_accounts sa
        LEFT JOIN supplier_subscriptions ss ON ss.account_id = sa.id
        WHERE sa.access_token = :tok AND sa.status = 'active'
        ORDER BY ss.created_at DESC LIMIT 1
    """), {"tok": access_token}).first()

    if row is None:
        raise HTTPException(status_code=401, detail="Invalid or expired access token")
    return row
