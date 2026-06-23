"""B1 / M9 Revenue Engine — CRITICAL staged lifecycle tests (real Postgres).

The e2e suite (test_s1_revenue_engine_e2e.py) proves the engine functions and the
3 wired downstream handlers work *when a customer_accounts row already exists*.

This suite asks the harder question: walking the real customer lifecycle through
the REAL Stripe webhook handlers, does the engine actually get populated and
maintained the way production will drive it?

Each test names a lifecycle STAGE and drives the genuine handler entrypoint,
seeding only the DB state the *previous* real stage leaves behind. Where a stage
is not wired, the test documents the gap with an xfail so it flips to a failure
the moment the wiring lands (and stops lying about coverage in the meantime).
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import text

from src.core.models import CustomerAccount, MrrMovement, Subscriber

pytestmark = pytest.mark.scenario_platform


def _real_checkout_subscriber(db, *, stripe_customer, stripe_sub="sub_stage", status="active"):
    """The exact DB state a real checkout.session.completed leaves behind today:
    a Subscriber row and nothing else. _on_checkout_completed does NOT create a
    customer_accounts row (verified in stripe_webhooks.py)."""
    sub = Subscriber(
        stripe_customer_id=stripe_customer,
        stripe_subscription_id=stripe_sub,
        tier="starter", vertical="roofing", county_id="hillsborough",
        status=status, email=None,
    )
    db.add(sub)
    db.flush()
    return sub


def _account_for(db, stripe_customer):
    return (
        db.query(CustomerAccount)
        .filter(CustomerAccount.stripe_customer_id == stripe_customer)
        .first()
    )


def _movements_for(db, stripe_customer):
    acct = _account_for(db, stripe_customer)
    if acct is None:
        return []
    return (
        db.query(MrrMovement)
        .filter(MrrMovement.account_id == acct.account_id)
        .all()
    )


# ── STAGE 1: conversion (checkout.session.completed) ─────────────────────────

def test_stage1_checkout_creates_active_account_and_new_mrr(fresh_db):
    """A paid checkout must create an ACTIVE customer_account bridged to the
    Subscriber and a 'new' MRR movement. This is the entrypoint that seeds
    everything downstream. Drives the REAL _on_checkout_completed; external
    side-effects (GHL, welcome email, redis) are patched out."""
    from src.services import stripe_webhooks
    from src.services.stripe_webhooks import _on_checkout_completed

    fresh_db.execute(text("""
        INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements)
        VALUES ('starter','Starter','starter',29900,'monthly', CAST(:ent AS jsonb))
        ON CONFLICT (plan_id) DO NOTHING
    """), {"ent": '{"gold": 20}'})

    session = {
        "customer": "cus_stage1",
        "subscription": "sub_stage1",
        "payment_status": "paid",
        "amount_total": 29900,  # >0 so the trial/price branch skips Stripe retrieve
        "customer_details": {"email": "stage1@test.com", "name": "Stage One"},
        "metadata": {
            "tier": "starter", "vertical": "roofing", "county_id": "hillsborough",
            "zip_codes": "33601", "is_founding": "False", "founding_price_id": "",
        },
    }
    with patch.object(stripe_webhooks, "push_subscriber_to_ghl"), \
         patch("src.services.email.send_welcome_email"), \
         patch("src.core.redis_client.rdelete"):
        _on_checkout_completed(session, fresh_db)
    fresh_db.flush()

    acct = _account_for(fresh_db, "cus_stage1")
    assert acct is not None, "checkout did not create a customer_accounts row"
    assert acct.status == "active"
    assert acct.mrr_cents == 29900
    assert acct.plan_tier == "starter"
    # bridged to the Subscriber checkout created
    sub = fresh_db.query(Subscriber).filter(
        Subscriber.stripe_customer_id == "cus_stage1"
    ).one()
    assert acct.subscriber_id == sub.id
    movements = _movements_for(fresh_db, "cus_stage1")
    assert any(m.movement_type == "new" and m.delta_cents == 29900 for m in movements)


# ── STAGE 2: dunning (invoice.payment_failed) ────────────────────────────────

def test_stage2_payment_failed_no_account_is_safe_noop(fresh_db):
    """Invariant: a customer with a Subscriber but NO account (e.g. an unmapped
    tier that skipped activation) must not crash payment_failed. The legacy path
    runs; the S1 mirror safely no-ops rather than erroring."""
    from src.services.stripe_webhooks import _on_payment_failed

    sub = _real_checkout_subscriber(fresh_db, stripe_customer="cus_stage2")

    with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"):
        _on_payment_failed({"customer": "cus_stage2"}, fresh_db)
    fresh_db.flush()

    # Legacy path still works…
    assert sub.payment_failed_at is not None
    assert sub.status == "active"  # never forced to 'grace' on failure
    # …but the engine has nothing to act on.
    assert _account_for(fresh_db, "cus_stage2") is None


# ── STAGE 3: upgrade / downgrade (customer.subscription.updated) ─────────────

def test_stage3_subscription_updated_records_expansion(fresh_db):
    """An in-place plan upgrade via subscription.updated must record an
    'expansion' movement and raise the account's run-rate. The new plan is
    resolved from the event's price id via plans.stripe_price_id."""
    from src.services.stripe_webhooks import _on_subscription_updated

    fresh_db.execute(text("""
        INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements, stripe_price_id)
        VALUES ('pro','Pro','pro',49900,'monthly', CAST(:ent AS jsonb), 'price_pro')
        ON CONFLICT (plan_id) DO NOTHING
    """), {"ent": '{"gold": 50}'})
    sub = _real_checkout_subscriber(fresh_db, stripe_customer="cus_stage3")
    acct = CustomerAccount(
        status="active", mrr_cents=29900, plan_tier="starter",
        stripe_customer_id="cus_stage3", subscriber_id=sub.id,
    )
    fresh_db.add(acct)
    fresh_db.flush()

    with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"):
        _on_subscription_updated(
            {"customer": "cus_stage3", "id": "sub_stage3", "status": "active",
             "items": {"data": [{"price": {"id": "price_pro"}}]}},
            fresh_db,
        )
    fresh_db.flush()
    fresh_db.refresh(acct)

    assert acct.mrr_cents == 49900
    movements = _movements_for(fresh_db, "cus_stage3")
    assert any(m.movement_type == "expansion" for m in movements)


# ── STAGE 4: cancellation (customer.subscription.deleted) ────────────────────

def test_stage4_churn_no_account_is_safe_noop(fresh_db):
    """Invariant: cancelling a customer with no account must not crash. The
    legacy grace path runs; no churn movement is written because there is no
    tracked revenue to remove."""
    from src.services.stripe_webhooks import _on_subscription_deleted

    sub = _real_checkout_subscriber(fresh_db, stripe_customer="cus_stage4")

    with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"):
        _on_subscription_deleted({"customer": "cus_stage4", "id": "sub_stage4"}, fresh_db)
    fresh_db.flush()

    # Legacy cancellation path fired…
    assert sub.status == "grace"
    # …but no churn was recorded in the ledger.
    assert _account_for(fresh_db, "cus_stage4") is None
    churn_count = fresh_db.execute(
        text("SELECT count(*) FROM mrr_movements WHERE stripe_event_id = :eid"),
        {"eid": "subdel:sub_stage4"},
    ).scalar()
    assert churn_count == 0


# ── FULL LIFECYCLE: one customer through every real handler ──────────────────

def test_full_lifecycle_chain_reconciles_to_zero_mrr(fresh_db):
    """The critical end-to-end proof: one customer driven through every real
    Stripe webhook handler in sequence — checkout → payment_failed → recovery →
    upgrade → cancel — produces a coherent MRR ledger that nets back to zero.

        new(+29900) → past_due → recovery → expansion(+20000) → churn(-49900) = 0
    """
    from src.services import stripe_webhooks
    from src.services.stripe_webhooks import (
        _on_checkout_completed, _on_payment_failed, _on_payment_succeeded,
        _on_subscription_updated, _on_subscription_deleted,
    )

    fresh_db.execute(text("""
        INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements, stripe_price_id)
        VALUES ('starter','Starter','starter',29900,'monthly', CAST(:s AS jsonb), 'price_starter'),
               ('pro','Pro','pro',49900,'monthly', CAST(:p AS jsonb), 'price_pro')
        ON CONFLICT (plan_id) DO NOTHING
    """), {"s": '{"gold": 20}', "p": '{"gold": 50}'})

    cust = "cus_life"
    with patch.object(stripe_webhooks, "push_subscriber_to_ghl"), \
         patch("src.services.email.send_welcome_email"), \
         patch("src.core.redis_client.rdelete"):

        # 1. conversion
        _on_checkout_completed({
            "customer": cust, "subscription": "sub_life", "payment_status": "paid",
            "amount_total": 29900,
            "customer_details": {"email": "life@test.com", "name": "Life Cycle"},
            "metadata": {"tier": "starter", "vertical": "roofing",
                         "county_id": "hillsborough", "zip_codes": "33601",
                         "is_founding": "False", "founding_price_id": ""},
        }, fresh_db)
        fresh_db.flush()
        acct = _account_for(fresh_db, cust)
        assert acct is not None and acct.status == "active" and acct.mrr_cents == 29900

        # 2. dunning
        _on_payment_failed({"customer": cust}, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(acct)
        assert acct.status == "past_due"

        # 3. recovery
        _on_payment_succeeded({"customer": cust}, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(acct)
        assert acct.status == "active"

        # 4. upgrade
        _on_subscription_updated({
            "customer": cust, "id": "sub_life", "status": "active",
            "items": {"data": [{"price": {"id": "price_pro"}}]},
        }, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(acct)
        assert acct.mrr_cents == 49900 and acct.plan_tier == "pro"

        # 5. cancel (voluntary — account was active, not past_due)
        _on_subscription_deleted({"customer": cust, "id": "sub_life"}, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(acct)
        assert acct.status == "churned" and acct.mrr_cents == 0

    movements = _movements_for(fresh_db, cust)
    types = sorted(m.movement_type for m in movements)
    assert types == ["churn", "expansion", "new"]
    assert sum(m.delta_cents for m in movements) == 0  # ledger reconciles
    churn = next(m for m in movements if m.movement_type == "churn")
    assert churn.is_involuntary is False
