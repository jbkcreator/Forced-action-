"""B1 / M9 Revenue Engine — DB-backed behavior tests (real Postgres via fresh_db).

Exercises the revenue_engine public interface and the Stripe webhook wiring.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from src.core.models import CustomerAccount, MrrMovement, Subscriber
from src.services.revenue_engine import record_subscription_active

pytestmark = pytest.mark.scenario_platform

_PERIOD_END = datetime(2026, 7, 23, tzinfo=timezone.utc)


def _ensure_starter_plan(db):
    db.execute(text("""
        INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements)
        VALUES ('starter','Starter','starter',29900,'monthly', CAST(:ent AS jsonb))
        ON CONFLICT (plan_id) DO NOTHING
    """), {"ent": '{"gold": 20}'})


def _make_account(db, *, status="free_trial", mrr=0, stripe_customer="cus_s1_b3"):
    acct = CustomerAccount(status=status, mrr_cents=mrr, stripe_customer_id=stripe_customer)
    db.add(acct)
    db.flush()
    return acct


def test_checkout_paid_activates_account_and_records_new_mrr(fresh_db):
    _ensure_starter_plan(fresh_db)
    acct = _make_account(fresh_db)

    mv = record_subscription_active(
        fresh_db, acct,
        plan_id="starter",
        stripe_subscription_id="sub_s1_b3",
        current_period_end=_PERIOD_END,
        stripe_event_id="evt_s1_b3_1",
    )
    fresh_db.flush()

    assert acct.status == "active"
    assert acct.mrr_cents == 29900
    assert acct.plan_tier == "starter"
    assert acct.converted_at is not None
    assert mv is not None
    assert mv.movement_type == "new"
    assert mv.delta_cents == 29900
    assert mv.mrr_after_cents == 29900


def _ensure_pro_plan(db):
    db.execute(text("""
        INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements)
        VALUES ('pro','Pro','pro',49900,'monthly', CAST(:ent AS jsonb))
        ON CONFLICT (plan_id) DO NOTHING
    """), {"ent": '{"gold": 50}'})


def test_upgrade_records_expansion(fresh_db):
    _ensure_starter_plan(fresh_db)
    _ensure_pro_plan(fresh_db)
    acct = _make_account(fresh_db, status="active", mrr=29900, stripe_customer="cus_s1_b5")
    acct.plan_tier = "starter"
    fresh_db.flush()

    mv = record_subscription_active(
        fresh_db, acct,
        plan_id="pro",
        stripe_subscription_id="sub_s1_b5",
        current_period_end=_PERIOD_END,
        stripe_event_id="evt_s1_b5_1",
    )
    fresh_db.flush()

    assert acct.mrr_cents == 49900
    assert acct.plan_tier == "pro"
    assert mv.movement_type == "expansion"
    assert mv.delta_cents == 20000


def test_replayed_event_does_not_duplicate_movement(fresh_db):
    _ensure_starter_plan(fresh_db)
    acct = _make_account(fresh_db, stripe_customer="cus_s1_b8")

    first = record_subscription_active(
        fresh_db, acct, plan_id="starter",
        stripe_subscription_id="sub_s1_b8", current_period_end=_PERIOD_END,
        stripe_event_id="evt_s1_b8_dup",
    )
    second = record_subscription_active(
        fresh_db, acct, plan_id="starter",
        stripe_subscription_id="sub_s1_b8", current_period_end=_PERIOD_END,
        stripe_event_id="evt_s1_b8_dup",
    )
    fresh_db.flush()

    assert first is not None
    assert second is None  # replay produced no new movement
    count = fresh_db.query(MrrMovement).filter(
        MrrMovement.stripe_event_id == "evt_s1_b8_dup"
    ).count()
    assert count == 1


def _make_subscriber(db, *, stripe_customer, status="active"):
    sub = Subscriber(
        stripe_customer_id=stripe_customer,
        tier="starter", vertical="roofing", county_id="hillsborough",
        status=status,
    )
    db.add(sub)
    db.flush()
    return sub


def test_payment_failed_sets_account_past_due_but_not_subscriber_status(fresh_db):
    from src.services.stripe_webhooks import _on_payment_failed

    sub = _make_subscriber(fresh_db, stripe_customer="cus_s1_b4", status="active")
    acct = CustomerAccount(
        status="active", mrr_cents=29900,
        stripe_customer_id="cus_s1_b4", subscriber_id=sub.id,
    )
    fresh_db.add(acct)
    fresh_db.flush()

    _on_payment_failed({"customer": "cus_s1_b4"}, fresh_db)
    fresh_db.flush()

    # account goes past_due
    assert acct.status == "past_due"
    # Subscriber.status is NOT changed (never 'grace' — that would forfeit the ZIP)
    assert sub.status == "active"
    assert sub.payment_failed_at is not None


def test_payment_succeeded_recovers_past_due_account(fresh_db):
    from src.services.stripe_webhooks import _on_payment_succeeded

    sub = _make_subscriber(fresh_db, stripe_customer="cus_s1_b6", status="active")
    acct = CustomerAccount(
        status="past_due", mrr_cents=29900,
        stripe_customer_id="cus_s1_b6", subscriber_id=sub.id,
    )
    fresh_db.add(acct)
    fresh_db.flush()

    _on_payment_succeeded({"customer": "cus_s1_b6"}, fresh_db)
    fresh_db.flush()

    assert acct.status == "active"


def test_subscription_deleted_churns_account_with_involuntary_flag(fresh_db):
    from src.services.stripe_webhooks import _on_subscription_deleted

    # account was past_due (dunning) before cancel -> involuntary churn
    sub = _make_subscriber(fresh_db, stripe_customer="cus_s1_b7", status="active")
    acct = CustomerAccount(
        status="past_due", mrr_cents=29900,
        stripe_customer_id="cus_s1_b7", subscriber_id=sub.id,
    )
    fresh_db.add(acct)
    fresh_db.flush()

    _on_subscription_deleted({"customer": "cus_s1_b7", "id": "sub_s1_b7"}, fresh_db)
    fresh_db.flush()

    assert acct.status == "churned"
    assert acct.mrr_cents == 0
    mv = (
        fresh_db.query(MrrMovement)
        .filter(MrrMovement.account_id == acct.account_id,
                MrrMovement.movement_type == "churn")
        .one()
    )
    assert mv.delta_cents == -29900
    assert mv.mrr_after_cents == 0
    assert mv.is_involuntary is True
