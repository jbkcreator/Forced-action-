"""
Unit tests for milestone_grants module.
Stripe is mocked at the SDK boundary. DB assertions use the same
module-scoped SQLite pattern as test_milestone_evaluator.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint, text
from sqlalchemy.orm import sessionmaker, Session

from src.services.milestone_grants import (
    PER_REFERRAL_CREDITS,
    grant_free_month,
    grant_lock_slot,
    grant_per_referral_credits,
)


# ── fixture ────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def db() -> Session:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    meta = MetaData()

    Table("subscribers", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stripe_customer_id", String(100), unique=True, nullable=False),
        Column("stripe_subscription_id", String(100), unique=True),
        Column("tier", String(20), nullable=False),
        Column("vertical", String(50), nullable=False),
        Column("county_id", String(50), nullable=False),
        Column("email", String(255)),
        Column("bonus_zip_slots", Integer, default=0, server_default="0", nullable=False),
        Column("status", String(20), default="active"),
        Column("created_at", DateTime),
        Column("updated_at", DateTime),
    )

    Table("referral_events", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("referrer_subscriber_id", Integer, ForeignKey("subscribers.id"), nullable=False),
        Column("referee_subscriber_id", Integer, ForeignKey("subscribers.id")),
        Column("referral_code", String(20), nullable=False),
        Column("status", String(20), nullable=False, default="confirmed"),
        Column("reward_type", String(30)),
        Column("reward_value", String(50)),
        Column("created_at", DateTime),
        Column("confirmed_at", DateTime),
    )

    Table("referral_milestone_awards", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("referrer_subscriber_id", Integer, ForeignKey("subscribers.id"), nullable=False),
        Column("milestone", String(30), nullable=False),
        Column("awarded_at", DateTime, nullable=False),
        Column("triggering_referral_event_id", Integer, ForeignKey("referral_events.id")),
        Column("grant_ref", Text),
        Column("notified_at", DateTime),
        UniqueConstraint("referrer_subscriber_id", "milestone", name="uq_referral_milestone_per_referrer"),
    )

    Table("wallet_balances", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("subscriber_id", Integer, ForeignKey("subscribers.id"), nullable=False, unique=True),
        Column("credits_remaining", Integer, default=0, nullable=False),
        Column("wallet_tier", String(20), default="starter_wallet", nullable=False),
        Column("created_at", DateTime),
        Column("updated_at", DateTime),
    )

    Table("wallet_transactions", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("subscriber_id", Integer, ForeignKey("subscribers.id"), nullable=False),
        Column("wallet_id", Integer, ForeignKey("wallet_balances.id"), nullable=False),
        Column("txn_type", String(20), nullable=False),
        Column("amount", Integer, nullable=False),
        Column("balance_after", Integer, nullable=False),
        Column("description", String(200)),
        Column("stripe_charge_id", String(100)),
        Column("created_at", DateTime),
    )

    meta.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.close()
    engine.dispose()


def _make_subscriber(db, suffix: str, sub_id: str = None, cus_id: str = None) -> int:
    r = db.execute(
        text("INSERT INTO subscribers (stripe_customer_id, stripe_subscription_id, tier, vertical, county_id, email, bonus_zip_slots) VALUES (:cus, :sub, 'starter', 'roofing', 'hillsborough', :email, 0)"),
        {"cus": cus_id or f"cus_{suffix}", "sub": sub_id, "email": f"{suffix}@test.com"},
    )
    db.flush()
    return r.lastrowid


def _make_event(db, referrer_id: int) -> int:
    r = db.execute(
        text("INSERT INTO referral_events (referrer_subscriber_id, referral_code, status) VALUES (:rid, 'code', 'confirmed')"),
        {"rid": referrer_id},
    )
    db.flush()
    return r.lastrowid


# ── grant_per_referral_credits ─────────────────────────────────────────────────

@patch("src.services.milestone_grants.credit")
def test_per_referral_credits_calls_credit_with_correct_args(mock_credit, db):
    sub_id = _make_subscriber(db, "cr1")
    event_id = _make_event(db, sub_id)
    grant_per_referral_credits(sub_id, event_id, db)
    mock_credit.assert_called_once_with(
        sub_id, PER_REFERRAL_CREDITS, f"referral_reward:event:{event_id}", db,
    )


@patch("src.services.milestone_grants.credit")
def test_per_referral_credits_includes_event_id_in_description(mock_credit, db):
    sub_id = _make_subscriber(db, "cr2")
    event_id = _make_event(db, sub_id)
    grant_per_referral_credits(sub_id, event_id, db)
    description = mock_credit.call_args[0][2]
    assert f"event:{event_id}" in description


# ── grant_free_month ───────────────────────────────────────────────────────────

def _mock_settings(coupon_id="coupon_test"):
    s = MagicMock()
    s.referral_free_month_coupon_id = coupon_id
    s.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
    return s


def _mock_sub(stripe_subscription_id=None, stripe_customer_id=None):
    sub = MagicMock()
    sub.stripe_subscription_id = stripe_subscription_id
    sub.stripe_customer_id = stripe_customer_id
    return sub


@patch("src.services.milestone_grants.stripe.Subscription.modify")
def test_grant_free_month_happy_path_with_subscription(mock_modify, db):
    sub_id = _make_subscriber(db, "fm1", sub_id="sub_abc123", cus_id="cus_fm1")
    event_id = _make_event(db, sub_id)

    with patch("src.services.milestone_grants.get_settings", return_value=_mock_settings()), \
         patch.object(db, "get", return_value=_mock_sub("sub_abc123", "cus_fm1")):
        award = grant_free_month(sub_id, event_id, db)

    mock_modify.assert_called_once_with("sub_abc123", coupon="coupon_test")
    assert award.milestone == "free_month_3"
    assert "sub_abc123" in award.grant_ref
    assert "coupon_test" in award.grant_ref


@patch("src.services.milestone_grants.stripe.Customer.modify")
def test_grant_free_month_falls_back_to_customer_when_no_sub(mock_cmod, db):
    sub_id = _make_subscriber(db, "fm2", sub_id=None, cus_id="cus_fm2")
    event_id = _make_event(db, sub_id)

    with patch("src.services.milestone_grants.get_settings", return_value=_mock_settings()), \
         patch.object(db, "get", return_value=_mock_sub(None, "cus_fm2")):
        award = grant_free_month(sub_id, event_id, db)

    mock_cmod.assert_called_once_with("cus_fm2", coupon="coupon_test")
    assert award.grant_ref.startswith("cus:")


@patch("src.services.milestone_grants.stripe.Subscription.modify")
def test_grant_free_month_idempotent_on_duplicate(mock_modify, db):
    sub_id = _make_subscriber(db, "fm3", sub_id="sub_dup", cus_id="cus_fm3")
    event_id = _make_event(db, sub_id)

    with patch("src.services.milestone_grants.get_settings", return_value=_mock_settings()), \
         patch.object(db, "get", return_value=_mock_sub("sub_dup", "cus_fm3")):
        award1 = grant_free_month(sub_id, event_id, db)
        award2 = grant_free_month(sub_id, event_id, db)  # duplicate

    assert award1.id == award2.id
    assert mock_modify.call_count == 1  # Stripe called only once


# ── grant_lock_slot ────────────────────────────────────────────────────────────

def test_grant_lock_slot_increments_bonus_zip_slots(db):
    sub_id = _make_subscriber(db, "ls1")
    event_id = _make_event(db, sub_id)
    grant_lock_slot(sub_id, event_id, db)
    slots = db.execute(text("SELECT bonus_zip_slots FROM subscribers WHERE id=:s"), {"s": sub_id}).scalar()
    assert slots == 1


def test_grant_lock_slot_creates_award_row(db):
    sub_id = _make_subscriber(db, "ls2")
    event_id = _make_event(db, sub_id)
    award = grant_lock_slot(sub_id, event_id, db)
    assert award.milestone == "lock_slot_5"
    assert award.triggering_referral_event_id == event_id


def test_grant_lock_slot_idempotent_on_duplicate(db):
    sub_id = _make_subscriber(db, "ls3")
    event_id = _make_event(db, sub_id)
    award1 = grant_lock_slot(sub_id, event_id, db)
    award2 = grant_lock_slot(sub_id, event_id, db)  # duplicate
    assert award1.id == award2.id
    # Slot incremented only once
    slots = db.execute(text("SELECT bonus_zip_slots FROM subscribers WHERE id=:s"), {"s": sub_id}).scalar()
    assert slots == 1
