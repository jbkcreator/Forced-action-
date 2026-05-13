"""
Unit tests for milestone_evaluator.evaluate().

Uses a module-scoped SQLite fixture that only creates the tables needed by
these tests, avoiding Postgres-specific JSONB/ARRAY columns in other tables.
"""

import pytest
from datetime import datetime, timezone

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint, text
from sqlalchemy.orm import sessionmaker, Session

from src.services.milestone_evaluator import Milestone, evaluate


@pytest.fixture(scope="module")
def db() -> Session:
    """Module-scoped SQLite DB with only the tables required for milestone tests."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    # Create only the minimal tables for these tests
    meta = MetaData()

    Table("subscribers", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stripe_customer_id", String(100), unique=True, nullable=False),
        Column("tier", String(20), nullable=False),
        Column("vertical", String(50), nullable=False),
        Column("county_id", String(50), nullable=False),
        Column("email", String(255)),
        Column("founding_member", Boolean, default=False),
        Column("status", String(20), default="active"),
        Column("referral_code", String(20), unique=True),
        Column("bonus_zip_slots", Integer, default=0, server_default="0"),
        Column("created_at", DateTime),
        Column("updated_at", DateTime),
    )

    Table("referral_events", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("referrer_subscriber_id", Integer, ForeignKey("subscribers.id"), nullable=False),
        Column("referee_subscriber_id", Integer, ForeignKey("subscribers.id")),
        Column("referral_code", String(20), nullable=False),
        Column("status", String(20), nullable=False, default="pending"),
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

    meta.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.close()
    engine.dispose()


def _make_subscriber(db, suffix="a") -> int:
    result = db.execute(
        text("INSERT INTO subscribers (stripe_customer_id, tier, vertical, county_id, email) VALUES (:sc, :t, :v, :c, :e)"),
        {"sc": f"cus_test_{suffix}", "t": "starter", "v": "roofing", "c": "hillsborough", "e": f"test_{suffix}@example.com"},
    )
    db.flush()
    return result.lastrowid


def _make_event(db, referrer_id: int, referee_id: int, status: str) -> int:
    result = db.execute(
        text("INSERT INTO referral_events (referrer_subscriber_id, referee_subscriber_id, referral_code, status, reward_type, reward_value) VALUES (:rr, :re, :code, :s, :rt, :rv)"),
        {"rr": referrer_id, "re": referee_id, "code": "testcode", "s": status, "rt": "credits", "rv": "5"},
    )
    db.flush()
    return result.lastrowid


def _make_milestone_award(db, referrer_id: int, milestone: str) -> None:
    db.execute(
        text("INSERT INTO referral_milestone_awards (referrer_subscriber_id, milestone, awarded_at) VALUES (:rid, :m, :a)"),
        {"rid": referrer_id, "m": milestone, "a": datetime.now(timezone.utc)},
    )
    db.flush()


# ── count-based cases ──────────────────────────────────────────────────────────

def test_zero_referrals_no_milestones(db):
    referrer = _make_subscriber(db, "z0")
    result = evaluate(referrer, db)
    assert result == []


def test_one_confirmed_referral_no_milestones(db):
    referrer = _make_subscriber(db, "z1r")
    referee = _make_subscriber(db, "z1e")
    _make_event(db, referrer, referee, "confirmed")
    result = evaluate(referrer, db)
    assert result == []


def test_two_confirmed_no_milestones(db):
    referrer = _make_subscriber(db, "z2r")
    for i in range(2):
        ref = _make_subscriber(db, f"z2e{i}")
        _make_event(db, referrer, ref, "confirmed")
    result = evaluate(referrer, db)
    assert result == []


def test_three_confirmed_returns_free_month(db):
    referrer = _make_subscriber(db, "z3r")
    for i in range(3):
        ref = _make_subscriber(db, f"z3e{i}")
        _make_event(db, referrer, ref, "confirmed")
    result = evaluate(referrer, db)
    assert Milestone.FREE_MONTH_3 in result
    assert Milestone.LOCK_SLOT_5 not in result


def test_four_confirmed_returns_only_free_month(db):
    referrer = _make_subscriber(db, "z4r")
    for i in range(4):
        ref = _make_subscriber(db, f"z4e{i}")
        _make_event(db, referrer, ref, "confirmed")
    result = evaluate(referrer, db)
    assert Milestone.FREE_MONTH_3 in result
    assert Milestone.LOCK_SLOT_5 not in result


def test_five_confirmed_returns_both_milestones(db):
    referrer = _make_subscriber(db, "z5r")
    for i in range(5):
        ref = _make_subscriber(db, f"z5e{i}")
        _make_event(db, referrer, ref, "confirmed")
    result = evaluate(referrer, db)
    assert Milestone.FREE_MONTH_3 in result
    assert Milestone.LOCK_SLOT_5 in result


def test_six_confirmed_returns_both_milestones(db):
    referrer = _make_subscriber(db, "z6r")
    for i in range(6):
        ref = _make_subscriber(db, f"z6e{i}")
        _make_event(db, referrer, ref, "confirmed")
    result = evaluate(referrer, db)
    assert Milestone.FREE_MONTH_3 in result
    assert Milestone.LOCK_SLOT_5 in result


# ── status-filtering cases ─────────────────────────────────────────────────────

def test_rewarded_status_counts_toward_milestones(db):
    referrer = _make_subscriber(db, "zrr")
    for i in range(3):
        ref = _make_subscriber(db, f"zrre{i}")
        _make_event(db, referrer, ref, "rewarded")
    result = evaluate(referrer, db)
    assert Milestone.FREE_MONTH_3 in result


def test_pending_does_not_count(db):
    referrer = _make_subscriber(db, "zpr")
    for i in range(5):
        ref = _make_subscriber(db, f"zpe{i}")
        _make_event(db, referrer, ref, "pending")
    result = evaluate(referrer, db)
    assert result == []


def test_revoked_does_not_count(db):
    referrer = _make_subscriber(db, "zvr")
    for i in range(5):
        ref = _make_subscriber(db, f"zve{i}")
        _make_event(db, referrer, ref, "revoked")
    result = evaluate(referrer, db)
    assert result == []


def test_mixed_statuses_only_counts_confirmed_rewarded(db):
    referrer = _make_subscriber(db, "zmr")
    statuses = ["confirmed", "confirmed", "confirmed", "pending", "revoked"]
    for i, status in enumerate(statuses):
        ref = _make_subscriber(db, f"zme{i}")
        _make_event(db, referrer, ref, status)
    result = evaluate(referrer, db)
    assert Milestone.FREE_MONTH_3 in result
    assert Milestone.LOCK_SLOT_5 not in result


# ── idempotency cases ──────────────────────────────────────────────────────────

def test_already_awarded_free_month_not_returned(db):
    referrer = _make_subscriber(db, "ziar")
    for i in range(3):
        ref = _make_subscriber(db, f"ziae{i}")
        _make_event(db, referrer, ref, "confirmed")
    _make_milestone_award(db, referrer, "free_month_3")
    result = evaluate(referrer, db)
    assert Milestone.FREE_MONTH_3 not in result


def test_already_awarded_both_milestones_returns_empty(db):
    referrer = _make_subscriber(db, "zibb")
    for i in range(5):
        ref = _make_subscriber(db, f"zibbe{i}")
        _make_event(db, referrer, ref, "confirmed")
    _make_milestone_award(db, referrer, "free_month_3")
    _make_milestone_award(db, referrer, "lock_slot_5")
    result = evaluate(referrer, db)
    assert result == []


def test_only_lock_slot_awarded_still_returns_free_month(db):
    """If lock_slot already exists but free_month does not, only free_month returned."""
    referrer = _make_subscriber(db, "zilr")
    for i in range(5):
        ref = _make_subscriber(db, f"zile{i}")
        _make_event(db, referrer, ref, "confirmed")
    _make_milestone_award(db, referrer, "lock_slot_5")
    result = evaluate(referrer, db)
    assert Milestone.FREE_MONTH_3 in result
    assert Milestone.LOCK_SLOT_5 not in result
