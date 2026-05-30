"""Phase 2 — global_contractor_mrr() tests.

Uses raw SQL DDL + inserts over SQLite to avoid JSONB incompatibility.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.services.contractor_mrr import MRR_ICP_GATE_THRESHOLD, global_contractor_mrr

_SUBSCRIBERS_DDL = """
CREATE TABLE IF NOT EXISTS subscribers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid        TEXT NOT NULL UNIQUE,
    county_id   TEXT NOT NULL,
    tier        TEXT NOT NULL DEFAULT 'starter',
    status      TEXT NOT NULL DEFAULT 'active',
    plan_price  NUMERIC(10,2),
    vertical    TEXT NOT NULL DEFAULT 'roofing',
    stripe_customer_id TEXT NOT NULL DEFAULT 'cus_test',
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_NOW = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)
_uid = 0


@pytest.fixture
def mrr_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    with engine.connect() as conn:
        conn.execute(text(_SUBSCRIBERS_DDL))
        conn.commit()
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


def _sub(session, *, county="hillsborough", tier="starter", status="active",
         plan_price="99.00", uuid=None):
    global _uid
    _uid += 1
    uid = uuid or f"u{_uid}"
    session.execute(text("""
        INSERT INTO subscribers (uuid, county_id, tier, status, plan_price)
        VALUES (:uuid, :county, :tier, :status, :plan_price)
    """), {"uuid": uid, "county": county, "tier": tier,
           "status": status, "plan_price": str(plan_price)})
    session.flush()


# ── P2-1 basic sum ────────────────────────────────────────────────────────────

def test_sums_active_paying_across_counties(mrr_db):
    _sub(mrr_db, county="hillsborough", plan_price="100.00")
    _sub(mrr_db, county="pinellas", plan_price="200.00")
    assert global_contractor_mrr(mrr_db) == Decimal("300.00")


def test_single_county_sum(mrr_db):
    _sub(mrr_db, plan_price="99.00")
    assert global_contractor_mrr(mrr_db) == Decimal("99.00")


# ── P2-2 tier exclusions ──────────────────────────────────────────────────────

def test_excludes_free_tier(mrr_db):
    _sub(mrr_db, tier="free", plan_price="0.00")
    assert global_contractor_mrr(mrr_db) == Decimal("0")


def test_excludes_data_only(mrr_db):
    _sub(mrr_db, tier="data_only", plan_price="0.00")
    assert global_contractor_mrr(mrr_db) == Decimal("0")


def test_mixed_includes_only_paying(mrr_db):
    _sub(mrr_db, tier="starter", plan_price="99.00")
    _sub(mrr_db, tier="free", plan_price="0.00")
    assert global_contractor_mrr(mrr_db) == Decimal("99.00")


# ── P2-3 status exclusions ────────────────────────────────────────────────────

def test_excludes_churned(mrr_db):
    _sub(mrr_db, status="churned", plan_price="99.00")
    assert global_contractor_mrr(mrr_db) == Decimal("0")


def test_excludes_cancelled(mrr_db):
    _sub(mrr_db, status="cancelled", plan_price="99.00")
    assert global_contractor_mrr(mrr_db) == Decimal("0")


def test_excludes_grace(mrr_db):
    _sub(mrr_db, status="grace", plan_price="99.00")
    assert global_contractor_mrr(mrr_db) == Decimal("0")


# ── P2-4 zero case ────────────────────────────────────────────────────────────

def test_returns_zero_when_no_paying_subs(mrr_db):
    assert global_contractor_mrr(mrr_db) == Decimal("0")


# ── P2-5 threshold constant ───────────────────────────────────────────────────

def test_threshold_is_50k():
    assert MRR_ICP_GATE_THRESHOLD == Decimal("50000")


def test_mrr_below_threshold(mrr_db):
    _sub(mrr_db, plan_price="100.00")
    assert global_contractor_mrr(mrr_db) < MRR_ICP_GATE_THRESHOLD


def test_mrr_at_threshold(mrr_db):
    for i in range(500):
        _sub(mrr_db, plan_price="100.00", uuid=f"t{i}")
    assert global_contractor_mrr(mrr_db) >= MRR_ICP_GATE_THRESHOLD


# ── P2-6 ICP exclusion seam (documented no-op in v1) ─────────────────────────

def test_icp_exclusion_seam_is_noop_in_v1():
    """_is_icp_subscriber_filter returns None until icp_channel_key column exists."""
    from src.services.contractor_mrr import _is_icp_subscriber_filter
    assert _is_icp_subscriber_filter() is None
