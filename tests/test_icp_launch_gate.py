"""Phase 3 — icp_launch_blocked() predicate tests.

Uses SQLite for the ICP channel + subscriber tables, and patches
_build_gate_snapshot + redis for the 7-gate check.
"""
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.core.models import ExpansionIcpChannel
from src.services.icp_launch_gate import icp_launch_blocked

_SUBSCRIBERS_DDL = """
CREATE TABLE IF NOT EXISTS subscribers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid        TEXT NOT NULL UNIQUE,
    county_id   TEXT NOT NULL DEFAULT 'hillsborough',
    tier        TEXT NOT NULL DEFAULT 'starter',
    status      TEXT NOT NULL DEFAULT 'active',
    plan_price  NUMERIC(10,2),
    vertical    TEXT NOT NULL DEFAULT 'roofing',
    stripe_customer_id TEXT NOT NULL DEFAULT 'cus_test',
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


@pytest.fixture
def gate_db():
    """SQLite with expansion_icp_channels + subscribers."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    ExpansionIcpChannel.__table__.create(engine)
    with engine.connect() as conn:
        conn.execute(text(_SUBSCRIBERS_DDL))
        conn.commit()
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


def _rei_channel(session, status="gated"):
    ch = ExpansionIcpChannel(
        key="rei_investor", display_name="REI Investor",
        price_monthly=Decimal("197.00"),
        feed_scope="single_county", landing_slug="rei-investor",
        status=status,
    )
    session.add(ch)
    session.flush()
    return ch


def _add_subs(session, count, plan_price="100.00"):
    for i in range(count):
        session.execute(text("""
            INSERT INTO subscribers (uuid, county_id, tier, status, plan_price)
            VALUES (:uuid, 'hillsborough', 'starter', 'active', :price)
        """), {"uuid": f"s{i}", "price": plan_price})
    session.flush()


_ALL_GREEN_SNAPSHOT = {
    "first_payment_rate":   {"value": 35.0, "threshold": 30, "color": "green"},
    "saved_card_rate":      {"value": 75.0, "threshold": 70, "color": "green"},
    "wallet_adoption":      {"value": 20.0, "threshold": 15, "color": "green"},
    "lock_conversion":      {"value": 7.0,  "threshold": 5,  "color": "green"},
    "payer_retention_30d":  {"value": 75.0, "threshold": 70, "color": "green"},
    "free_tier_cost_ratio": {"value": 35.0, "threshold": 40, "color": "green"},
    "county_profitability": {"value": 1.0,  "threshold": None, "color": "green"},
}


# ── P3-1 blocked when a gate is red ──────────────────────────────────────────

def test_blocked_when_any_gate_red(gate_db):
    _rei_channel(gate_db)
    _add_subs(gate_db, 600)  # $60K MRR — clears money gate

    red_snapshot = {**_ALL_GREEN_SNAPSHOT,
                    "first_payment_rate": {"value": 10.0, "threshold": 30, "color": "red"}}
    with patch("src.services.icp_launch_gate._build_gate_snapshot", return_value=red_snapshot):
        reasons = icp_launch_blocked(gate_db, "rei_investor")

    assert any("first_payment_rate" in r for r in reasons)
    assert any("red" in r for r in reasons)


# ── P3-2 blocked when MRR below threshold ────────────────────────────────────

def test_blocked_when_mrr_below_threshold(gate_db):
    _rei_channel(gate_db)
    _add_subs(gate_db, 10)  # $1K MRR — well below $50K

    with patch("src.services.icp_launch_gate._build_gate_snapshot",
               return_value=_ALL_GREEN_SNAPSHOT):
        reasons = icp_launch_blocked(gate_db, "rei_investor")

    assert len(reasons) == 1
    assert "contractor_mrr" in reasons[0]
    assert "50000" in reasons[0]


# ── P3-3 blocked by both gate and MRR ────────────────────────────────────────

def test_blocked_by_both_gate_and_mrr(gate_db):
    _rei_channel(gate_db)
    _add_subs(gate_db, 10)  # low MRR

    red_snapshot = {**_ALL_GREEN_SNAPSHOT,
                    "lock_conversion": {"value": 1.0, "threshold": 5, "color": "red"}}
    with patch("src.services.icp_launch_gate._build_gate_snapshot", return_value=red_snapshot):
        reasons = icp_launch_blocked(gate_db, "rei_investor")

    assert any("lock_conversion" in r for r in reasons)
    assert any("contractor_mrr" in r for r in reasons)
    assert len(reasons) == 2


# ── P3-4 permitted when all green + MRR met ──────────────────────────────────

def test_permitted_when_all_green_and_mrr_met(gate_db):
    _rei_channel(gate_db)
    _add_subs(gate_db, 600)  # $60K MRR

    with patch("src.services.icp_launch_gate._build_gate_snapshot",
               return_value=_ALL_GREEN_SNAPSHOT):
        reasons = icp_launch_blocked(gate_db, "rei_investor")

    assert reasons == [], f"Expected no blocking reasons, got: {reasons}"


# ── P3-5 REI today is blocked on MRR (real current state) ───────────────────

def test_rei_today_is_blocked_on_mrr(gate_db):
    """With all gates green but $0 MRR, sole reason is contractor_mrr."""
    _rei_channel(gate_db)
    # No subs → MRR = $0

    with patch("src.services.icp_launch_gate._build_gate_snapshot",
               return_value=_ALL_GREEN_SNAPSHOT):
        reasons = icp_launch_blocked(gate_db, "rei_investor")

    assert len(reasons) == 1
    assert "contractor_mrr" in reasons[0]


# ── P3-6 unknown channel key ─────────────────────────────────────────────────

def test_unknown_channel_key_blocked(gate_db):
    reasons = icp_launch_blocked(gate_db, "nonexistent_channel")
    assert len(reasons) == 1
    assert "not found" in reasons[0]


# ── P3-7 None gate value → red → blocked ─────────────────────────────────────

def test_none_gate_value_blocks(gate_db):
    _rei_channel(gate_db)
    _add_subs(gate_db, 600)

    none_snapshot = {**_ALL_GREEN_SNAPSHOT,
                     "free_tier_cost_ratio": {"value": None, "threshold": 40, "color": "red"}}
    with patch("src.services.icp_launch_gate._build_gate_snapshot",
               return_value=none_snapshot):
        reasons = icp_launch_blocked(gate_db, "rei_investor")

    assert any("free_tier_cost_ratio" in r for r in reasons)


# ── P3-8 wrong channel status blocks early ───────────────────────────────────

def test_wrong_channel_status_blocks(gate_db):
    _rei_channel(gate_db, status="live")  # already live
    reasons = icp_launch_blocked(gate_db, "rei_investor")
    assert any("status" in r for r in reasons)


def test_retired_channel_blocks(gate_db):
    _rei_channel(gate_db, status="retired")
    reasons = icp_launch_blocked(gate_db, "rei_investor")
    assert any("status" in r for r in reasons)


# ── P3-9 county launch path unaffected by MRR ────────────────────────────────

def test_county_launch_does_not_import_mrr_gate():
    """Regression: county_launch_evaluator must not import icp_launch_gate or MRR check."""
    import inspect
    from src.tasks import county_launch_evaluator as mod
    src = inspect.getsource(mod)
    assert "contractor_mrr" not in src, (
        "county_launch_evaluator must not reference contractor_mrr — "
        "MRR gate is ICP-only (grilling Q4)"
    )
    assert "icp_launch_gate" not in src
