"""Phase 1 — cost gate computation tests.

Tests _compute_revenue_30d, _compute_attributable_cost, and the resulting
free_tier_cost_ratio / county_profitability gate values.

Uses SQLite in-memory DB with only the tables needed (subscribers,
api_usage_logs).  No Postgres required for the unit tests.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.tasks.kill_switch_metric_ingest import (
    _compute_attributable_cost,
    _compute_revenue_30d,
)
from src.tasks.county_launch_evaluator import _gate_color


# ── fixture ───────────────────────────────────────────────────────────────────

_SUBSCRIBERS_DDL = """
CREATE TABLE IF NOT EXISTS subscribers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid                TEXT NOT NULL UNIQUE,
    stripe_customer_id  TEXT NOT NULL DEFAULT 'cus_test',
    stripe_subscription_id TEXT,
    county_id           TEXT NOT NULL,
    tier                TEXT NOT NULL DEFAULT 'starter',
    status              TEXT NOT NULL DEFAULT 'active',
    vertical            TEXT NOT NULL DEFAULT 'roofing',
    plan_price          NUMERIC(10,2),
    has_saved_card      INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_API_USAGE_DDL = """
CREATE TABLE IF NOT EXISTS api_usage_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    service         TEXT NOT NULL DEFAULT 'claude',
    model           TEXT,
    input_tokens    INTEGER,
    output_tokens   INTEGER,
    cost_usd        NUMERIC(10,6),
    task_type       TEXT,
    graph_name      TEXT,
    pause_target    TEXT,
    blocked_by_pause INTEGER NOT NULL DEFAULT 0,
    block_reason    TEXT,
    subscriber_id   INTEGER REFERENCES subscribers(id),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


@pytest.fixture
def cost_db():
    """SQLite with minimal subscribers + api_usage_logs tables."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    with engine.connect() as conn:
        conn.execute(text(_SUBSCRIBERS_DDL))
        conn.execute(text(_API_USAGE_DDL))
        conn.commit()
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


_NOW = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)
_AGO_30 = _NOW - timedelta(days=30)
_COUNTY = "hillsborough"
_OTHER = "pinellas"

_uuid_counter = 0


def _sub(session, *, county=_COUNTY, tier="starter", status="active",
         plan_price="99.00", uuid=None):
    global _uuid_counter
    _uuid_counter += 1
    uid = uuid or f"u{_uuid_counter}"
    session.execute(text("""
        INSERT INTO subscribers (uuid, county_id, tier, status, plan_price, created_at)
        VALUES (:uuid, :county, :tier, :status, :plan_price, :created_at)
    """), {
        "uuid": uid, "county": county, "tier": tier, "status": status,
        "plan_price": str(plan_price),
        "created_at": (_NOW - timedelta(days=10)).isoformat(),
    })
    row = session.execute(text("SELECT id FROM subscribers WHERE uuid = :u"), {"u": uid}).first()
    session.flush()

    class _Sub:
        pass
    s = _Sub()
    s.id = row.id
    return s


def _log(session, *, subscriber_id, cost_usd, service="claude", created_at=None):
    ts = (created_at or (_NOW - timedelta(days=5))).isoformat()
    session.execute(text("""
        INSERT INTO api_usage_logs (service, cost_usd, subscriber_id, created_at)
        VALUES (:service, :cost_usd, :subscriber_id, :created_at)
    """), {
        "service": service, "cost_usd": str(cost_usd),
        "subscriber_id": subscriber_id, "created_at": ts,
    })
    session.flush()


# ── P1-1 revenue helper ───────────────────────────────────────────────────────

def test_revenue_sums_active_paying(cost_db):
    _sub(cost_db, plan_price="100.00")
    _sub(cost_db, plan_price="200.00")
    rev = _compute_revenue_30d(cost_db, _COUNTY)
    assert rev == Decimal("300.00")


def test_revenue_excludes_free_tier(cost_db):
    _sub(cost_db, tier="free", plan_price="0.00")
    rev = _compute_revenue_30d(cost_db, _COUNTY)
    assert rev == Decimal("0")


def test_revenue_excludes_data_only(cost_db):
    _sub(cost_db, tier="data_only", plan_price="0.00")
    rev = _compute_revenue_30d(cost_db, _COUNTY)
    assert rev == Decimal("0")


def test_revenue_excludes_churned(cost_db):
    _sub(cost_db, status="churned", plan_price="99.00")
    rev = _compute_revenue_30d(cost_db, _COUNTY)
    assert rev == Decimal("0")


def test_revenue_excludes_other_county(cost_db):
    _sub(cost_db, county=_OTHER, plan_price="99.00")
    rev = _compute_revenue_30d(cost_db, _COUNTY)
    assert rev == Decimal("0")


def test_revenue_zero_when_no_subs(cost_db):
    rev = _compute_revenue_30d(cost_db, _COUNTY)
    assert rev == Decimal("0")


# ── P1-2 cost helper ──────────────────────────────────────────────────────────

def test_cost_sums_attributed_rows(cost_db):
    s = _sub(cost_db)
    _log(cost_db, subscriber_id=s.id, cost_usd="0.05")
    _log(cost_db, subscriber_id=s.id, cost_usd="0.03")
    cost = _compute_attributable_cost(cost_db, _COUNTY, _AGO_30, free_tier_only=False)
    assert cost == Decimal("0.08")


def test_cost_excludes_null_subscriber_id(cost_db):
    """Shared/system rows (subscriber_id IS NULL) must not be counted — ADR 0006."""
    _sub(cost_db)
    cost_db.execute(text("""
        INSERT INTO api_usage_logs (service, cost_usd, subscriber_id, created_at)
        VALUES ('claude', '1.00', NULL, :ts)
    """), {"ts": (_NOW - timedelta(days=5)).isoformat()})
    cost_db.flush()
    cost = _compute_attributable_cost(cost_db, _COUNTY, _AGO_30, free_tier_only=False)
    assert cost == Decimal("0")


def test_cost_excludes_other_county(cost_db):
    s_other = _sub(cost_db, county=_OTHER)
    _log(cost_db, subscriber_id=s_other.id, cost_usd="0.50")
    cost = _compute_attributable_cost(cost_db, _COUNTY, _AGO_30, free_tier_only=False)
    assert cost == Decimal("0")


def test_cost_free_tier_only_filters_correctly(cost_db):
    paying = _sub(cost_db, tier="starter", uuid="paying1")
    free = _sub(cost_db, tier="free", plan_price="0.00", uuid="free1")
    _log(cost_db, subscriber_id=paying.id, cost_usd="0.20")
    _log(cost_db, subscriber_id=free.id, cost_usd="0.05")

    free_only = _compute_attributable_cost(cost_db, _COUNTY, _AGO_30, free_tier_only=True)
    all_cost = _compute_attributable_cost(cost_db, _COUNTY, _AGO_30, free_tier_only=False)
    assert free_only == Decimal("0.05")
    assert all_cost == Decimal("0.25")


def test_cost_excludes_rows_before_window(cost_db):
    s = _sub(cost_db)
    _log(cost_db, subscriber_id=s.id, cost_usd="0.10",
         created_at=_NOW - timedelta(days=35))  # outside 30d window
    cost = _compute_attributable_cost(cost_db, _COUNTY, _AGO_30, free_tier_only=False)
    assert cost == Decimal("0")


# ── P1-3 gate grading ────────────────────────────────────────────────────────

def test_free_tier_ratio_green(cost_db):
    # 35% < 40 threshold → green
    assert _gate_color("free_tier_cost_ratio", 35.0) == "green"


def test_free_tier_ratio_yellow(cost_db):
    assert _gate_color("free_tier_cost_ratio", 45.0) == "yellow"


def test_free_tier_ratio_red(cost_db):
    assert _gate_color("free_tier_cost_ratio", 55.0) == "red"


def test_free_tier_ratio_none_is_red(cost_db):
    assert _gate_color("free_tier_cost_ratio", None) == "red"


def test_county_profitability_green_when_net_positive():
    assert _gate_color("county_profitability", 1.0) == "green"


def test_county_profitability_red_when_not_positive():
    assert _gate_color("county_profitability", 0.0) == "red"


# ── P1-4 ADR 0006 guard — Cost Ledger, not agent_decisions ───────────────────

def test_cost_uses_api_usage_logs_not_agent_decisions(cost_db):
    """_compute_attributable_cost must reference ApiUsageLog, not AgentDecision (ADR 0004)."""
    import inspect
    from src.tasks import kill_switch_metric_ingest as mod
    src = inspect.getsource(mod._compute_attributable_cost)
    assert "AgentDecision" not in src, (
        "_compute_attributable_cost must not reference AgentDecision (ADR 0004)"
    )
    assert "ApiUsageLog" in src, (
        "_compute_attributable_cost must query ApiUsageLog (the Cost Ledger)"
    )
