"""Entitlement snapshot re-trigger — src/services/entitlement_sync.py.

Covers the failure mode that required a manual backfill migration: a plan-catalog
edit leaves every existing account on a stale `lead_entitlement` snapshot, which
`lead_delivery.bucket_for()` reads instead of the catalog.
"""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.services.entitlement_sync import (
    find_entitlement_drift,
    resync_lead_entitlements,
)

STARTER = {"ultra": 2, "platinum": 5, "gold": 20, "silver": 50, "bronze": 10}
PRO = {"ultra": 4, "platinum": 10, "gold": 40, "silver": 100, "bronze": 20}

# Minimal stand-ins for the two tables the sync touches. The real models carry
# Postgres server_defaults ('{}'::jsonb) that SQLite cannot parse, and the shared
# in_memory_db fixture's JSONB adapter is declared but never installed.
_DDL = [
    """CREATE TABLE plans (
        plan_id TEXT PRIMARY KEY,
        name TEXT,
        tier TEXT,
        price_cents INTEGER,
        interval TEXT,
        entitlements JSON
    )""",
    """CREATE TABLE customer_accounts (
        account_id TEXT PRIMARY KEY,
        plan_tier TEXT,
        lead_entitlement JSON,
        status TEXT
    )""",
]


@pytest.fixture
def db():
    """Isolated SQLite engine holding only the two tables this module touches."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        for stmt in _DDL:
            conn.execute(text(stmt))
    session = sessionmaker(bind=engine)()
    yield session
    session.close()
    engine.dispose()


def _seed_plan(db, plan_id: str, entitlements: dict, tier: str = "starter") -> None:
    db.execute(
        text(
            "INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements) "
            "VALUES (:plan_id, :name, :tier, 29900, 'monthly', :ent)"
        ),
        {
            "plan_id": plan_id,
            "name": plan_id.title(),
            "tier": tier,
            "ent": json.dumps(entitlements),
        },
    )


def _seed_account(db, plan_id: str, snapshot: dict) -> str:
    account_id = str(uuid.uuid4())
    db.execute(
        text(
            "INSERT INTO customer_accounts (account_id, plan_tier, lead_entitlement, status) "
            "VALUES (:aid, :plan, :snap, 'active')"
        ),
        {"aid": account_id, "plan": plan_id, "snap": json.dumps(snapshot)},
    )
    return account_id


def _snapshot_of(db, account_id: str) -> dict:
    raw = db.execute(
        text("SELECT lead_entitlement FROM customer_accounts WHERE account_id = :aid"),
        {"aid": account_id},
    ).scalar_one()
    if raw is None:
        return {}
    return json.loads(raw) if isinstance(raw, str) else raw


def test_empty_snapshot_is_repaired(db):
    """The exact PR #170 case — an account stuck on {} after a catalog edit."""
    _seed_plan(db, "pro", PRO)
    account_id = _seed_account(db, "pro", {})

    result = resync_lead_entitlements(db)

    assert result.updated == 1
    assert _snapshot_of(db, account_id) == PRO


def test_stale_nonempty_snapshot_is_repaired(db):
    """A re-priced plan must reach accounts that already had a populated snapshot."""
    _seed_plan(db, "pro", PRO)
    account_id = _seed_account(db, "pro", STARTER)

    result = resync_lead_entitlements(db)

    assert result.updated == 1
    assert _snapshot_of(db, account_id) == PRO


def test_in_sync_account_is_untouched(db):
    """Idempotency — a second run after a repair updates nothing."""
    _seed_plan(db, "pro", PRO)
    _seed_account(db, "pro", PRO)

    result = resync_lead_entitlements(db)

    assert result.updated == 0
    assert result.drifted == []


def test_empty_catalog_never_revokes_a_populated_snapshot(db):
    """An unconfigured plan must not wipe a paying account's entitlement."""
    _seed_plan(db, "unconfigured", {})
    account_id = _seed_account(db, "unconfigured", PRO)

    result = resync_lead_entitlements(db)

    assert result.updated == 0
    assert _snapshot_of(db, account_id) == PRO


def test_plan_scope_limits_blast_radius(db):
    """Scoping to the edited plan leaves other plans' accounts alone."""
    _seed_plan(db, "pro", PRO)
    _seed_plan(db, "starter", STARTER)
    pro_account = _seed_account(db, "pro", {})
    starter_account = _seed_account(db, "starter", {})

    result = resync_lead_entitlements(db, plan_ids=["pro"])

    assert result.updated == 1
    assert _snapshot_of(db, pro_account) == PRO
    assert _snapshot_of(db, starter_account) == {}


def test_dry_run_reports_without_writing(db):
    _seed_plan(db, "pro", PRO)
    account_id = _seed_account(db, "pro", {})

    result = resync_lead_entitlements(db, dry_run=True)

    assert result.dry_run is True
    assert result.updated == 0
    assert len(result.drifted) == 1
    assert result.drifted[0].catalog == PRO
    assert _snapshot_of(db, account_id) == {}


def test_drift_report_carries_both_sides(db):
    _seed_plan(db, "pro", PRO)
    _seed_account(db, "pro", STARTER)

    drift = find_entitlement_drift(db)

    assert len(drift) == 1
    assert drift[0].plan_id == "pro"
    assert drift[0].snapshot == STARTER
    assert drift[0].catalog == PRO


def test_account_on_unknown_plan_is_ignored(db):
    """plan_tier with no matching catalog row must not raise or be touched."""
    account_id = _seed_account(db, "ghost_plan", {"gold": 1})

    result = resync_lead_entitlements(db)

    assert result.updated == 0
    assert _snapshot_of(db, account_id) == {"gold": 1}


def test_multiple_plans_repaired_in_one_pass(db):
    _seed_plan(db, "pro", PRO)
    _seed_plan(db, "starter", STARTER)
    pro_account = _seed_account(db, "pro", {})
    starter_account = _seed_account(db, "starter", {})

    result = resync_lead_entitlements(db)

    assert result.updated == 2
    assert result.plan_ids == {"pro", "starter"}
    assert _snapshot_of(db, pro_account) == PRO
    assert _snapshot_of(db, starter_account) == STARTER
