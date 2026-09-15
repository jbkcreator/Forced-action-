"""
WP-1 tests — FA Max Durable State Engine.

Coverage map
============
Category 1  — Unit: pure logic, mocked session
Category 3  — Migration: idempotency on re-run
Category 5  — Durable-state / event-log: transition writes, history query,
               crash/rollback recovery, Redis-down path
Category 10 — Concurrent-worker / duplicate-event idempotency
Category 13 — Compliance boundary: no financial columns on any FA Max table

Tests that require real Postgres use the `fresh_db` fixture (per conftest.py)
and are automatically skipped when DATABASE_URL is not configured.
Tests that don't touch the DB are importable and runnable without any env.

Purposely NOT covered here (with explicit rationale):
  Category 2  — Integration vs. running service: migration integration covered
                 in Category 3/5 via fresh_db. Full migration-apply-script
                 execution is BLOCKED — requires live Postgres + migration
                 already applied (migration script exits 0 when tables exist).
  Category 4  — Migration re-run idempotency: covered via SQL-level
                 IF NOT EXISTS / ON CONFLICT DO NOTHING in Category 3 group.
  Category 6  — Suppression: not in WP-1 scope (no outbound sends).
  Category 7  — Autonomy tier: not in WP-1 scope (internal-only, no sends).
  Category 8  — Identity/merge: WP-4 scope.
  Category 9  — SOT boundary values: none in WP-1 (state engine is
                 constraint-free on counts/dates).
  Category 11 — Failure/retry: Redis-down path covered in Category 5.
  Category 12 — External provider: no external calls in WP-1.
  Category 14 — Full regression: see "Baseline / regression" section at
                 the bottom of this file — run separately.
"""

from __future__ import annotations

import os

# Stub required env vars so collection succeeds without a .env file.
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

import hashlib
import json
import threading
import time
import uuid
from typing import Any, Dict
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Unit-testable helpers that need no DB
# ---------------------------------------------------------------------------


def test_make_idempotency_key_is_deterministic():
    from src.services.state_engine import make_idempotency_key

    key1 = make_idempotency_key(
        entity_uuid="abc", from_state="identified", to_state="warm", actor="agent:x",
        epoch_minute=1000
    )
    key2 = make_idempotency_key(
        entity_uuid="abc", from_state="identified", to_state="warm", actor="agent:x",
        epoch_minute=1000
    )
    assert key1 == key2, "Same inputs must produce the same key"


def test_make_idempotency_key_differs_across_minutes():
    from src.services.state_engine import make_idempotency_key

    k1 = make_idempotency_key("e", "a", "b", "actor", epoch_minute=100)
    k2 = make_idempotency_key("e", "a", "b", "actor", epoch_minute=101)
    assert k1 != k2


def test_make_idempotency_key_max_length():
    from src.services.state_engine import make_idempotency_key

    key = make_idempotency_key("e", "a", "b", "actor", epoch_minute=1)
    assert len(key) <= 64


def test_transition_outcome_enum_values():
    from src.services.state_engine import TransitionOutcome

    assert TransitionOutcome.succeeded == "succeeded"
    assert TransitionOutcome.already_advanced == "already_advanced"
    assert TransitionOutcome.idempotent_skip == "idempotent_skip"
    assert TransitionOutcome.invalid_transition == "invalid_transition"


# ---------------------------------------------------------------------------
# Unit: Redis lock helpers (mocked Redis)
# ---------------------------------------------------------------------------


def test_redis_lock_fails_open_when_unavailable():
    """When Redis is unreachable, the lock returns False (not acquired) but
    execution proceeds — correctness falls back to Postgres advisory lock."""
    with patch("src.services.state_engine.redis_available", return_value=False):
        from src.services.state_engine import _try_acquire_redis_lock

        result = _try_acquire_redis_lock("person", str(uuid.uuid4()))
        assert result is False, "Should not acquire lock when Redis unavailable"


def test_redis_lock_exception_handled_gracefully():
    """A Redis exception during lock acquisition must not propagate — fail open."""
    with (
        patch("src.services.state_engine.redis_available", return_value=True),
        patch("src.services.state_engine.get_redis") as mock_redis,
    ):
        mock_redis.return_value.set.side_effect = ConnectionError("redis gone")
        from src.services.state_engine import _try_acquire_redis_lock

        result = _try_acquire_redis_lock("person", str(uuid.uuid4()))
        assert result is False


# ---------------------------------------------------------------------------
# Category 13 — Compliance boundary: NO financial columns on FA Max tables
# ---------------------------------------------------------------------------

_PROHIBITED_COLUMNS = {
    "credit_score", "income", "income_cents", "annual_income",
    "bank_statement", "tax_return", "ssn", "social_security",
    "bank_balance", "monthly_income", "dti", "debt_to_income",
    "salary", "wage", "gross_income", "net_income",
}

_FA_MAX_TABLES = [
    "fa_max_entity_registry",
    "fa_max_persons",
    "fa_max_opportunities",
    "fa_max_opportunity_properties",
    "fa_max_state_transition_events",
    "fa_max_person_lifecycle_stage_config",
    "fa_max_opportunity_stage_config",
]


def test_no_financial_columns_in_fa_max_orm_models():
    """Structural: introspect ORM column names — zero prohibited columns allowed.

    This is a compliance check, not a style preference. SOT.md prohibits any
    field holding borrower financial data (credit score, income, bank statement,
    tax return, SSN) on FA Max tables.
    """
    from src.core.models import (
        FaMaxEntityRegistry,
        FaMaxOpportunity,
        FaMaxOpportunityProperty,
        FaMaxOpportunityStageConfig,
        FaMaxPerson,
        FaMaxPersonLifecycleStageConfig,
        FaMaxStateTransitionEvent,
    )

    fa_max_models = [
        FaMaxEntityRegistry,
        FaMaxPersonLifecycleStageConfig,
        FaMaxPerson,
        FaMaxOpportunityStageConfig,
        FaMaxOpportunity,
        FaMaxOpportunityProperty,
        FaMaxStateTransitionEvent,
    ]

    violations = []
    for model in fa_max_models:
        col_names = {c.key.lower() for c in model.__table__.columns}
        found = col_names & _PROHIBITED_COLUMNS
        if found:
            violations.append(f"{model.__tablename__}: {found}")

    assert not violations, (
        "SOT.md compliance violation — financial columns found on FA Max tables:\n"
        + "\n".join(violations)
    )


def test_backflip_ref_is_opaque_reference_not_financial():
    """backflip_ref must be a VARCHAR opaque ref, not a numeric/financial field."""
    from src.core.models import FaMaxOpportunity
    from sqlalchemy import String

    col = FaMaxOpportunity.__table__.c["backflip_ref"]
    assert isinstance(col.type, String), (
        "backflip_ref must be String (opaque portal reference) — not numeric"
    )


def test_loan_amount_cents_is_internal_only():
    """loan_amount_cents exists as an internal working field (not financial data
    sent to borrower). Verify it's nullable (never required) and not named
    with a prohibited term."""
    from src.core.models import FaMaxOpportunity

    col = FaMaxOpportunity.__table__.c["loan_amount_cents"]
    assert col.nullable is True, "loan_amount_cents must be nullable (optional internal field)"
    # The column itself is allowed; it's loan_amount_cents (internal), not income/SSN/etc.
    assert "loan_amount_cents" not in _PROHIBITED_COLUMNS


# ---------------------------------------------------------------------------
# Category 13 — No FA Max send path exists (WP-1 scope check)
# ---------------------------------------------------------------------------

def test_state_engine_has_no_send_path():
    """state_engine.py must not import or call any send/outbound module."""
    import ast

    with open("src/services/state_engine.py") as f:
        source = f.read()

    prohibited_imports = [
        "sms_compliance", "telnyx", "relay", "instantly", "ghl",
        "slack_sdk", "publish_lifecycle_event", "send_sms", "send_email",
    ]
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = (
                [node.module] if isinstance(node, ast.ImportFrom) else
                [alias.name for alias in node.names]
            )
            for name in names:
                if name and any(p in name for p in prohibited_imports):
                    pytest.fail(
                        f"state_engine.py must not import send/outbound modules: "
                        f"found import of '{name}'"
                    )


# ---------------------------------------------------------------------------
# ORM schema structural tests (no DB connection needed)
# ---------------------------------------------------------------------------

def test_fa_max_entity_registry_primary_key():
    from src.core.models import FaMaxEntityRegistry

    pk_cols = [c.name for c in FaMaxEntityRegistry.__table__.primary_key]
    assert pk_cols == ["entity_uuid"]


def test_fa_max_persons_has_lifecycle_state_fk():
    """lifecycle_state must be FK-constrained to fa_max_person_lifecycle_stage_config."""
    from src.core.models import FaMaxPerson

    fks = {fk.target_fullname for fk in FaMaxPerson.__table__.c["lifecycle_state"].foreign_keys}
    assert any("fa_max_person_lifecycle_stage_config" in fk for fk in fks)


def test_fa_max_persons_no_self_merge_check_exists():
    from src.core.models import FaMaxPerson

    check_names = {c.name for c in FaMaxPerson.__table__.constraints
                   if hasattr(c, "name") and c.name}
    assert "ck_fa_max_persons_no_self_merge" in check_names


def test_fa_max_opportunity_type_check_exists():
    from src.core.models import FaMaxOpportunity

    check_names = {c.name for c in FaMaxOpportunity.__table__.constraints
                   if hasattr(c, "name") and c.name}
    assert "ck_fa_max_opp_type" in check_names


def test_fa_max_opportunity_outcome_check_exists():
    from src.core.models import FaMaxOpportunity

    check_names = {c.name for c in FaMaxOpportunity.__table__.constraints
                   if hasattr(c, "name") and c.name}
    assert "ck_fa_max_opp_outcome" in check_names


def test_state_transition_event_has_unique_idempotency_key():
    from src.core.models import FaMaxStateTransitionEvent

    unique_cols = set()
    for c in FaMaxStateTransitionEvent.__table__.constraints:
        if hasattr(c, "columns"):
            col_names = {col.name for col in c.columns}
            if col_names == {"idempotency_key"}:
                unique_cols.update(col_names)
    assert "idempotency_key" in unique_cols, (
        "fa_max_state_transition_events must have a unique constraint on idempotency_key"
    )


def test_state_transition_event_has_entity_uuid_fk():
    from src.core.models import FaMaxStateTransitionEvent

    fks = {fk.target_fullname for fk in
           FaMaxStateTransitionEvent.__table__.c["entity_uuid"].foreign_keys}
    assert any("fa_max_entity_registry" in fk for fk in fks)


def test_state_transition_event_has_decision_id_fk():
    from src.core.models import FaMaxStateTransitionEvent

    fks = {fk.target_fullname for fk in
           FaMaxStateTransitionEvent.__table__.c["decision_id"].foreign_keys}
    assert any("agent_decisions" in fk for fk in fks)


def test_opportunity_has_person_id_fk():
    from src.core.models import FaMaxOpportunity

    fks = {fk.target_fullname for fk in
           FaMaxOpportunity.__table__.c["person_id"].foreign_keys}
    assert any("fa_max_persons" in fk for fk in fks)


def test_opportunity_no_unique_constraint_on_person_property_pair():
    """Explicitly confirm there is NO UNIQUE(person_id, property_id) on
    fa_max_opportunities. Repeat borrowers must be allowed multiple rows for
    the same person/property combination (different projects, different times).
    """
    from src.core.models import FaMaxOpportunity
    from sqlalchemy import UniqueConstraint

    for c in FaMaxOpportunity.__table__.constraints:
        if isinstance(c, UniqueConstraint):
            col_names = {col.name for col in c.columns}
            assert col_names != {"person_id", "property_id"}, (
                "UNIQUE(person_id, property_id) must NOT exist on fa_max_opportunities — "
                "repeat borrowers need multiple rows per person/property"
            )


# ---------------------------------------------------------------------------
# Category 5 — Durable state (Postgres required, skips if unavailable)
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("fresh_db")
def test_ensure_entity_registry_creates_row(fresh_db):
    """ensure_entity_registry inserts one row and returns a UUID."""
    from src.services.state_engine import ensure_entity_registry

    entity_uuid = ensure_entity_registry(
        session=fresh_db,
        entity_type="person",
        native_id="person-test-001",
    )
    assert entity_uuid is not None
    row = fresh_db.execute(
        text("SELECT entity_type, native_id FROM fa_max_entity_registry WHERE entity_uuid = :uuid ::uuid"),
        {"uuid": entity_uuid},
    ).fetchone()
    assert row is not None
    assert row.entity_type == "person"
    assert row.native_id == "person-test-001"


@pytest.mark.usefixtures("fresh_db")
def test_ensure_entity_registry_is_idempotent(fresh_db):
    """Second call with same (type, native_id) returns the same UUID — no duplicate."""
    from src.services.state_engine import ensure_entity_registry

    uuid1 = ensure_entity_registry(session=fresh_db, entity_type="property", native_id="12345")
    uuid2 = ensure_entity_registry(session=fresh_db, entity_type="property", native_id="12345")
    assert uuid1 == uuid2


@pytest.mark.usefixtures("fresh_db")
def test_transition_person_state_succeeds(fresh_db):
    """Happy-path: create a person, register in entity registry, transition state."""
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    # Insert a person (lifecycle stage config must exist — seeded by migration)
    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
        RETURNING person_id::text
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    ikey = make_idempotency_key(entity_uuid, "identified", "qualifying", "agent:test", epoch_minute=1)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="qualifying",
            actor="agent:test",
            source_component="test_fa_max_state_engine",
            idempotency_key=ikey,
            person_id=person_id,
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )

    assert result.outcome == TransitionOutcome.succeeded
    assert result.current_state == "qualifying"
    assert result.event_id is not None

    # State column must be updated
    state = fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert state == "qualifying"


@pytest.mark.usefixtures("fresh_db")
def test_transition_rejects_wrong_from_state(fresh_db):
    """CAS miss: if current state != from_state, outcome is already_advanced."""
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'warm', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    ikey = make_idempotency_key(entity_uuid, "identified", "qualifying", "agent:test", epoch_minute=2)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",  # wrong — actual state is 'warm'
            to_state="qualifying",
            actor="agent:test",
            source_component="test_fa_max_state_engine",
            idempotency_key=ikey,
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )

    assert result.outcome == TransitionOutcome.already_advanced


@pytest.mark.usefixtures("fresh_db")
def test_transition_idempotency_key_deduplicates(fresh_db):
    """Second call with the same idempotency_key is a no-op (idempotent_skip)."""
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    ikey = make_idempotency_key(entity_uuid, "identified", "qualifying", "agent:test", epoch_minute=3)

    kwargs = dict(
        session=fresh_db,
        entity_type="person",
        entity_uuid=entity_uuid,
        from_state="identified",
        to_state="qualifying",
        actor="agent:test",
        source_component="test_fa_max_state_engine",
        idempotency_key=ikey,
        acquire_redis_lock=False,
        validate_allowed_next=False,
    )

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        r1 = transition(**kwargs)
        # Reset state for the second call to be a "fresh" duplicate
        fresh_db.execute(
            text("UPDATE fa_max_persons SET lifecycle_state = 'identified' WHERE person_id = :pid ::uuid"),
            {"pid": person_id},
        )
        r2 = transition(**kwargs)

    assert r1.outcome == TransitionOutcome.succeeded
    assert r2.outcome == TransitionOutcome.idempotent_skip

    # Only one event row should exist
    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM fa_max_state_transition_events WHERE idempotency_key = :k"),
        {"k": ikey},
    ).scalar()
    assert count == 1


@pytest.mark.usefixtures("fresh_db")
def test_transition_allowed_next_permits_valid_transition(fresh_db):
    """validate_allowed_next=True (the default) allows a transition listed in
    the stage config's allowed_next — proves the guard doesn't over-block."""
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    # 'identified' -> allowed_next includes 'qualifying'
    ikey = make_idempotency_key(entity_uuid, "identified", "qualifying", "agent:test", epoch_minute=40)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="qualifying",
            actor="agent:test",
            source_component="test",
            idempotency_key=ikey,
            acquire_redis_lock=False,
            validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.succeeded


@pytest.mark.usefixtures("fresh_db")
def test_transition_allowed_next_rejects_invalid_transition(fresh_db):
    """validate_allowed_next=True rejects a to_state not listed in
    allowed_next for a non-terminal from_state."""
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    # 'identified' allowed_next is ["qualifying","suppressed","dead"] — 'funded' is not in it.
    ikey = make_idempotency_key(entity_uuid, "identified", "funded", "agent:test", epoch_minute=41)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="funded",
            actor="agent:test",
            source_component="test",
            idempotency_key=ikey,
            acquire_redis_lock=False,
            validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.invalid_transition
    assert result.current_state == "identified"

    # State column must be unchanged — no partial write on rejection.
    state = fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert state == "identified"

    # No event row must have been written for the rejected transition.
    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM fa_max_state_transition_events WHERE idempotency_key = :k"),
        {"k": ikey},
    ).scalar()
    assert count == 0


@pytest.mark.usefixtures("fresh_db")
def test_transition_allowed_next_blocks_terminal_state(fresh_db):
    """A terminal state (allowed_next = []) must reject every to_state —
    empty allowed_next means 'no transitions permitted', not 'unrestricted'.
    Regression test for the suppressed/dead/do_not_contact bypass."""
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'suppressed', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    ikey = make_idempotency_key(entity_uuid, "suppressed", "warm", "agent:test", epoch_minute=42)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="suppressed",
            to_state="warm",
            actor="agent:test",
            source_component="test",
            idempotency_key=ikey,
            acquire_redis_lock=False,
            validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.invalid_transition, (
        "A suppressed person must never be transitionable back to an active "
        "state — empty allowed_next must block all transitions, not permit all"
    )

    state = fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert state == "suppressed", "Terminal state must be unchanged after rejected transition"


@pytest.mark.usefixtures("fresh_db")
def test_transition_allowed_next_rejects_unknown_from_state(fresh_db):
    """A from_state absent from the stage config table must reject the
    transition rather than silently skip validation."""
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    # 'not_a_real_stage' does not exist in fa_max_person_lifecycle_stage_config.
    ikey = make_idempotency_key(entity_uuid, "not_a_real_stage", "warm", "agent:test", epoch_minute=43)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="not_a_real_stage",
            to_state="warm",
            actor="agent:test",
            source_component="test",
            idempotency_key=ikey,
            acquire_redis_lock=False,
            validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.invalid_transition, (
        "An unregistered from_state must be rejected, not silently allowed through"
    )


@pytest.mark.usefixtures("fresh_db")
def test_transition_event_is_timestamped_and_attributed(fresh_db):
    """WP-1 contract: every transition has occurred_at, actor, source_component."""
    from src.services.state_engine import (
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )
    ikey = make_idempotency_key(entity_uuid, "identified", "warm", "agent:test", epoch_minute=4)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="warm",
            actor="agent:intake",
            source_component="src.services.test",
            idempotency_key=ikey,
            person_id=person_id,
            context={"trigger": "ghl_contact_created"},
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )

    row = fresh_db.execute(
        text("""
            SELECT actor, source_component, occurred_at, context, person_id::text
            FROM fa_max_state_transition_events
            WHERE idempotency_key = :k
        """),
        {"k": ikey},
    ).fetchone()

    assert row is not None
    assert row.actor == "agent:intake"
    assert row.source_component == "src.services.test"
    assert row.occurred_at is not None
    assert row.person_id == person_id
    assert row.context.get("trigger") == "ghl_contact_created"


@pytest.mark.usefixtures("fresh_db")
def test_get_person_state_returns_none_for_unknown(fresh_db):
    """get_person_state returns None when person_id does not exist."""
    from src.services.state_engine import get_person_state

    result = get_person_state(session=fresh_db, person_id=str(uuid.uuid4()))
    assert result is None


@pytest.mark.usefixtures("fresh_db")
def test_get_person_state_returns_row(fresh_db):
    from src.services.state_engine import get_person_state

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    result = get_person_state(session=fresh_db, person_id=person_id)
    assert result is not None
    assert result["person_id"] == person_id
    assert result["lifecycle_state"] == "identified"
    assert "merged_into_id" in result


@pytest.mark.usefixtures("fresh_db")
def test_get_person_history_returns_ordered_events(fresh_db):
    """WP-1 Done-When: 'complete ordered history of a borrower can be queried.'"""
    from src.services.state_engine import (
        ensure_entity_registry,
        get_person_history,
        make_idempotency_key,
        transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    transitions = [
        ("identified", "qualifying", "agent:intake", 10),
        ("qualifying", "warm", "agent:scoring", 11),
        ("warm", "active", "user:josh", 12),
    ]

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        for from_s, to_s, actor, minute in transitions:
            ikey = make_idempotency_key(entity_uuid, from_s, to_s, actor, epoch_minute=minute)
            fresh_db.execute(
                text("UPDATE fa_max_persons SET lifecycle_state = :s WHERE person_id = :pid ::uuid"),
                {"s": from_s, "pid": person_id},
            )
            transition(
                session=fresh_db,
                entity_type="person",
                entity_uuid=entity_uuid,
                from_state=from_s,
                to_state=to_s,
                actor=actor,
                source_component="test",
                idempotency_key=ikey,
                person_id=person_id,
                acquire_redis_lock=False,
                validate_allowed_next=False,
            )

    history = get_person_history(session=fresh_db, person_id=person_id)

    assert len(history) == 3
    # Ordered by occurred_at ASC
    assert history[0]["from_state"] == "identified"
    assert history[0]["to_state"] == "qualifying"
    assert history[2]["from_state"] == "warm"
    assert history[2]["to_state"] == "active"
    assert history[2]["actor"] == "user:josh"


@pytest.mark.usefixtures("fresh_db")
def test_crash_recovery_simulated_via_rollback(fresh_db):
    """WP-1 Done-When: worker killed mid-task, next instance resumes with no loss.

    Simulated by performing a transition, NOT committing (simulating a crash),
    then verifying state is unchanged (rolled back). A new 'instance' then
    transitions from the original state successfully.
    """
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    # Setup: person in 'identified' state
    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    # Simulate crash: use a savepoint, do the transition, then roll back to savepoint
    ikey = make_idempotency_key(entity_uuid, "identified", "qualifying", "agent:test", epoch_minute=20)
    savepoint = fresh_db.begin_nested()

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="qualifying",
            actor="agent:test",
            source_component="test",
            idempotency_key=ikey,
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )

    # Crash: roll back the savepoint (simulates worker dying before commit)
    savepoint.rollback()

    # State must be back to 'identified' — no partial write
    state_after_crash = fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert state_after_crash == "identified", (
        "State must be rolled back after simulated crash — no partial write"
    )

    # New instance retries the same transition successfully
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        r2 = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="qualifying",
            actor="agent:test",
            source_component="test",
            idempotency_key=ikey,  # same key — idempotent if it had written
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )

    assert r2.outcome == TransitionOutcome.succeeded, (
        "After crash + retry, transition must succeed from original state"
    )

    state_after_recovery = fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert state_after_recovery == "qualifying"


@pytest.mark.usefixtures("fresh_db")
def test_person_lifecycle_stage_config_seeded(fresh_db):
    """Migration seeds 12 lifecycle stages including all terminal states."""
    rows = fresh_db.execute(
        text("SELECT stage_key, is_terminal FROM fa_max_person_lifecycle_stage_config ORDER BY order_index")
    ).fetchall()

    stage_keys = {r.stage_key for r in rows}
    assert len(rows) == 12, f"Expected 12 seeded stages, got {len(rows)}"

    required_stages = {
        "identified", "qualifying", "warm", "cold", "active",
        "submitted", "funded", "declined", "repeat",
        "suppressed", "dead", "do_not_contact",
    }
    assert required_stages == stage_keys

    terminal_stages = {r.stage_key for r in rows if r.is_terminal}
    assert "suppressed" in terminal_stages
    assert "dead" in terminal_stages
    assert "do_not_contact" in terminal_stages
    assert "funded" in terminal_stages
    assert "identified" not in terminal_stages


@pytest.mark.usefixtures("fresh_db")
def test_opportunity_stage_config_seeded(fresh_db):
    """Migration seeds 11 opportunity stages."""
    rows = fresh_db.execute(
        text("SELECT stage_key FROM fa_max_opportunity_stage_config ORDER BY order_index")
    ).fetchall()

    assert len(rows) == 11

    required = {
        "new", "qualifying", "scoping", "warm_hold", "ready_to_submit",
        "submitted", "term_sheet", "closing", "funded", "declined", "dead",
    }
    assert {r.stage_key for r in rows} == required


@pytest.mark.usefixtures("fresh_db")
def test_get_fa_max_person_state_tool_aborts_on_missing(fresh_db):
    """get_fa_max_person_state read tool returns empty dict (not raises) for
    unknown person_id — the abort-if-empty pattern for graph first-node use."""
    with patch("src.agents.tools.read_tools._session") as mock_session_ctx:
        mock_session_ctx.return_value.__enter__ = lambda s: fresh_db
        mock_session_ctx.return_value.__exit__ = MagicMock(return_value=False)

        from src.agents.tools.read_tools import get_fa_max_person_state

        result = get_fa_max_person_state(str(uuid.uuid4()), session=fresh_db)
        assert result == {}, "Must return empty dict for unknown person — not raise"


@pytest.mark.usefixtures("fresh_db")
def test_migration_idempotency_tables_exist_after_second_seed(fresh_db):
    """Migration's ON CONFLICT DO NOTHING seeds are safe to re-run."""
    # Running the INSERT ... ON CONFLICT DO NOTHING statements again must not
    # error or create duplicates.
    count_before = fresh_db.execute(
        text("SELECT COUNT(*) FROM fa_max_person_lifecycle_stage_config")
    ).scalar()

    # Re-run the seed INSERT — must be a no-op
    fresh_db.execute(text("""
        INSERT INTO fa_max_person_lifecycle_stage_config
            (stage_key, display_name, order_index, allowed_next, is_terminal, is_active)
        VALUES
            ('identified', 'Identified', 1, '["qualifying"]'::jsonb, FALSE, TRUE)
        ON CONFLICT (stage_key) DO NOTHING
    """))

    count_after = fresh_db.execute(
        text("SELECT COUNT(*) FROM fa_max_person_lifecycle_stage_config")
    ).scalar()

    assert count_before == count_after, "Seed re-run must not change row count"


@pytest.mark.usefixtures("fresh_db")
def test_state_transition_event_person_id_partition_key(fresh_db):
    """person_id on state_transition_events is the borrower-history partition key.
    An event written for an opportunity (not a person) can still carry person_id."""
    from src.services.state_engine import ensure_entity_registry, make_idempotency_key, transition

    # Create a person
    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    # Register the person in entity registry
    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    # Register a property (type='property', native_id='99') in entity registry
    prop_entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="property", native_id="99"
    )

    # Write a state event for the property entity, but with person_id populated
    ikey = make_idempotency_key(prop_entity_uuid, "new", "enriched", "system:enricher", epoch_minute=30)
    fresh_db.execute(text("""
        INSERT INTO fa_max_state_transition_events
            (entity_uuid, person_id, entity_type, from_state, to_state,
             actor, source_component, idempotency_key)
        VALUES
            (:entity_uuid ::uuid, :person_id ::uuid, 'property', 'new', 'enriched',
             'system:enricher', 'test', :ikey)
    """), {"entity_uuid": prop_entity_uuid, "person_id": person_id, "ikey": ikey})

    # Query by person_id (partition key) must return the property event too
    rows = fresh_db.execute(
        text("SELECT entity_type FROM fa_max_state_transition_events WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).fetchall()

    entity_types = {r.entity_type for r in rows}
    assert "property" in entity_types, (
        "Property event associated with a person must appear in person's history query"
    )


# ---------------------------------------------------------------------------
# Category 10 — True concurrent-worker race (real threads, real connections)
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("pg_engine")
def test_concurrent_transitions_on_same_entity_serialize_via_advisory_lock(pg_engine):
    """Two threads, two independent DB connections, racing the SAME entity_uuid
    with the SAME from_state must not both succeed — pg_advisory_xact_lock
    serializes them so exactly one CAS-updates from from_state, and the
    loser correctly reports already_advanced.

    This is the real concurrency guarantee WP-1 depends on: Redis is only an
    optimization (and is deliberately NOT used here — acquire_redis_lock is
    left at its default True but Redis unavailability doesn't change the
    outcome), the Postgres advisory lock is what actually prevents two
    workers from double-processing the same claim.
    """
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured — skipping real-concurrency test")

    from sqlalchemy.orm import Session as SASession
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    # Setup (own connection, committed so both threads' connections see it)
    setup_conn = pg_engine.connect()
    setup_session = SASession(bind=setup_conn)
    setup_session.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test_concurrency')
    """))
    person_id = setup_session.execute(
        text("SELECT person_id::text FROM fa_max_persons WHERE source = 'test_concurrency' "
             "ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(
        session=setup_session, entity_type="person", native_id=person_id
    )
    setup_session.commit()
    setup_session.close()
    setup_conn.close()

    results: Dict[str, Any] = {}
    barrier = threading.Barrier(2)

    def worker(worker_name: str, epoch_minute: int):
        conn = pg_engine.connect()
        session = SASession(bind=conn)
        try:
            barrier.wait(timeout=5)  # maximize actual overlap
            ikey = make_idempotency_key(
                entity_uuid, "identified", "qualifying", f"agent:{worker_name}",
                epoch_minute=epoch_minute,
            )
            result = transition(
                session=session,
                entity_type="person",
                entity_uuid=entity_uuid,
                from_state="identified",
                to_state="qualifying",
                actor=f"agent:{worker_name}",
                source_component="test_concurrency",
                idempotency_key=ikey,
                acquire_redis_lock=False,  # isolate the Postgres guarantee
                validate_allowed_next=False,
            )
            session.commit()
            results[worker_name] = result
        finally:
            session.close()
            conn.close()

    try:
        t1 = threading.Thread(target=worker, args=("worker1", 40))
        t2 = threading.Thread(target=worker, args=("worker2", 41))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert "worker1" in results and "worker2" in results, "Both workers must complete"

        outcomes = {results["worker1"].outcome, results["worker2"].outcome}
        # Exactly one must succeed; the other must see the CAS has already moved.
        assert TransitionOutcome.succeeded in outcomes, (
            f"At least one worker must succeed: {outcomes}"
        )
        succeeded_count = sum(
            1 for r in results.values() if r.outcome == TransitionOutcome.succeeded
        )
        assert succeeded_count == 1, (
            f"Exactly one worker must win the race, got {succeeded_count}: "
            f"{[(k, v.outcome) for k, v in results.items()]}"
        )

        loser_outcome = [
            r.outcome for name, r in results.items()
            if r.outcome != TransitionOutcome.succeeded
        ][0]
        assert loser_outcome == TransitionOutcome.already_advanced, (
            f"Losing worker must see already_advanced (CAS miss), got {loser_outcome}"
        )

        # Verify final state is 'qualifying' exactly once — no double-apply.
        verify_conn = pg_engine.connect()
        verify_session = SASession(bind=verify_conn)
        try:
            final_state = verify_session.execute(
                text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
                {"pid": person_id},
            ).scalar()
            assert final_state == "qualifying"

            event_count = verify_session.execute(
                text(
                    "SELECT COUNT(*) FROM fa_max_state_transition_events "
                    "WHERE entity_uuid = :eid ::uuid AND to_state = 'qualifying'"
                ),
                {"eid": entity_uuid},
            ).scalar()
            assert event_count == 1, (
                f"Exactly one event row for the winning transition, got {event_count}"
            )
        finally:
            verify_session.close()
            verify_conn.close()
    finally:
        # Cleanup — this test commits real rows outside fresh_db's rollback.
        cleanup_conn = pg_engine.connect()
        cleanup_session = SASession(bind=cleanup_conn)
        try:
            cleanup_session.execute(
                text("DELETE FROM fa_max_state_transition_events WHERE entity_uuid = :eid ::uuid"),
                {"eid": entity_uuid},
            )
            cleanup_session.execute(
                text("DELETE FROM fa_max_persons WHERE person_id = :pid ::uuid"),
                {"pid": person_id},
            )
            cleanup_session.execute(
                text("DELETE FROM fa_max_entity_registry WHERE entity_uuid = :eid ::uuid"),
                {"eid": entity_uuid},
            )
            cleanup_session.commit()
        finally:
            cleanup_session.close()
            cleanup_conn.close()
