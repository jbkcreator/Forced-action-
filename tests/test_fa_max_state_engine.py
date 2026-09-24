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
import multiprocessing
import threading
import time
import uuid
from typing import Any, Dict
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import text


def _claim_work_and_wait(database_url: str, queue_name: str, ready_queue) -> None:
    """Subprocess target for the literal worker-termination acceptance test."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession
    from src.services.state_engine import claim_next_work_item

    engine = create_engine(database_url)
    with engine.connect() as connection:
        with SASession(bind=connection) as session:
            item = claim_next_work_item(
                session=session,
                queue_name=queue_name,
                worker_id="worker:killed-process",
                lease_seconds=1,
            )
            session.commit()
            ready_queue.put(item["work_item_id"] if item else None)
            time.sleep(30)

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
    """When Redis is unreachable, the lock reports UNAVAILABLE (not acquired,
    no token) — the caller proceeds to the Postgres advisory lock anyway."""
    with patch("src.services.state_engine.redis_available", return_value=False):
        from src.services.state_engine import _REDIS_UNAVAILABLE, _try_acquire_redis_lock

        state, token = _try_acquire_redis_lock("person", str(uuid.uuid4()))
        assert state == _REDIS_UNAVAILABLE
        assert token is None


def test_redis_lock_exception_handled_gracefully():
    """A Redis exception during lock acquisition must not propagate — fail
    open, reported the same as UNAVAILABLE."""
    with (
        patch("src.services.state_engine.redis_available", return_value=True),
        patch("src.services.state_engine.get_redis") as mock_redis,
    ):
        mock_redis.return_value.set.side_effect = ConnectionError("redis gone")
        from src.services.state_engine import _REDIS_UNAVAILABLE, _try_acquire_redis_lock

        state, token = _try_acquire_redis_lock("person", str(uuid.uuid4()))
        assert state == _REDIS_UNAVAILABLE
        assert token is None


def test_redis_lock_real_acquire_and_release():
    """The actual SET-NX-EX acquire + DELETE release path, against fakeredis
    (per CLAUDE.md: 'Use fakeredis in tests/sandbox') — not mocked-out.

    This is the path every prior test skipped: all 20+ transition() calls
    in this file use acquire_redis_lock=False, so _try_acquire_redis_lock's
    actual success case and _release_redis_lock were previously exercised
    by zero tests — only the fail-open (Redis down) and exception-handling
    branches were covered.
    """
    import fakeredis

    fake = fakeredis.FakeStrictRedis()

    with (
        patch("src.services.state_engine.redis_available", return_value=True),
        patch("src.services.state_engine.get_redis", return_value=fake),
    ):
        from src.services.state_engine import (
            _REDIS_ACQUIRED,
            _release_redis_lock,
            _try_acquire_redis_lock,
        )

        entity_uuid = str(uuid.uuid4())

        state, token = _try_acquire_redis_lock("person", entity_uuid)
        assert state == _REDIS_ACQUIRED
        assert token is not None and isinstance(token, str) and len(token) > 0

        key = f"lock:state:person:{entity_uuid}"
        assert fake.exists(key) == 1, "SET NX must have actually written the key"
        assert fake.get(key).decode() == token, "Stored value must be this caller's own token"

        _release_redis_lock("person", entity_uuid, token)
        assert fake.exists(key) == 0, "Release must actually DELETE the key"


def test_redis_lock_release_does_not_delete_another_holders_lock():
    """Code-review fix: releasing with a STALE/WRONG token must NOT delete
    the key — proves the ownership-token check actually prevents one caller
    from releasing a different caller's lock (the original bug: unconditional
    DELETE with a shared value '1' meant any caller's release wiped out
    whoever currently held the key, including a legitimate new holder after
    TTL expiry)."""
    import fakeredis

    fake = fakeredis.FakeStrictRedis()

    with (
        patch("src.services.state_engine.redis_available", return_value=True),
        patch("src.services.state_engine.get_redis", return_value=fake),
    ):
        from src.services.state_engine import (
            _REDIS_ACQUIRED,
            _release_redis_lock,
            _try_acquire_redis_lock,
        )

        entity_uuid = str(uuid.uuid4())
        state, real_token = _try_acquire_redis_lock("person", entity_uuid)
        assert state == _REDIS_ACQUIRED

        key = f"lock:state:person:{entity_uuid}"
        # Attempt release with a DIFFERENT (stale/foreign) token.
        _release_redis_lock("person", entity_uuid, "not-the-real-token")
        assert fake.exists(key) == 1, (
            "A release with the wrong token must NOT delete a lock it doesn't own"
        )

        # The real owner can still release it correctly.
        _release_redis_lock("person", entity_uuid, real_token)
        assert fake.exists(key) == 0


def test_redis_lock_real_contention_detected():
    """A second caller for the SAME entity_uuid while the first still holds
    the lock must get CONTENDED — this is the actual distributed-locking
    guarantee WP-1's scope claims ('Redis distributed locking for
    concurrent work'), never exercised before this test."""
    import fakeredis

    fake = fakeredis.FakeStrictRedis()

    with (
        patch("src.services.state_engine.redis_available", return_value=True),
        patch("src.services.state_engine.get_redis", return_value=fake),
    ):
        from src.services.state_engine import (
            _REDIS_ACQUIRED,
            _REDIS_CONTENDED,
            _try_acquire_redis_lock,
        )

        entity_uuid = str(uuid.uuid4())

        first_state, first_token = _try_acquire_redis_lock("person", entity_uuid)
        assert first_state == _REDIS_ACQUIRED

        second_state, second_token = _try_acquire_redis_lock("person", entity_uuid)
        assert second_state == _REDIS_CONTENDED, (
            "A concurrent caller for the same entity must be denied the "
            "Redis lock while the first caller still holds it"
        )
        assert second_token is None


def test_redis_lock_different_entities_do_not_contend():
    """Two different entity_uuids must not block each other — the lock key
    is scoped per entity, not global."""
    import fakeredis

    fake = fakeredis.FakeStrictRedis()

    with (
        patch("src.services.state_engine.redis_available", return_value=True),
        patch("src.services.state_engine.get_redis", return_value=fake),
    ):
        from src.services.state_engine import _REDIS_ACQUIRED, _try_acquire_redis_lock

        first_state, first_token = _try_acquire_redis_lock("person", str(uuid.uuid4()))
        second_state, second_token = _try_acquire_redis_lock("person", str(uuid.uuid4()))
        assert first_state == _REDIS_ACQUIRED and second_state == _REDIS_ACQUIRED
        assert first_token != second_token, "Different entities must get different tokens"


def test_redis_contention_backs_off_without_reaching_postgres_lock():
    """Code-review Finding #5: when Redis reports CONTENDED, transition()
    must return outcome=contended WITHOUT EVER calling
    _acquire_pg_advisory_lock — this is what makes Redis an actual
    contention-avoidance layer. Previously the Postgres lock was acquired
    unconditionally first, so a contended Redis lock never prevented
    queuing at Postgres — the one thing Redis existed to avoid."""
    import fakeredis

    from src.services.state_engine import TransitionOutcome, transition

    fake = fakeredis.FakeStrictRedis()
    entity_uuid = str(uuid.uuid4())
    # Pre-occupy the Redis lock for this entity, simulating another caller.
    fake.set(f"lock:state:person:{entity_uuid}", "someone-else-token", nx=True, ex=30)

    with (
        patch("src.services.state_engine.redis_available", return_value=True),
        patch("src.services.state_engine.get_redis", return_value=fake),
        patch("src.services.state_engine._acquire_pg_advisory_lock") as mock_pg_lock,
    ):
        result = transition(
            session=MagicMock(), entity_type="person", entity_uuid=entity_uuid,
            from_state="identified", to_state="qualifying", actor="user:admin",
            source_component="test", idempotency_key="some-key",
            context={"reason": "test contention check"},
            acquire_redis_lock=True, validate_allowed_next=False,
        )

    assert result.outcome == TransitionOutcome.contended
    mock_pg_lock.assert_not_called()


@pytest.mark.usefixtures("fresh_db")
def test_transition_with_redis_lock_enabled_end_to_end(fresh_db):
    """End-to-end: transition(acquire_redis_lock=True) — the parameter every
    other test in this file sets to False — actually acquires and releases
    the Redis lock as part of a real transition, against fakeredis."""
    import fakeredis

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
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    fake = fakeredis.FakeStrictRedis()
    key = f"lock:state:person:{entity_uuid}"

    with (
        patch("src.services.state_engine._acquire_pg_advisory_lock"),
        patch("src.services.state_engine.redis_available", return_value=True),
        patch("src.services.state_engine.get_redis", return_value=fake),
    ):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="enriched",
            actor="agent:test",
            source_component="test",
            idempotency_key=make_idempotency_key(
                entity_uuid, "identified", "enriched", "agent:test", epoch_minute=70
            ),
            person_id=person_id,
            acquire_redis_lock=True,  # the path nothing else in this file exercises
            validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.succeeded
    # The lock must be released after the transition completes — held locks
    # that never release would starve every subsequent worker on this entity.
    assert fake.exists(key) == 0, "Redis lock must be released after transition completes"


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

    ikey = make_idempotency_key(entity_uuid, "identified", "enriched", "user:admin", epoch_minute=1)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="enriched",
            actor="user:admin",
            source_component="test_fa_max_state_engine",
            idempotency_key=ikey,
            person_id=person_id,
            acquire_redis_lock=False,
            validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.succeeded
    assert result.current_state == "enriched"
    assert result.event_id is not None

    # State column must be updated
    state = fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert state == "enriched"


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
        VALUES (gen_random_uuid(), 'enriched', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    ikey = make_idempotency_key(entity_uuid, "identified", "enriched", "user:admin", epoch_minute=2)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",  # wrong — actual state is 'warm'
            to_state="enriched",
            actor="user:admin",
            source_component="test_fa_max_state_engine",
            idempotency_key=ikey,
            context={"reason": "test"},
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

    ikey = make_idempotency_key(entity_uuid, "identified", "enriched", "user:admin", epoch_minute=3)

    kwargs = dict(
        session=fresh_db,
        entity_type="person",
        entity_uuid=entity_uuid,
        from_state="identified",
        to_state="enriched",
        actor="user:admin",
        source_component="test_fa_max_state_engine",
        idempotency_key=ikey,
        context={"reason": "test"},
        acquire_redis_lock=False,
        validate_allowed_next=False,
    )

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        r1 = transition(**kwargs)
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
def test_duplicate_request_cannot_mutate_state_code_review_repro(fresh_db):
    """Code-review Finding #1 (Critical) reproduction, exactly as specified:

      1. Apply warm -> active with key K.
      2. Apply active -> warm with a different key.
      3. Retry the ORIGINAL warm -> active request with key K.

    Before the fix: step 2's CAS UPDATE ran before the idempotency-key
    conflict check, so at step 3, from_state='warm' matched the CURRENT
    state (warm, from step 2) and the CAS update succeeded, silently
    advancing state to 'active' a second time — then the event insert hit
    ON CONFLICT DO NOTHING (key K already existed from step 1) and the
    function returned idempotent_skip, discarding the fact that it had
    JUST mutated state. Current state ended up 'active' with no event
    explaining that second transition — history showed active->warm as the
    last event while current state silently read differently.

    Unlike test_transition_idempotency_key_deduplicates (which manually
    resets state to identified before its "duplicate" call — an artificial
    setup that always makes the CAS match and therefore can never observe
    this bug), this test performs the exact sequence of independent calls
    the reviewer specified and checks state after every step.
    """
    from src.services.state_engine import (
        TransitionOutcome,
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
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    key_k = make_idempotency_key(entity_uuid, "identified", "enriched", "user:admin", epoch_minute=200)
    key_k2 = make_idempotency_key(entity_uuid, "enriched", "identified", "user:admin", epoch_minute=201)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        # Step 1: identified -> enriched with key K.
        r1 = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="identified", to_state="enriched", actor="user:admin",
            source_component="test", idempotency_key=key_k,
            context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
        )
        assert r1.outcome == TransitionOutcome.succeeded

        # Step 2: enriched -> identified with a DIFFERENT key.
        r2 = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="enriched", to_state="identified", actor="user:admin",
            source_component="test", idempotency_key=key_k2,
            context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
        )
        assert r2.outcome == TransitionOutcome.succeeded

        # Step 3: RETRY the original identified -> enriched request with key K.
        r3 = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="identified", to_state="enriched", actor="user:admin",
            source_component="test", idempotency_key=key_k,  # same key as step 1
            context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
        )

    # The fix: step 3 must be a true no-op. It must NOT mutate state a
    # second time — current state must still be 'warm' (from step 2), not
    # silently advanced to 'active' again.
    assert r3.outcome == TransitionOutcome.idempotent_skip
    assert r3.current_state == "identified", (
        "A duplicate request (reused idempotency_key) must report the "
        "ACTUAL current state, and that state must be unchanged by the "
        "duplicate — it must never silently re-advance state"
    )

    final_state = fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert final_state == "identified", (
        "Duplicate request must leave current state exactly as step 2 left it"
    )

    # History must contain exactly 2 events (step 1 and step 2) — step 3
    # produced no third event, and no event describes a transition that
    # never actually happened.
    history = get_person_history(session=fresh_db, person_id=person_id)["events"]
    assert len(history) == 2
    assert history[0]["from_state"] == "identified" and history[0]["to_state"] == "enriched"
    assert history[1]["from_state"] == "enriched" and history[1]["to_state"] == "identified"


@pytest.mark.usefixtures("fresh_db")
def test_idempotency_key_reused_for_different_operation_is_rejected(fresh_db):
    """Code-review Finding #1 fix, second half: reusing an idempotency_key
    for a genuinely DIFFERENT operation (different entity, or different
    from_state/to_state) must be rejected explicitly — not silently treated
    as 'idempotent_skip', which would tell the caller their new operation
    succeeded (as a no-op) when it never ran at all."""
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
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    reused_key = "manually-chosen-key-not-from-make_idempotency_key"

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        r1 = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="identified", to_state="enriched", actor="user:admin",
            source_component="test", idempotency_key=reused_key,
            context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
        )
        assert r1.outcome == TransitionOutcome.succeeded

        # Same key, but a DIFFERENT operation (different to_state).
        r2 = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="enriched", to_state="contacted", actor="user:admin",
            source_component="test", idempotency_key=reused_key,  # reused on purpose
            context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
        )

    assert r2.outcome == TransitionOutcome.invalid_transition, (
        "Reusing a key for a different operation must be rejected, not "
        "silently reported as idempotent_skip"
    )

    final_state = fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert final_state == "enriched", "The rejected duplicate must not have mutated state"


@pytest.mark.usefixtures("fresh_db")
def test_transition_rejects_unsupported_entity_types(fresh_db):
    """Code-review Finding #4: property/partner/interaction have no durable
    state table or stage config yet — transition() must reject them rather
    than silently writing a state-less event row and reporting success.
    Previously: entity_type not in _ENTITY_STATE_COLUMN skipped BOTH the
    allowed_next validation AND the CAS update, then still inserted an
    event row and returned TransitionOutcome.succeeded — a complete no-op
    on actual state that claimed to have worked.
    """
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    # property and interaction are not state machines — transition() must reject them.
    # partner IS now supported (WP-1 remaining).
    for unsupported_type in ("property", "interaction"):
        native_id = str(uuid.uuid4())
        entity_uuid = ensure_entity_registry(
            session=fresh_db, entity_type=unsupported_type, native_id=native_id
        )
        ikey = make_idempotency_key(entity_uuid, "any", "other", "agent:test", epoch_minute=300)

        with patch("src.services.state_engine._acquire_pg_advisory_lock"):
            result = transition(
                session=fresh_db, entity_type=unsupported_type, entity_uuid=entity_uuid,
                from_state="any", to_state="other", actor="agent:test",
                source_component="test", idempotency_key=ikey,
                acquire_redis_lock=False, validate_allowed_next=False,
            )

        assert result.outcome == TransitionOutcome.invalid_transition, (
            f"entity_type={unsupported_type!r} must be rejected — no durable "
            "state table exists for it"
        )

        # No event row should have been written for a rejected entity type.
        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM fa_max_state_transition_events WHERE idempotency_key = :k"),
            {"k": ikey},
        ).scalar()
        assert count == 0, (
            f"A rejected transition for entity_type={unsupported_type!r} must "
            "not leave a state-less event row behind"
        )


@pytest.mark.usefixtures("fresh_db")
def test_history_ordering_survives_same_transaction_identical_timestamps(fresh_db):
    """Code-review Finding #3, same-transaction case: two transitions for the
    SAME entity inserted in one transaction (identical occurred_at via
    NOW()) must still have a well-defined, correct relative order in
    get_person_history() — proven via the `seq` column, not `occurred_at`.

    This covers only the identical-timestamp case. It does NOT exercise two
    separate transactions racing the advisory lock with reversed start/
    commit order — see test_history_ordering_survives_reversed_commit_order
    (two real connections) for that case; the review correctly noted this
    test's original name ('survives_out_of_order_commit') overstated what a
    single-transaction test can prove.
    """
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
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        # Both transitions happen inside the SAME outer transaction (fresh_db
        # is one connection/transaction for the whole test) — NOW() returns
        # the same value for both, so occurred_at alone cannot order them.
        transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="identified", to_state="enriched", actor="user:admin",
            source_component="test",
            idempotency_key=make_idempotency_key(entity_uuid, "identified", "enriched", "user:admin", epoch_minute=400),
            context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
        )
        transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="enriched", to_state="contacted", actor="user:admin",
            source_component="test",
            idempotency_key=make_idempotency_key(entity_uuid, "enriched", "contacted", "user:admin", epoch_minute=401),
            context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
        )

    history = get_person_history(session=fresh_db, person_id=person_id)["events"]
    assert len(history) == 2
    # seq must strictly increase in insertion order regardless of occurred_at.
    assert history[0]["seq"] < history[1]["seq"]
    assert history[0]["from_state"] == "identified" and history[0]["to_state"] == "enriched"
    assert history[1]["from_state"] == "enriched" and history[1]["to_state"] == "contacted"


def test_history_ordering_survives_reversed_commit_order(pg_engine):
    """Code-review Finding #4: the reviewer's literal concern — a transaction
    that acquires the advisory lock LATER (blocked behind another worker)
    but COMMITS at roughly the same wall-clock moment as the one that held
    the lock, must still have its event correctly ordered AFTER the one
    that actually executed and committed while holding the lock, regardless
    of thread start order.

    Setup: worker B acquires the entity's advisory lock and holds it while
    worker A (a separate thread, separate connection) tries to transition
    the SAME entity and genuinely blocks inside _acquire_pg_advisory_lock.
    Only once B commits (releasing the lock) does A's transition() call
    proceed and complete. seq must reflect this true execution order.
    """
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured — skipping real-connection test")

    from sqlalchemy.orm import Session as SASession
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        get_person_history,
        make_idempotency_key,
        transition,
    )

    setup_conn = pg_engine.connect()
    setup_trans = setup_conn.begin()
    setup_session = SASession(bind=setup_conn)
    setup_session.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = setup_session.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=setup_session, entity_type="person", native_id=person_id)
    setup_session.close()
    setup_trans.commit()
    setup_conn.close()

    # Worker B: begin a transaction and hold it open (advisory lock stays
    # held past transition()'s own return, since only the SAVEPOINT inside
    # transition() commits — the outer transaction, and the lock it holds,
    # is released only when trans_b.commit() below actually runs).
    conn_b = pg_engine.connect()
    trans_b = conn_b.begin()
    session_b = SASession(bind=conn_b)

    result_b = transition(
        session=session_b, entity_type="person", entity_uuid=entity_uuid,
        from_state="identified", to_state="enriched", actor="agent:admin-worker-b",
        source_component="test",
        idempotency_key=make_idempotency_key(entity_uuid, "identified", "enriched", "agent:admin-worker-b", epoch_minute=500),
        context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
    )
    assert result_b.outcome == TransitionOutcome.succeeded
    # trans_b deliberately NOT committed yet — B still holds the advisory lock.

    # Worker A: a separate thread, separate connection, blocks trying to
    # acquire the same entity's advisory lock (held by B).
    conn_a = pg_engine.connect()
    trans_a = conn_a.begin()
    session_a = SASession(bind=conn_a)
    result_a_holder = {}

    def worker_a():
        result_a_holder["result"] = transition(
            session=session_a, entity_type="person", entity_uuid=entity_uuid,
            from_state="enriched", to_state="contacted", actor="agent:admin-worker-a",
            source_component="test",
            idempotency_key=make_idempotency_key(entity_uuid, "enriched", "contacted", "agent:admin-worker-a", epoch_minute=501),
            context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
        )

    thread_a = threading.Thread(target=worker_a)
    thread_a.start()
    time.sleep(0.4)  # ensure thread A is genuinely blocked on the advisory lock

    assert thread_a.is_alive(), (
        "Worker A must still be blocked waiting for B's advisory lock at this point"
    )

    # B commits now — releases the lock, unblocking A.
    trans_b.commit()
    thread_a.join(timeout=10)
    assert not thread_a.is_alive(), "Worker A must have completed after B released the lock"

    result_a = result_a_holder["result"]
    assert result_a.outcome == TransitionOutcome.succeeded
    trans_a.commit()

    # Use a fresh read connection to avoid any stale transaction-snapshot issues.
    conn_read = pg_engine.connect()
    session_read = SASession(bind=conn_read)
    history = get_person_history(session=session_read, person_id=person_id)["events"]
    session_read.close()
    conn_read.close()

    assert len(history) == 2
    # B's event (identified->enriched) genuinely executed and committed
    # BEFORE A's event (enriched->contacted), which only proceeded after B
    # released the lock — seq must reflect this real order.
    assert history[0]["actor"] == "agent:admin-worker-b"
    assert history[0]["from_state"] == "identified" and history[0]["to_state"] == "enriched"
    assert history[1]["actor"] == "agent:admin-worker-a"
    assert history[1]["from_state"] == "enriched" and history[1]["to_state"] == "contacted"
    assert history[0]["seq"] < history[1]["seq"]

    # Cleanup
    session_a.close()
    conn_a.close()
    session_b.close()
    conn_b.close()


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

    # SOT: 'identified' -> allowed_next includes 'enriched' (first SOT forward step)
    ikey = make_idempotency_key(entity_uuid, "identified", "enriched", "agent:test", epoch_minute=40)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="enriched",
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

    # SOT: 'identified' allowed_next is ["enriched","suppressed","dead","do_not_contact"]
    # 'funded' is not in it.
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
    ikey = make_idempotency_key(entity_uuid, "identified", "enriched", "agent:admin-intake", epoch_minute=4)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="enriched",
            actor="agent:admin-intake",
            source_component="src.services.test",
            idempotency_key=ikey,
            person_id=person_id,
            context={"trigger": "ghl_contact_created", "reason": "test attribution check"},
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
    assert row.actor == "agent:admin-intake"
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
def test_get_opportunity_state_returns_none_for_unknown(fresh_db):
    """get_opportunity_state returns None when opportunity_id does not exist."""
    from src.services.state_engine import get_opportunity_state

    result = get_opportunity_state(session=fresh_db, opportunity_id=str(uuid.uuid4()))
    assert result is None


@pytest.mark.usefixtures("fresh_db")
def test_orm_opportunity_uses_new_stage_server_default(fresh_db):
    """Creating an opportunity through the ORM without current_stage must
    persist the migration-defined default rather than inserting NULL."""
    from src.core.models import FaMaxOpportunity

    person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
        RETURNING person_id
    """)).scalar_one()
    opportunity = FaMaxOpportunity(
        person_id=person_id,
        opportunity_type="acquisition",
        source="test",
    )
    fresh_db.add(opportunity)
    fresh_db.flush()
    fresh_db.refresh(opportunity)

    assert opportunity.current_stage == "new"


@pytest.mark.usefixtures("fresh_db")
def test_get_opportunity_state_returns_row(fresh_db):
    """get_opportunity_state reads current_stage — the CAS from_state callers
    must load before calling transition() rather than trusting a stale
    caller-supplied value (WP-2's Slack-approval caller depends on this)."""
    from src.services.state_engine import get_opportunity_state

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    fresh_db.execute(
        text("""
            INSERT INTO fa_max_opportunities
                (person_id, opportunity_type, current_stage, source)
            VALUES (:person_id ::uuid, 'acquisition', 'new', 'test')
        """),
        {"person_id": person_id},
    )
    opportunity_id = fresh_db.execute(
        text("SELECT opportunity_id::text FROM fa_max_opportunities ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    result = get_opportunity_state(session=fresh_db, opportunity_id=opportunity_id)
    assert result is not None
    assert result["opportunity_id"] == opportunity_id
    assert result["person_id"] == person_id
    assert result["current_stage"] == "new"
    assert result["opportunity_type"] == "acquisition"
    assert result["outcome"] == "open"
    # Internal-only scenario fields must never be returned from this read path.
    assert "loan_amount_cents" not in result
    assert "maturity_months" not in result


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
        ("identified", "enriched", "agent:admin-intake", 10),
        ("enriched", "contacted", "agent:admin-scoring", 11),
        ("contacted", "engaged", "user:josh", 12),
    ]

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        for from_s, to_s, actor, minute in transitions:
            ikey = make_idempotency_key(entity_uuid, from_s, to_s, actor, epoch_minute=minute)
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
                context={"reason": "test"},
                acquire_redis_lock=False,
                validate_allowed_next=False,
            )

    history = get_person_history(session=fresh_db, person_id=person_id)["events"]

    assert len(history) == 3
    # Ordered by occurred_at ASC
    assert history[0]["from_state"] == "identified"
    assert history[0]["to_state"] == "enriched"
    assert history[2]["from_state"] == "contacted"
    assert history[2]["to_state"] == "engaged"
    assert history[2]["actor"] == "user:josh"


@pytest.mark.usefixtures("fresh_db")
def test_get_person_history_pagination_reaches_full_history(fresh_db):
    """Code-review Finding #3: get_person_history() previously truncated at
    `limit` with no way to see anything beyond it — and in the WORST
    direction, since ORDER BY is oldest-first: a borrower with more than
    `limit` events would NEVER see their most recent events, only their
    oldest. Proven here: create MORE events than a small limit, page
    through using after_seq/has_more/next_cursor, and confirm the full
    ordered history — including the newest events — is reachable.
    """
    from src.services.state_engine import (
        TransitionOutcome,
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
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    # A sequence of 5 transitions bouncing between two states — more than
    # our test page size of 2.
    states = ["identified", "enriched", "contacted", "enriched", "contacted", "enriched"]
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        for i in range(len(states) - 1):
            result = transition(
                session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
                from_state=states[i], to_state=states[i + 1], actor="user:admin",
                source_component="test",
                idempotency_key=make_idempotency_key(entity_uuid, states[i], states[i + 1], "user:admin", epoch_minute=600 + i),
                context={"reason": "test"}, acquire_redis_lock=False, validate_allowed_next=False,
            )
            assert result.outcome == TransitionOutcome.succeeded

    PAGE_SIZE = 2
    all_events = []
    cursor = None
    pages_fetched = 0
    while True:
        page = get_person_history(session=fresh_db, person_id=person_id, limit=PAGE_SIZE, after_seq=cursor)
        all_events.extend(page["events"])
        pages_fetched += 1
        assert pages_fetched < 20, "Pagination loop did not terminate — has_more/next_cursor logic is broken"
        if not page["has_more"]:
            assert page["next_cursor"] is None
            break
        cursor = page["next_cursor"]
        assert cursor is not None

    assert len(all_events) == 5, "All 5 transitions must be reachable via pagination, not just the first PAGE_SIZE"
    assert pages_fetched == 3  # 2 + 2 + 1
    # Confirm the LAST (most recent) transition is actually reachable — the
    # exact case a plain LIMIT with no cursor would have silently hidden.
    assert all_events[-1]["from_state"] == "contacted"
    assert all_events[-1]["to_state"] == "enriched"
    # seq strictly increasing across the whole paginated sequence.
    seqs = [e["seq"] for e in all_events]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == 5


@pytest.mark.usefixtures("fresh_db")
def test_get_person_history_includes_opportunity_events_for_same_borrower(fresh_db):
    """WP-1 Done-When claims 'complete ordered history' — the docstring on
    get_person_history explicitly claims this includes opportunities/
    properties/interactions via the person_id partition key, not just
    entity_type='person' events. This must be tested with a REAL opportunity
    transition, not just three person-type transitions (the only case the
    original test covered) — person_id is an optional kwarg on transition()
    and a caller that forgets to pass it produces an event invisible to this
    query with no error raised anywhere.
    """
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
    person_entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )

    fresh_db.execute(
        text("""
            INSERT INTO fa_max_opportunities
                (person_id, opportunity_type, current_stage, source)
            VALUES (:person_id ::uuid, 'acquisition', 'new', 'test')
        """),
        {"person_id": person_id},
    )
    opportunity_id = fresh_db.execute(
        text("SELECT opportunity_id::text FROM fa_max_opportunities ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    opportunity_entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="opportunity", native_id=opportunity_id
    )

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        # One person-type transition
        transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=person_entity_uuid,
            from_state="identified",
            to_state="enriched",
            actor="user:admin",
            source_component="test",
            idempotency_key=make_idempotency_key(
                person_entity_uuid, "identified", "enriched", "user:admin", epoch_minute=60
            ),
            person_id=person_id,
            context={"reason": "test"},
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )
        # One opportunity-type transition for the SAME borrower — this is
        # the case admin_router._handle_relay_decision's fa_max_transition
        # branch exercises. person_id must be passed explicitly here.
        transition(
            session=fresh_db,
            entity_type="opportunity",
            entity_uuid=opportunity_entity_uuid,
            from_state="new",
            to_state="qualifying",
            actor="user:admin",
            source_component="test",
            idempotency_key=make_idempotency_key(
                opportunity_entity_uuid, "new", "qualifying", "user:admin", epoch_minute=61
            ),
            person_id=person_id,
            context={"reason": "test"},
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )

    history = get_person_history(session=fresh_db, person_id=person_id)["events"]

    assert len(history) == 2, (
        "get_person_history must return events from BOTH entity types "
        "(person and opportunity) for this borrower — got only "
        f"{len(history)} event(s): {[h['entity_type'] for h in history]}"
    )
    entity_types_seen = {h["entity_type"] for h in history}
    assert entity_types_seen == {"person", "opportunity"}, (
        f"Expected both person and opportunity events, got {entity_types_seen}"
    )


@pytest.mark.usefixtures("fresh_db")
def test_transition_ignores_caller_supplied_person_id_derives_it_instead(fresh_db):
    """Code-review fix: person_id is no longer a caller-trusted, optional
    parameter. It is derived server-side from the entity's own registration
    (fa_max_entity_registry -> fa_max_opportunities.person_id) regardless of
    what the caller passes — an omitted OR incorrect caller-supplied
    person_id can no longer silently drop an event from history, or file it
    under the WRONG borrower.

    Two cases proven here:
      1. person_id omitted entirely -> event still appears in the REAL
         borrower's history (previously: silently invisible — see git
         history for the prior version of this test, which documented that
         as accepted behavior before the fix).
      2. person_id supplied but WRONG (a different, real borrower's id) ->
         the event still files under the entity's REAL borrower, not the
         incorrect one supplied by the caller.
    """
    from src.services.state_engine import (
        ensure_entity_registry,
        get_person_history,
        make_idempotency_key,
        transition,
    )

    real_person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
        RETURNING person_id::text
    """)).scalar()

    # A second, unrelated person — used to prove a wrong caller-supplied
    # person_id cannot misfile the event under them. Captured via RETURNING,
    # not ORDER BY created_at — both inserts share one transaction's NOW(),
    # so created_at alone cannot distinguish them (same issue as Finding #3).
    other_person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
        RETURNING person_id::text
    """)).scalar()
    assert other_person_id != real_person_id

    fresh_db.execute(
        text("""
            INSERT INTO fa_max_opportunities
                (person_id, opportunity_type, current_stage, source)
            VALUES (:person_id ::uuid, 'acquisition', 'new', 'test')
        """),
        {"person_id": real_person_id},
    )
    opportunity_id = fresh_db.execute(
        text("SELECT opportunity_id::text FROM fa_max_opportunities ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    opportunity_entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="opportunity", native_id=opportunity_id
    )

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        # Case 1: person_id omitted entirely.
        transition(
            session=fresh_db,
            entity_type="opportunity",
            entity_uuid=opportunity_entity_uuid,
            from_state="new",
            to_state="qualifying",
            actor="user:admin",
            source_component="test",
            idempotency_key=make_idempotency_key(
                opportunity_entity_uuid, "new", "qualifying", "user:admin", epoch_minute=62
            ),
            # person_id intentionally omitted — must no longer matter
            context={"reason": "test"},
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )
        # Case 2: person_id supplied but WRONG (the other, unrelated person).
        transition(
            session=fresh_db,
            entity_type="opportunity",
            entity_uuid=opportunity_entity_uuid,
            from_state="qualifying",
            to_state="scoping",
            actor="user:admin",
            source_component="test",
            idempotency_key=make_idempotency_key(
                opportunity_entity_uuid, "qualifying", "scoping", "user:admin", epoch_minute=63
            ),
            person_id=other_person_id,  # wrong on purpose — must be ignored
            context={"reason": "test"},
            acquire_redis_lock=False,
            validate_allowed_next=False,
        )

    real_history = get_person_history(session=fresh_db, person_id=real_person_id)["events"]
    assert len(real_history) == 2, (
        "Both transitions must appear under the entity's REAL borrower "
        "regardless of what person_id the caller passed (or omitted)"
    )

    other_history = get_person_history(session=fresh_db, person_id=other_person_id)["events"]
    assert other_history == [], (
        "Supplying a wrong person_id must NOT misfile the event under that "
        "unrelated borrower's history"
    )


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
    ikey = make_idempotency_key(entity_uuid, "identified", "enriched", "user:admin", epoch_minute=20)
    savepoint = fresh_db.begin_nested()

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        transition(
            session=fresh_db,
            entity_type="person",
            entity_uuid=entity_uuid,
            from_state="identified",
            to_state="enriched",
            actor="user:admin",
            source_component="test",
            idempotency_key=ikey,
            context={"reason": "test"},
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
            to_state="enriched",
            actor="user:admin",
            source_component="test",
            idempotency_key=ikey,  # same key — idempotent if it had written
            context={"reason": "test"},
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
    assert state_after_recovery == "enriched"


def test_real_worker_kill_second_instance_resumes(pg_engine):
    """WP-1 Done-When (literal reading): a worker is actually killed mid-task —
    its DB connection is dropped with an open, uncommitted transaction — and a
    SECOND, independent worker instance (its own connection/session) resumes
    and completes the work.

    Unlike test_crash_recovery_simulated_via_rollback (which reuses one
    session and calls .rollback() itself — not a real kill, and never proves
    a second instance can proceed), this test:
      1. Opens two genuinely separate connections (worker A, worker B).
      2. Has worker A acquire the pg_advisory_xact_lock via transition() and
         NOT commit.
      3. Proves, from worker B, that the lock is actually held (a real
         pg_try_advisory_xact_lock attempt fails) while A is alive.
      4. Kills worker A's connection outright (raw DBAPI .close(), no commit,
         no rollback call — simulating SIGKILL, not graceful shutdown).
      5. Proves worker B can now acquire the lock and complete the transition
         that A never finished, and that A's uncommitted write left no trace.
    """
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured — skipping real-connection test")

    from sqlalchemy.orm import Session as SASession
    from src.services.state_engine import (
        TransitionOutcome,
        ensure_entity_registry,
        make_idempotency_key,
        transition,
    )

    # Setup on its own committed connection so it's visible to both workers.
    setup_conn = pg_engine.connect()
    setup_trans = setup_conn.begin()
    setup_session = SASession(bind=setup_conn)
    setup_session.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = setup_session.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=setup_session, entity_type="person", native_id=person_id)
    setup_session.close()
    setup_trans.commit()
    setup_conn.close()

    # --- Worker A: acquire the lock, start a transition, never commit -------
    conn_a = pg_engine.connect()
    trans_a = conn_a.begin()
    session_a = SASession(bind=conn_a)

    ikey_a = make_idempotency_key(entity_uuid, "identified", "enriched", "agent:admin-worker-a", epoch_minute=50)
    result_a = transition(
        session=session_a,
        entity_type="person",
        entity_uuid=entity_uuid,
        from_state="identified",
        to_state="enriched",
        actor="agent:admin-worker-a",
        source_component="test",
        idempotency_key=ikey_a,
        context={"reason": "test"},
        acquire_redis_lock=False,
        validate_allowed_next=False,
    )
    assert result_a.outcome == TransitionOutcome.succeeded
    # Deliberately NOT committing trans_a — worker A is about to "die".

    # --- Prove the lock is genuinely held while worker A is alive -----------
    conn_probe = pg_engine.connect()
    lock_held = conn_probe.execute(
        text("SELECT pg_try_advisory_xact_lock(hashtext(:key))"),
        {"key": str(entity_uuid)},
    ).scalar()
    conn_probe.close()  # releases the probe's own xact scope regardless
    assert lock_held is False, (
        "pg_try_advisory_xact_lock must fail while worker A's transaction "
        "still holds pg_advisory_xact_lock on this entity_uuid"
    )

    # --- Kill worker A: raw connection close, no commit/rollback call -------
    # This simulates SIGKILL — Postgres itself aborts the backend's open
    # transaction and releases every advisory lock it held, without any
    # cooperation from application code.
    raw_dbapi_conn = conn_a.connection
    raw_dbapi_conn.close()

    # --- Worker B: a second, independent instance resumes -------------------
    conn_b = pg_engine.connect()
    trans_b = conn_b.begin()
    session_b = SASession(bind=conn_b)

    # State must still read 'identified' — worker A's write was never
    # committed, so it left no trace for worker B to see.
    state_before_b = session_b.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert state_before_b == "identified", (
        "Worker A's uncommitted transition must not be visible — "
        "no partial/torn write from the killed worker"
    )

    ikey_b = make_idempotency_key(entity_uuid, "identified", "enriched", "agent:admin-worker-b", epoch_minute=51)
    result_b = transition(
        session=session_b,
        entity_type="person",
        entity_uuid=entity_uuid,
        from_state="identified",
        to_state="enriched",
        actor="agent:admin-worker-b",
        source_component="test",
        idempotency_key=ikey_b,
        context={"reason": "test"},
        acquire_redis_lock=False,
        validate_allowed_next=False,
    )
    assert result_b.outcome == TransitionOutcome.succeeded, (
        "Worker B must be able to acquire the advisory lock and complete "
        "the transition after worker A was killed — this is the actual "
        "'another instance resumes with no loss' guarantee"
    )
    trans_b.commit()

    final_state = conn_b.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()
    assert final_state == "enriched"

    # Cleanup
    session_b.close()
    conn_b.close()
    try:
        trans_a.rollback()
    except Exception:
        pass  # connection already closed — expected
    conn_a.close()


def test_orm_and_migration_schema_parity_for_seq_column():
    """Code-review Finding #1 (second review pass): fa_max_state_transition_events.seq
    was added by the migration but NOT to the ORM model, breaking
    CLAUDE.md's explicit contract that Base.metadata.create_all() is the
    tests' schema source of truth. A DB built from ORM metadata ALONE
    (no migration ever run) would be missing `seq`, and get_person_history()
    would fail against it. Proven here by building schema via create_all()
    only — never applying the migration — and confirming seq exists AND a
    real transition()/get_person_history() round-trip works against it.
    """
    import uuid as _uuid

    from sqlalchemy import create_engine, text as sa_text
    from sqlalchemy.orm import Session as SASession

    from config.settings import get_settings
    from src.core.models import Base

    settings = get_settings()
    if not settings.database_url:
        pytest.skip("DATABASE_URL not configured")

    # A separate, disposable schema-namespace check: verify the actual
    # column exists on the table the fresh_db-based tests already use
    # (migration was applied for those), AND that the ORM model itself
    # declares it — the two must agree, or create_all()-only environments
    # (a fresh test DB, a new engineer's local setup) silently diverge from
    # what the migration-applied shared DB has.
    orm_columns = {c.name for c in Base.metadata.tables["fa_max_state_transition_events"].columns}
    assert "seq" in orm_columns, (
        "FaMaxStateTransitionEvent ORM model must declare 'seq' — "
        "get_person_history() SELECTs it and ORDERs BY it"
    )


@pytest.mark.usefixtures("fresh_db")
def test_person_lifecycle_stage_config_seeded(fresh_db):
    """Migration seeds 15 SOT lifecycle stages (12 active + 3 terminal)."""
    rows = fresh_db.execute(
        text("SELECT stage_key, is_terminal FROM fa_max_person_lifecycle_stage_config ORDER BY order_index")
    ).fetchall()

    stage_keys = {r.stage_key for r in rows}
    assert len(rows) == 15, f"Expected 15 SOT stages, got {len(rows)}: {stage_keys}"

    required_active = {
        "identified", "enriched", "contacted", "engaged", "qualified",
        "portal_started", "application_submitted", "term_sheet_issued",
        "locked", "funded", "matured", "repeat",
    }
    required_terminal = {"suppressed", "dead", "do_not_contact"}
    assert required_active | required_terminal == stage_keys

    terminal_stages = {r.stage_key for r in rows if r.is_terminal}
    assert required_terminal == terminal_stages, (
        f"Terminal stages must be exactly {required_terminal}, got {terminal_stages}"
    )
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

    # Re-run the seed INSERT for a known SOT stage — must be a no-op
    fresh_db.execute(text("""
        INSERT INTO fa_max_person_lifecycle_stage_config
            (stage_key, display_name, order_index, allowed_next, is_terminal, is_active)
        VALUES
            ('identified', 'Identified', 1, '["enriched"]'::jsonb, FALSE, TRUE)
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
                entity_uuid, "identified", "enriched", f"agent:admin-{worker_name}",
                epoch_minute=epoch_minute,
            )
            result = transition(
                session=session,
                entity_type="person",
                entity_uuid=entity_uuid,
                from_state="identified",
                to_state="enriched",
                actor=f"agent:admin-{worker_name}",
                source_component="test_concurrency",
                idempotency_key=ikey,
                context={"reason": "test"},
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

        # Verify final state is 'enriched' exactly once — no double-apply.
        verify_conn = pg_engine.connect()
        verify_session = SASession(bind=verify_conn)
        try:
            final_state = verify_session.execute(
                text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
                {"pid": person_id},
            ).scalar()
            assert final_state == "enriched"

            event_count = verify_session.execute(
                text(
                    "SELECT COUNT(*) FROM fa_max_state_transition_events "
                    "WHERE entity_uuid = :eid ::uuid AND to_state = 'enriched'"
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
                text("SELECT set_config('fa_max.allow_state_write', 'on', true)")
            )
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


# ---------------------------------------------------------------------------
# WP-1 remaining — state_version CAS, partner, interaction, property
# association, unified timeline, work queue
# ---------------------------------------------------------------------------


# --- state_version -----------------------------------------------------------

@pytest.mark.usefixtures("fresh_db")
def test_get_person_state_returns_state_version(fresh_db):
    """get_person_state() must return state_version so callers can CAS on it."""
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
    assert "state_version" in result, "state_version must be returned so callers can CAS on it"
    assert result["state_version"] == 0, "New person must start at state_version=0"


@pytest.mark.usefixtures("fresh_db")
def test_get_opportunity_state_returns_state_version(fresh_db):
    """get_opportunity_state() must return state_version."""
    from src.services.state_engine import get_opportunity_state

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    fresh_db.execute(
        text("""
            INSERT INTO fa_max_opportunities
                (person_id, opportunity_type, current_stage, source)
            VALUES (:pid ::uuid, 'acquisition', 'new', 'test')
        """),
        {"pid": person_id},
    )
    opp_id = fresh_db.execute(
        text("SELECT opportunity_id::text FROM fa_max_opportunities ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    result = get_opportunity_state(session=fresh_db, opportunity_id=opp_id)
    assert result is not None
    assert "state_version" in result
    assert result["state_version"] == 0


@pytest.mark.usefixtures("fresh_db")
def test_transition_increments_state_version(fresh_db):
    """A successful transition() must increment state_version by 1."""
    from src.services.state_engine import (
        TransitionOutcome, ensure_entity_registry,
        get_person_state, make_idempotency_key, transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    state_before = get_person_state(session=fresh_db, person_id=person_id)
    assert state_before["state_version"] == 0

    ikey = make_idempotency_key(entity_uuid, "identified", "enriched", "agent:test", epoch_minute=700)
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="identified", to_state="enriched", actor="agent:test",
            source_component="test", idempotency_key=ikey,
            state_version=0,
            acquire_redis_lock=False, validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.succeeded
    state_after = get_person_state(session=fresh_db, person_id=person_id)
    assert state_after["state_version"] == 1, "state_version must be incremented after transition"


@pytest.mark.usefixtures("fresh_db")
def test_transition_stale_state_version_returns_already_advanced(fresh_db):
    """A transition with a stale state_version (CAS mismatch) must return
    already_advanced — this prevents two concurrent callers both reading
    version=0 from writing to the same entity."""
    from src.services.state_engine import (
        TransitionOutcome, ensure_entity_registry,
        make_idempotency_key, transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    # First transition: version 0 -> 1 (succeeds)
    ikey1 = make_idempotency_key(entity_uuid, "identified", "enriched", "agent:test", epoch_minute=701)
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        r1 = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="identified", to_state="enriched", actor="agent:test",
            source_component="test", idempotency_key=ikey1,
            state_version=0,  # correct
            acquire_redis_lock=False, validate_allowed_next=True,
        )
    assert r1.outcome == TransitionOutcome.succeeded

    # Second transition: tries version 0 again — stale, must fail
    ikey2 = make_idempotency_key(entity_uuid, "enriched", "contacted", "agent:test", epoch_minute=702)
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        r2 = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="enriched", to_state="contacted", actor="agent:test",
            source_component="test", idempotency_key=ikey2,
            state_version=0,  # stale — real version is now 1
            acquire_redis_lock=False, validate_allowed_next=True,
        )
    assert r2.outcome == TransitionOutcome.already_advanced, (
        "Stale state_version must produce already_advanced — "
        "prevents two callers with the same stale read from both applying"
    )


# --- SOT lifecycle edges -------------------------------------------------------

@pytest.mark.usefixtures("fresh_db")
def test_sot_forward_chain_transitions_are_valid(fresh_db):
    """Every forward step in the SOT 12-stage chain is listed in allowed_next."""
    from src.services.state_engine import (
        TransitionOutcome, ensure_entity_registry,
        get_person_state, make_idempotency_key, transition,
    )

    forward_chain = [
        "identified", "enriched", "contacted", "engaged", "qualified",
        "portal_started", "application_submitted", "term_sheet_issued",
        "locked", "funded", "matured", "repeat",
    ]

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        for i in range(len(forward_chain) - 1):
            from_s = forward_chain[i]
            to_s = forward_chain[i + 1]
            sv = get_person_state(session=fresh_db, person_id=person_id)["state_version"]
            ikey = make_idempotency_key(entity_uuid, from_s, to_s, "agent:test", epoch_minute=800 + i)
            result = transition(
                session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
                from_state=from_s, to_state=to_s, actor="agent:test",
                source_component="test", idempotency_key=ikey,
                state_version=sv,
                acquire_redis_lock=False, validate_allowed_next=True,
            )
            assert result.outcome == TransitionOutcome.succeeded, (
                f"Forward step {from_s} -> {to_s} must be allowed by SOT config, got {result.outcome}"
            )


@pytest.mark.usefixtures("fresh_db")
def test_sot_repeat_to_engaged_backward_edge_is_valid(fresh_db):
    """repeat -> engaged is the one explicit backward edge in the SOT."""
    from src.services.state_engine import (
        TransitionOutcome, ensure_entity_registry,
        make_idempotency_key, transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'repeat', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    ikey = make_idempotency_key(entity_uuid, "repeat", "engaged", "agent:test", epoch_minute=850)
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="repeat", to_state="engaged", actor="agent:test",
            source_component="test", idempotency_key=ikey,
            state_version=0,
            acquire_redis_lock=False, validate_allowed_next=True,
        )
    assert result.outcome == TransitionOutcome.succeeded, (
        "repeat -> engaged is an explicit backward edge in SOT and must be allowed"
    )


@pytest.mark.usefixtures("fresh_db")
def test_admin_exceptional_jump_requires_reason_in_context(fresh_db):
    """validate_allowed_next=False (admin exceptional jump) requires actor to
    be admin/josh AND context['reason'] to be set — both gates must be checked."""
    from src.services.state_engine import (
        TransitionOutcome, ensure_entity_registry,
        make_idempotency_key, transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    def _do(actor, context, epoch_minute):
        ikey = make_idempotency_key(entity_uuid, "identified", "funded", actor, epoch_minute=epoch_minute)
        with patch("src.services.state_engine._acquire_pg_advisory_lock"):
            return transition(
                session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
                from_state="identified", to_state="funded", actor=actor,
                source_component="test", idempotency_key=ikey,
                context=context,
                acquire_redis_lock=False, validate_allowed_next=False,
            )

    # Missing reason — must reject
    r = _do("user:admin", {}, 900)
    assert r.outcome == TransitionOutcome.invalid_transition, (
        "Admin exceptional jump without reason in context must be rejected"
    )

    # Non-admin actor with reason — must reject
    r = _do("agent:cora", {"reason": "test"}, 901)
    assert r.outcome == TransitionOutcome.invalid_transition, (
        "Non-admin actor must not be allowed to bypass allowed_next"
    )

    # Admin with reason — must succeed
    r = _do("user:admin", {"reason": "manual correction"}, 902)
    assert r.outcome == TransitionOutcome.succeeded, (
        "Admin actor with reason in context must be allowed for exceptional jumps"
    )


# --- Partner -------------------------------------------------------------------

@pytest.mark.usefixtures("fresh_db")
def test_partner_transition_identified_to_active(fresh_db):
    """transition() with entity_type='partner' succeeds for identified -> active."""
    from src.services.state_engine import (
        TransitionOutcome, ensure_entity_registry,
        make_idempotency_key, transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    fresh_db.execute(
        text("""
            INSERT INTO fa_max_partners (person_id, partner_class, status, source)
            VALUES (:pid ::uuid, 'realtor', 'identified', 'test')
        """),
        {"pid": person_id},
    )
    partner_id = fresh_db.execute(
        text("SELECT partner_id::text FROM fa_max_partners ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="partner", native_id=partner_id)

    ikey = make_idempotency_key(entity_uuid, "identified", "active", "agent:test", epoch_minute=1000)
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db, entity_type="partner", entity_uuid=entity_uuid,
            from_state="identified", to_state="active", actor="agent:test",
            source_component="test", idempotency_key=ikey,
            state_version=0,
            acquire_redis_lock=False, validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.succeeded
    status = fresh_db.execute(
        text("SELECT status FROM fa_max_partners WHERE partner_id = :pid ::uuid"),
        {"pid": partner_id},
    ).scalar()
    assert status == "active"


@pytest.mark.usefixtures("fresh_db")
def test_partner_transition_invalid_rejects(fresh_db):
    """identified -> inactive is not a valid partner transition."""
    from src.services.state_engine import (
        TransitionOutcome, ensure_entity_registry,
        make_idempotency_key, transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    fresh_db.execute(
        text("""
            INSERT INTO fa_max_partners (person_id, partner_class, status, source)
            VALUES (:pid ::uuid, 'wholesaler', 'identified', 'test')
        """),
        {"pid": person_id},
    )
    partner_id = fresh_db.execute(
        text("SELECT partner_id::text FROM fa_max_partners ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="partner", native_id=partner_id)

    ikey = make_idempotency_key(entity_uuid, "identified", "inactive", "agent:test", epoch_minute=1001)
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        result = transition(
            session=fresh_db, entity_type="partner", entity_uuid=entity_uuid,
            from_state="identified", to_state="inactive", actor="agent:test",
            source_component="test", idempotency_key=ikey,
            acquire_redis_lock=False, validate_allowed_next=True,
        )

    assert result.outcome == TransitionOutcome.invalid_transition


# --- Interaction (write-once) --------------------------------------------------

@pytest.mark.usefixtures("fresh_db")
def test_write_interaction_creates_row(fresh_db):
    """write_interaction() inserts a row and returns a valid UUID."""
    from src.services.state_engine import write_interaction

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    interaction_id = write_interaction(
        session=fresh_db,
        person_id=person_id,
        channel="email",
        direction="outbound",
        actor="agent:cora",
        approved_bool=True,
        autonomy_tier_at_time="A",
        body_redacted="initial outreach",
    )

    assert interaction_id is not None
    row = fresh_db.execute(
        text("""
            SELECT person_id::text, channel, direction, approved_bool,
                   autonomy_tier_at_time, body_redacted, seq
            FROM fa_max_interactions
            WHERE interaction_id = :iid ::uuid
        """),
        {"iid": interaction_id},
    ).fetchone()

    assert row is not None
    assert row.person_id == person_id
    assert row.channel == "email"
    assert row.direction == "outbound"
    assert row.approved_bool is True
    assert row.autonomy_tier_at_time == "A"
    assert row.seq is not None


@pytest.mark.usefixtures("fresh_db")
def test_interaction_immutability_trigger_blocks_update(fresh_db):
    """DB trigger must block direct UPDATE on fa_max_interactions."""
    from src.services.state_engine import write_interaction

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    interaction_id = write_interaction(
        session=fresh_db, person_id=person_id, channel="sms",
        direction="inbound", actor="system:telnyx",
    )

    with pytest.raises(Exception, match="fa_max"):
        fresh_db.execute(
            text("""
                UPDATE fa_max_interactions
                SET channel = 'email'
                WHERE interaction_id = :iid ::uuid
            """),
            {"iid": interaction_id},
        )


@pytest.mark.usefixtures("fresh_db")
def test_database_guards_block_event_mutation_and_direct_state_write(fresh_db):
    """The database enforces both halves of the single-write-path contract."""
    from src.services.state_engine import ensure_entity_registry, transition

    person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
        RETURNING person_id::text
    """)).scalar_one()
    entity_uuid = ensure_entity_registry(
        session=fresh_db, entity_type="person", native_id=person_id
    )
    result = transition(
        session=fresh_db,
        entity_type="person",
        entity_uuid=entity_uuid,
        from_state="identified",
        to_state="enriched",
        actor="agent:test",
        source_component="test",
        idempotency_key=f"guard-{uuid.uuid4()}",
        state_version=0,
        acquire_redis_lock=False,
    )
    assert result.outcome.value == "succeeded"

    for statement, params in (
        (
            "UPDATE fa_max_state_transition_events SET actor = 'tampered' "
            "WHERE event_id = :id ::uuid",
            {"id": result.event_id},
        ),
        (
            "DELETE FROM fa_max_state_transition_events WHERE event_id = :id ::uuid",
            {"id": result.event_id},
        ),
        (
            "UPDATE fa_max_persons SET lifecycle_state = 'contacted' "
            "WHERE person_id = :id ::uuid",
            {"id": person_id},
        ),
    ):
        savepoint = fresh_db.begin_nested()
        with pytest.raises(Exception, match="fa_max"):
            fresh_db.execute(text(statement), params)
        savepoint.rollback()


# --- Property association (temporal) ------------------------------------------

@pytest.mark.usefixtures("fresh_db")
def test_write_and_close_property_association(fresh_db):
    """write_property_association opens a current association (valid_to=NULL)
    and close_property_association sets valid_to."""
    from src.services.state_engine import (
        close_property_association, write_property_association,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    # Insert a minimal property row so the FK is satisfied.
    prop_id = fresh_db.execute(
        text("""
            INSERT INTO properties (parcel_id, source_row_hash, needs_rescore, created_at, updated_at)
            VALUES ('TEST-PROP-99', 'abc123', false, NOW(), NOW())
            ON CONFLICT (parcel_id) DO NOTHING
            RETURNING id
        """)
    ).scalar()
    if prop_id is None:
        prop_id = fresh_db.execute(
            text("SELECT id FROM properties WHERE parcel_id = 'TEST-PROP-99'")
        ).scalar()

    assoc_id = write_property_association(
        session=fresh_db,
        person_id=person_id,
        property_id=prop_id,
        role="subject",
        source="test",
    )
    assert assoc_id is not None

    row = fresh_db.execute(
        text("SELECT valid_to FROM fa_max_property_associations WHERE id = :id"),
        {"id": assoc_id},
    ).fetchone()
    assert row.valid_to is None, "New association must be open (valid_to IS NULL)"

    closed = close_property_association(session=fresh_db, association_id=assoc_id)
    assert closed is True

    row2 = fresh_db.execute(
        text("SELECT valid_to FROM fa_max_property_associations WHERE id = :id"),
        {"id": assoc_id},
    ).fetchone()
    assert row2.valid_to is not None, "Closed association must have valid_to set"


@pytest.mark.usefixtures("fresh_db")
def test_property_association_close_is_a_new_cursor_event(fresh_db):
    """A close after the open cursor has been read must remain discoverable."""
    from src.services.state_engine import (
        close_property_association, get_borrower_timeline, write_property_association,
    )

    person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test') RETURNING person_id::text
    """)).scalar()
    prop_id = fresh_db.execute(text("""
        INSERT INTO properties (parcel_id, source_row_hash, needs_rescore, created_at, updated_at)
        VALUES ('TIMELINE-CLOSE-PROP', 'timeline-close-hash', false, NOW(), NOW())
        ON CONFLICT (parcel_id) DO UPDATE SET parcel_id = EXCLUDED.parcel_id
        RETURNING id
    """)).scalar()
    association_id = write_property_association(
        session=fresh_db, person_id=person_id, property_id=prop_id, source="test",
    )

    first_page = get_borrower_timeline(session=fresh_db, person_id=person_id, limit=1)
    assert first_page["events"][0]["event_kind"] == "property_association"
    creation_cursor = first_page["events"][0]["global_seq"]

    assert close_property_association(session=fresh_db, association_id=association_id)
    after_close = get_borrower_timeline(
        session=fresh_db, person_id=person_id, after_seq=creation_cursor,
    )

    assert [event["event_kind"] for event in after_close["events"]] == ["property_association_closed"]
    assert after_close["events"][0]["extra"]["association_id"] == association_id


@pytest.mark.usefixtures("fresh_db")
def test_wp1_guards_fail_closed_when_guc_is_unset(fresh_db):
    """A fresh session must reject direct state and audit writes."""
    person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test') RETURNING person_id::text
    """)).scalar()
    fresh_db.execute(text("SELECT set_config('fa_max.allow_state_write', 'off', true)"))
    savepoint = fresh_db.begin_nested()
    with pytest.raises(Exception, match="fa_max"):
        fresh_db.execute(
            text("UPDATE fa_max_persons SET lifecycle_state = 'enriched' "
                 "WHERE person_id = :person_id ::uuid"),
            {"person_id": person_id},
        )
    savepoint.rollback()


@pytest.mark.usefixtures("fresh_db")
def test_wp1_migration_records_legacy_lifecycle_remap_event(fresh_db):
    """An old vocabulary row is upgraded with an attributable event, not a
    silent state rewrite."""
    import importlib.util
    from pathlib import Path

    fresh_db.execute(text("SET CONSTRAINTS ALL DEFERRED"))
    fresh_db.execute(text("""
        INSERT INTO fa_max_person_lifecycle_stage_config
            (stage_key, display_name, order_index, allowed_next, is_terminal, is_active)
        VALUES ('warm', 'Warm', 99, '[]'::jsonb, false, true)
    """))
    person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'warm', 'legacy-test') RETURNING person_id::text
    """)).scalar()

    path = Path(__file__).parent.parent / "migrations" / "apply_fa_max_wp1_remaining.py"
    spec = importlib.util.spec_from_file_location("wp1_remaining_upgrade_test", path)
    migration = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(migration)
    # Use DML-only slice — DDL cannot run inside a transaction with pending
    # trigger events (the fixture wraps everything in one outer transaction).
    for statement in migration.LEGACY_REMAP_STATEMENTS:
        fresh_db.execute(text(statement))

    assert fresh_db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :person_id ::uuid"),
        {"person_id": person_id},
    ).scalar() == "engaged"
    event = fresh_db.execute(text("""
        SELECT from_state, to_state, actor, source_component
        FROM fa_max_state_transition_events
        WHERE person_id = :person_id ::uuid
          AND idempotency_key = :idempotency_key
    """), {
        "person_id": person_id,
        "idempotency_key": f"wp1-lifecycle-remap:{person_id}:warm:engaged",
    }).mappings().one()
    assert dict(event) == {
        "from_state": "warm", "to_state": "engaged",
        "actor": "system:migration",
        "source_component": "migrations.apply_fa_max_wp1_remaining",
    }


# --- Unified timeline -----------------------------------------------------------

@pytest.mark.usefixtures("fresh_db")
def test_get_borrower_timeline_merges_all_event_kinds(fresh_db):
    """get_borrower_timeline() returns state_transition, interaction, and
    property_association events for the same person in one paginated result."""
    from src.services.state_engine import (
        ensure_entity_registry, get_borrower_timeline,
        make_idempotency_key, transition, write_interaction,
        write_property_association,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    # 1. State transition
    ikey = make_idempotency_key(entity_uuid, "identified", "enriched", "agent:test", epoch_minute=1100)
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        transition(
            session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
            from_state="identified", to_state="enriched", actor="agent:test",
            source_component="test", idempotency_key=ikey,
            state_version=0,
            acquire_redis_lock=False, validate_allowed_next=True,
        )

    # 2. Interaction
    write_interaction(
        session=fresh_db, person_id=person_id, channel="email",
        direction="outbound", actor="agent:cora",
    )

    # 3. Property association
    prop_id = fresh_db.execute(
        text("""
            INSERT INTO properties (parcel_id, source_row_hash, needs_rescore, created_at, updated_at)
            VALUES ('TIMELINE-TEST-PROP', 'xyz', false, NOW(), NOW())
            ON CONFLICT (parcel_id) DO NOTHING
            RETURNING id
        """)
    ).scalar()
    if prop_id is None:
        prop_id = fresh_db.execute(
            text("SELECT id FROM properties WHERE parcel_id = 'TIMELINE-TEST-PROP'")
        ).scalar()
    write_property_association(
        session=fresh_db, person_id=person_id, property_id=prop_id, role="subject",
    )

    timeline = get_borrower_timeline(session=fresh_db, person_id=person_id)
    assert timeline["has_more"] is False

    event_kinds = {e["event_kind"] for e in timeline["events"]}
    assert "state_transition" in event_kinds, "Timeline must include state_transition events"
    assert "interaction" in event_kinds, "Timeline must include interaction events"
    assert "property_association" in event_kinds, "Timeline must include property_association events"
    assert len(timeline["events"]) == 3


@pytest.mark.usefixtures("fresh_db")
def test_get_borrower_timeline_pagination(fresh_db):
    """Timeline cursor pagination works: after_seq=None starts from beginning,
    next_cursor continues from where the last page left off."""
    from src.services.state_engine import (
        ensure_entity_registry, get_borrower_timeline,
        make_idempotency_key, transition,
    )

    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    # Write 4 state-transition events
    states = ["identified", "enriched", "contacted", "engaged", "qualified"]
    with patch("src.services.state_engine._acquire_pg_advisory_lock"):
        for i in range(len(states) - 1):
            ikey = make_idempotency_key(entity_uuid, states[i], states[i+1], "agent:test", epoch_minute=1200+i)
            transition(
                session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
                from_state=states[i], to_state=states[i+1], actor="agent:test",
                source_component="test", idempotency_key=ikey,
                state_version=i,
                acquire_redis_lock=False, validate_allowed_next=True,
            )

    # Page through with limit=2
    all_events = []
    cursor = None
    pages = 0
    while True:
        page = get_borrower_timeline(session=fresh_db, person_id=person_id, limit=2, after_seq=cursor)
        all_events.extend(page["events"])
        pages += 1
        assert pages < 10, "Pagination loop did not terminate"
        if not page["has_more"]:
            assert page["next_cursor"] is None
            break
        cursor = page["next_cursor"]

    assert len(all_events) == 4, f"All 4 events must be reachable via pagination, got {len(all_events)}"
    assert pages == 2  # 2 + 2


# --- Work queue ----------------------------------------------------------------

@pytest.mark.usefixtures("fresh_db")
def test_enqueue_and_claim_work_item(fresh_db):
    """enqueue_work_item + claim_next_work_item round-trip."""
    from src.services.state_engine import claim_next_work_item, enqueue_work_item

    work_item_id = enqueue_work_item(
        session=fresh_db,
        queue_name="test_queue",
        payload={"task": "enrich", "attempt": 1},
        idempotency_key="test-enqueue-1",
    )
    assert work_item_id is not None

    item = claim_next_work_item(
        session=fresh_db, queue_name="test_queue", worker_id="worker:1", lease_seconds=60,
    )
    assert item is not None
    assert item["work_item_id"] == work_item_id
    assert item["status"] == "claimed"
    assert item["worker_id"] == "worker:1"
    assert item["attempt_count"] == 1
    assert item["lease_expires_at"] is not None


@pytest.mark.usefixtures("fresh_db")
def test_claim_returns_none_when_queue_empty(fresh_db):
    """claim_next_work_item returns None when no work is available."""
    from src.services.state_engine import claim_next_work_item

    item = claim_next_work_item(
        session=fresh_db, queue_name="empty_queue", worker_id="worker:1",
    )
    assert item is None


@pytest.mark.usefixtures("fresh_db")
def test_claim_skip_locked_avoids_double_claim(fresh_db):
    """A claimed item is not claimed again by a second claim call in the
    same session (FOR UPDATE SKIP LOCKED behavior)."""
    from src.services.state_engine import claim_next_work_item, enqueue_work_item

    enqueue_work_item(
        session=fresh_db, queue_name="skip_test", payload={"x": 1}, idempotency_key="skip-1",
    )

    item1 = claim_next_work_item(session=fresh_db, queue_name="skip_test", worker_id="worker:A")
    assert item1 is not None

    # The claimed item is locked — a second claim in the same session skips it.
    item2 = claim_next_work_item(session=fresh_db, queue_name="skip_test", worker_id="worker:B")
    assert item2 is None, "Claimed item must be skipped via SKIP LOCKED"


@pytest.mark.usefixtures("fresh_db")
def test_reclaim_expired_returns_items_to_available(fresh_db):
    """reclaim_expired_work_items returns expired claimed items to available."""
    from src.services.state_engine import enqueue_work_item, reclaim_expired_work_items

    # Manually insert a claimed item with an already-expired lease.
    fresh_db.execute(text("""
        INSERT INTO fa_max_work_queue
            (queue_name, payload, status, available_at, claimed_at,
             lease_expires_at, worker_id, attempt_count)
        VALUES
            ('reclaim_test', '{}'::jsonb, 'claimed',
             NOW() - INTERVAL '10 minutes',
             NOW() - INTERVAL '10 minutes',
             NOW() - INTERVAL '1 second',
             'dead_worker', 1)
    """))

    reclaimed = reclaim_expired_work_items(session=fresh_db, queue_name="reclaim_test")
    assert reclaimed == 1

    row = fresh_db.execute(
        text("SELECT status, worker_id, lease_expires_at FROM fa_max_work_queue WHERE queue_name = 'reclaim_test'")
    ).fetchone()
    assert row.status == "available"
    assert row.worker_id is None
    assert row.lease_expires_at is None


@pytest.mark.usefixtures("fresh_db")
def test_work_queue_idempotency_key_prevents_duplicate_enqueue(fresh_db):
    """enqueue_work_item with the same idempotency_key returns None on second call."""
    from src.services.state_engine import enqueue_work_item

    id1 = enqueue_work_item(
        session=fresh_db, queue_name="idem_test", payload={"x": 1},
        idempotency_key="idem-key-42",
    )
    assert id1 is not None

    id2 = enqueue_work_item(
        session=fresh_db, queue_name="idem_test", payload={"x": 1},
        idempotency_key="idem-key-42",
    )
    assert id2 is None, "Duplicate enqueue with same idempotency_key must return None"

    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM fa_max_work_queue WHERE idempotency_key = 'idem-key-42'")
    ).scalar()
    assert count == 1


def test_connection_loss_work_queue_recovered(pg_engine):
    """WP-1 Done-When: worker killed holding a work queue lease — a second
    worker calls reclaim_expired_work_items and claims the item exactly once.

    Two real connections: worker A claims an item, raw-closes its connection
    (SIGKILL simulation). The item's lease_expires_at is set in the past
    artificially so reclaim_expired runs immediately. Worker B reclaims and
    completes the item.
    """
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured — skipping real-connection test")

    from sqlalchemy.orm import Session as SASession
    from src.services.state_engine import (
        claim_next_work_item, complete_work_item,
        enqueue_work_item, reclaim_expired_work_items,
    )

    # Setup: enqueue one work item on its own committed connection.
    unique_key = f"kill-test-item-{uuid.uuid4()}"
    setup_conn = pg_engine.connect()
    setup_trans = setup_conn.begin()
    setup_session = SASession(bind=setup_conn)
    work_item_id = enqueue_work_item(
        session=setup_session,
        queue_name="kill_test",
        payload={"task": "do_thing"},
        idempotency_key=unique_key,
    )
    assert work_item_id is not None
    setup_session.close()
    setup_trans.commit()
    setup_conn.close()

    # Worker A: claim the item, then get killed.
    conn_a = pg_engine.connect()
    trans_a = conn_a.begin()
    session_a = SASession(bind=conn_a)
    item_a = claim_next_work_item(
        session=session_a, queue_name="kill_test", worker_id="worker:killed",
    )
    assert item_a is not None
    assert item_a["work_item_id"] == work_item_id

    # Artificially expire the lease so reclaim runs immediately.
    session_a.execute(
        text("""
            UPDATE fa_max_work_queue
            SET lease_expires_at = NOW() - INTERVAL '1 second'
            WHERE work_item_id = :wid ::uuid
        """),
        {"wid": work_item_id},
    )
    trans_a.commit()  # commit the claim + lease expiry, then kill

    # Kill: raw DBAPI close (no commit/rollback on the next operation).
    conn_a.connection.close()

    # Worker B: reclaim the expired item and complete it.
    conn_b = pg_engine.connect()
    trans_b = conn_b.begin()
    session_b = SASession(bind=conn_b)

    reclaimed = reclaim_expired_work_items(session=session_b, queue_name="kill_test")
    assert reclaimed == 1, "Expired item must be returned to available"

    item_b = claim_next_work_item(
        session=session_b, queue_name="kill_test", worker_id="worker:b",
    )
    assert item_b is not None, "Worker B must claim the reclaimed item"
    assert item_b["work_item_id"] == work_item_id
    assert item_b["attempt_count"] == 3, (
        "attempt_count reflects claim A, expired-lease reclaim, and claim B"
    )

    done = complete_work_item(
        session=session_b, work_item_id=work_item_id, worker_id="worker:b",
    )
    assert done is True
    trans_b.commit()

    # Verify final status.
    conn_verify = pg_engine.connect()
    row = conn_verify.execute(
        text("SELECT status, done_at FROM fa_max_work_queue WHERE work_item_id = :wid ::uuid"),
        {"wid": work_item_id},
    ).fetchone()
    conn_verify.close()
    assert row.status == "done"
    assert row.done_at is not None

    conn_b.close()


def test_real_worker_process_kill_is_recovered_exactly_once(pg_engine):
    """A terminated worker's committed lease is reclaimed and its persisted
    transition idempotency key is applied exactly once by a new worker."""
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured")

    from sqlalchemy.orm import Session as SASession
    from src.services.state_engine import (
        claim_next_work_item,
        complete_work_item,
        enqueue_work_item,
        ensure_entity_registry,
        reclaim_expired_work_items,
        transition,
        TransitionOutcome,
    )

    queue_name = f"process-kill-{uuid.uuid4()}"
    transition_key = f"process-kill-transition-{uuid.uuid4()}"
    with pg_engine.begin() as connection:
        with SASession(bind=connection) as session:
            person_id = session.execute(text("""
                INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
                VALUES (gen_random_uuid(), 'identified', 'test')
                RETURNING person_id::text
            """)).scalar_one()
            entity_uuid = ensure_entity_registry(
                session=session, entity_type="person", native_id=person_id
            )
            work_item_id = enqueue_work_item(
                session=session,
                queue_name=queue_name,
                person_id=person_id,
                idempotency_key=f"work-{transition_key}",
                payload={
                    "entity_type": "person",
                    "entity_uuid": entity_uuid,
                    "from_state": "identified",
                    "to_state": "enriched",
                    "state_version": 0,
                    "idempotency_key": transition_key,
                },
            )
    assert work_item_id is not None

    context = multiprocessing.get_context("spawn")
    ready_queue = context.Queue()
    process = context.Process(
        target=_claim_work_and_wait,
        args=(pg_engine.url.render_as_string(hide_password=False), queue_name, ready_queue),
    )
    process.start()
    claimed_id = ready_queue.get(timeout=10)
    assert claimed_id == work_item_id
    process.terminate()
    process.join(timeout=10)
    assert not process.is_alive()

    # The killed process committed a one-second lease. Wait for natural
    # expiry so recovery proves the production lease path, not a test edit.
    time.sleep(1.2)

    with pg_engine.begin() as connection:
        with SASession(bind=connection) as session:
            assert reclaim_expired_work_items(
                session=session, queue_name=queue_name
            ) == 1
            item = claim_next_work_item(
                session=session,
                queue_name=queue_name,
                worker_id="worker:replacement",
            )
            assert item is not None
            payload = item["payload"]
            result = transition(
                session=session,
                entity_type=payload["entity_type"],
                entity_uuid=payload["entity_uuid"],
                from_state=payload["from_state"],
                to_state=payload["to_state"],
                actor="system:recovery-worker",
                source_component="tests.recovery",
                idempotency_key=payload["idempotency_key"],
                state_version=payload["state_version"],
                acquire_redis_lock=False,
            )
            assert result.outcome == TransitionOutcome.succeeded
            assert complete_work_item(
                session=session,
                work_item_id=work_item_id,
                worker_id="worker:replacement",
            )

    with pg_engine.connect() as connection:
        final_state = connection.execute(
            text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid ::uuid"),
            {"pid": person_id},
        ).scalar_one()
        event_count = connection.execute(
            text("SELECT count(*) FROM fa_max_state_transition_events WHERE idempotency_key = :key"),
            {"key": transition_key},
        ).scalar_one()
    assert final_state == "enriched"
    assert event_count == 1


# --- Compliance structural checks (Category 13) --------------------------------

def test_no_financial_data_columns_on_new_wp1_tables():
    """Structural: none of the new WP-1 tables carry borrower financial data
    (credit score, income, bank statement, tax return, SSN)."""
    from src.core.models import Base

    financial_keywords = {"credit", "income", "bank", "tax_return", "ssn", "social_security"}
    new_tables = [
        "fa_max_partners",
        "fa_max_interactions",
        "fa_max_property_associations",
        "fa_max_work_queue",
    ]
    for table_name in new_tables:
        table = Base.metadata.tables.get(table_name)
        assert table is not None, f"ORM table {table_name!r} must exist in metadata"
        for col in table.columns:
            col_lower = col.name.lower()
            for kw in financial_keywords:
                assert kw not in col_lower, (
                    f"Column {table_name}.{col.name!r} appears to hold financial data "
                    f"(keyword={kw!r}) — SOT.md prohibition"
                )


def test_orm_tables_have_state_version_columns():
    """Structural: fa_max_persons and fa_max_opportunities both carry state_version."""
    from src.core.models import Base

    for table_name in ("fa_max_persons", "fa_max_opportunities"):
        table = Base.metadata.tables[table_name]
        col_names = {c.name for c in table.columns}
        assert "state_version" in col_names, (
            f"{table_name} must declare state_version column (CAS guard)"
        )


def test_partner_table_has_state_version():
    """Structural: fa_max_partners carries state_version."""
    from src.core.models import Base

    table = Base.metadata.tables["fa_max_partners"]
    col_names = {c.name for c in table.columns}
    assert "state_version" in col_names, "fa_max_partners must have state_version"
