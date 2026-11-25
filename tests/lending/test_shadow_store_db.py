"""R1 0.9 DB tests for the shadow store and migration.

NOT run automatically — the shared DB is single-instance prod (see CLAUDE.md).
Run manually:
    PYTHONPATH=. pytest tests/lending/test_shadow_store_db.py -v

Requires the migration to have been applied:
    PYTHONPATH=. python migrations/apply_lending_shadow.py
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.skip(reason=(
    "Shadow DB tests: run manually after applying migrations/apply_lending_shadow.py. "
    "Not run in CI — the shared DB is single-instance prod."
))

from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.lending.shadow import store as shadow_store

UTC = timezone.utc


@pytest.fixture(scope="module")
def engine():
    url = get_settings().database_url
    if not url:
        pytest.skip("DATABASE_URL not set")
    return create_engine(url, pool_pre_ping=True)


@pytest.fixture
def db(engine):
    """A session whose work is rolled back so nothing persists."""
    conn = engine.connect()
    trans = conn.begin()
    session = Session(bind=conn)
    yield session
    session.close()
    trans.rollback()
    conn.close()


class TestShadowClassModeRoundtrip:
    def test_set_and_get_mode(self, db):
        shadow_store.set_mode(db, "test-class-db", "shadow", changed_by="pytest")
        assert shadow_store.get_mode(db, "test-class-db") == "shadow"

    def test_started_at_set_when_mode_shadow(self, db):
        shadow_store.set_mode(db, "test-class-db-2", "shadow")
        started = shadow_store.get_started_at(db, "test-class-db-2")
        assert started is not None

    def test_ended_at_set_when_mode_off(self, db):
        shadow_store.set_mode(db, "test-class-db-3", "shadow")
        shadow_store.set_mode(db, "test-class-db-3", "off")
        row = db.execute(
            text("SELECT ended_at FROM lending.shadow_classes WHERE class_name = 'test-class-db-3'")
        ).mappings().first()
        assert row is not None
        assert row["ended_at"] is not None

    def test_get_mode_returns_none_for_unknown(self, db):
        assert shadow_store.get_mode(db, "not-registered") is None

    def test_invalid_mode_raises(self, db):
        with pytest.raises(ValueError):
            shadow_store.set_mode(db, "test-class-db-4", "invalid")


class TestShadowDecisionInsert:
    def test_insert_returns_id(self, db):
        shadow_store.set_mode(db, "test-insert-class", "shadow")
        row_id = shadow_store.insert_decision(
            db,
            class_name="test-insert-class",
            class_version=1,
            subject_kind="pending_action",
            subject_ref="action:999",
            inputs={"template_version": "LF-D2-SMS-v1"},
            rule_version=None,
            confidence=0.8,
            proposed={"decision": "approve"},
        )
        assert row_id is not None

    def test_duplicate_insert_returns_none(self, db):
        shadow_store.set_mode(db, "test-dup-class", "shadow")
        shadow_store.insert_decision(
            db, class_name="test-dup-class", class_version=1,
            subject_kind="pending_action", subject_ref="action:1001",
            inputs={}, rule_version=None, confidence=None,
            proposed={"decision": "approve"},
        )
        second = shadow_store.insert_decision(
            db, class_name="test-dup-class", class_version=1,
            subject_kind="pending_action", subject_ref="action:1001",
            inputs={}, rule_version=None, confidence=None,
            proposed={"decision": "approve"},
        )
        assert second is None

    def test_inputs_stored_as_jsonb(self, db):
        shadow_store.set_mode(db, "test-jsonb-class", "shadow")
        row_id = shadow_store.insert_decision(
            db, class_name="test-jsonb-class", class_version=1,
            subject_kind="pending_action", subject_ref="action:2001",
            inputs={"template_version": "LF-D2-SMS-v1", "past_total": 10},
            rule_version=None, confidence=None, proposed={"decision": "edit"},
        )
        assert row_id is not None
        row = db.execute(
            text("SELECT inputs FROM lending.shadow_decisions WHERE id = :id"), {"id": row_id}
        ).mappings().first()
        assert row["inputs"]["template_version"] == "LF-D2-SMS-v1"


class TestFillActual:
    def test_fill_actual_updates_row(self, db):
        shadow_store.set_mode(db, "test-fill-class", "shadow")
        shadow_store.insert_decision(
            db, class_name="test-fill-class", class_version=1,
            subject_kind="pending_action", subject_ref="action:3001",
            inputs={}, rule_version=None, confidence=None, proposed={"decision": "approve"},
        )
        updated = shadow_store.fill_actual(
            db, "action:3001",
            actual="edit",
            decided_at=datetime.now(UTC),
            decision_seconds=3600.0,
            edit_distance=0.15,
            send_result="sent",
        )
        assert updated == 1

    def test_fill_actual_idempotent_second_call_updates_zero(self, db):
        shadow_store.set_mode(db, "test-fill-idem", "shadow")
        shadow_store.insert_decision(
            db, class_name="test-fill-idem", class_version=1,
            subject_kind="pending_action", subject_ref="action:4001",
            inputs={}, rule_version=None, confidence=None, proposed={"decision": "approve"},
        )
        shadow_store.fill_actual(db, "action:4001", actual="approve",
                                 decided_at=datetime.now(UTC), decision_seconds=None,
                                 edit_distance=None, send_result="sent")
        second = shadow_store.fill_actual(db, "action:4001", actual="edit",
                                          decided_at=datetime.now(UTC), decision_seconds=None,
                                          edit_distance=None, send_result="sent")
        assert second == 0  # already filled, nothing to update


class TestShadowSentCount:
    def test_zero_sent_when_no_shadow_requests(self, db):
        count = shadow_store.shadow_sent_count(db)
        assert count == 0
