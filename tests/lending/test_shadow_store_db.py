"""R1 0.9 shadow store and card-reader SQL against Postgres, inside one rolled-back transaction.

Skipped by default: the shared DB is prod and these lock live tables. Run by hand once the migration is applied:
    PYTHONPATH=. pytest tests/lending/test_shadow_store_db.py -v -p no:cacheprovider --run-shadow-db
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.lending.shadow import store as shadow_store
from src.lending.shadow.cards import SqlCardReader

pytestmark = pytest.mark.skipif(os.environ.get("SHADOW_DB_TESTS") != "1",
                                reason="shadow DB tests lock live tables; run by hand with SHADOW_DB_TESTS=1")

NOW = datetime.now(timezone.utc)


@pytest.fixture(scope="module")
def engine():
    return create_engine(get_settings().database_url, pool_pre_ping=True)


@pytest.fixture
def db(engine):
    conn = engine.connect()
    trans = conn.begin()
    session = Session(bind=conn)
    yield session
    session.close()
    trans.rollback()
    conn.close()


def _decision(db, class_name="db-test-class", ref="action:-1", proposed="approve"):
    return shadow_store.insert_decision(db, class_name=class_name, class_version=1, subject_kind="pending_action",
                                        subject_ref=ref, inputs={"template_version": "T-1"}, rule_version=None,
                                        confidence=0.5, proposed={"decision": proposed})


class TestModes:
    def test_start_records_who_and_when(self, db):
        shadow_store.set_mode(db, "db-test-class", "shadow", changed_by="pytest")
        row = db.execute(text("SELECT mode, started_at, ended_at, changed_by FROM lending.shadow_classes "
                              "WHERE class_name = 'db-test-class'")).mappings().one()
        assert (row["mode"], row["changed_by"], row["ended_at"]) == ("shadow", "pytest", None)
        assert row["started_at"] is not None

    def test_stop_sets_ended_at(self, db):
        shadow_store.set_mode(db, "db-test-class", "shadow", changed_by="pytest")
        shadow_store.set_mode(db, "db-test-class", "off", changed_by="pytest")
        assert shadow_store.get_mode(db, "db-test-class") == "off"

    def test_live_refused(self, db):
        with pytest.raises(ValueError):
            shadow_store.set_mode(db, "db-test-class", "live", changed_by="pytest")


class TestDecisions:
    def test_insert_is_idempotent(self, db):
        assert _decision(db) is not None
        assert _decision(db) is None

    def test_update_match_reports_only_changes_and_only_its_class(self, db):
        _decision(db, "db-a")
        _decision(db, "db-b")
        fields = dict(actual="approve", decision_match=True, decided_at=NOW, decision_seconds=60.0,
                      edit_distance=None, send_result="sent")
        assert shadow_store.update_match(db, "db-a", "action:-1", **fields) == 1
        assert shadow_store.update_match(db, "db-a", "action:-1", **fields) == 0
        assert shadow_store.open_rows(db, "db-b", NOW - timedelta(days=90))[0]["actual"] is None

    def test_settled_row_leaves_open_rows(self, db):
        _decision(db)
        shadow_store.update_match(db, "db-test-class", "action:-1", actual="approve", decision_match=True,
                                  decided_at=NOW, decision_seconds=1.0, edit_distance=None, send_result="sent")
        assert shadow_store.open_rows(db, "db-test-class", NOW - timedelta(days=90))
        shadow_store.set_outcome(db, "db-test-class", "action:-1", "funded", NOW)
        assert shadow_store.open_rows(db, "db-test-class", NOW - timedelta(days=90)) == []

    def test_funded_deal_before_the_send_keeps_the_row_open(self, db):
        _decision(db)
        shadow_store.update_match(db, "db-test-class", "action:-1", actual="approve", decision_match=True,
                                  decided_at=NOW, decision_seconds=1.0, edit_distance=None, send_result=None)
        shadow_store.set_outcome(db, "db-test-class", "action:-1", "funded", NOW)
        assert len(shadow_store.open_rows(db, "db-test-class", NOW - timedelta(days=90))) == 1

    def test_declined_can_become_funded_but_funded_never_changes(self, db):
        _decision(db)
        assert shadow_store.set_outcome(db, "db-test-class", "action:-1", "declined", NOW) == 1
        assert shadow_store.set_outcome(db, "db-test-class", "action:-1", "funded", NOW) == 1
        assert shadow_store.set_outcome(db, "db-test-class", "action:-1", None, None) == 0

    def test_rows_for_digest_runs_and_drops_the_phone(self, db):
        _decision(db)
        rows = shadow_store.rows_for_digest(db, "db-test-class", NOW - timedelta(days=1))
        assert len(rows) == 1
        assert "recipient_phone" not in rows[0] and "last4" in rows[0]

    def test_shadow_sent_count_is_zero(self, db):
        assert shadow_store.shadow_sent_count(db) == 0


class TestCardReaderSql:
    def test_queries_run(self, db):
        reader = SqlCardReader()
        cards = reader.new_cards(db, NOW - timedelta(days=30))
        if cards:
            reader.features(db, cards[0])
            assert reader.cards(db, [cards[0]["action_id"]])
        assert reader.deal_outcome(db, "loan_file:-1") is None
