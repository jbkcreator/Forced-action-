"""R1 0.7 against Postgres: the snapshot trigger, append-only history and the rollback ("Done when").

Runs in a throwaway schema inside one rolled-back transaction (every ``lending.`` in a statement is rewritten to it),
so no live table is touched.  Skips unless TEST_DATABASE_URL points at a throwaway Postgres.
"""
from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from migrations.apply_lending_lender_rule_snapshots import apply_to
from src.lending import lender_rules_store as store
from src.lending.models import LENDING_SCHEMA, LendingBase, LendingCoraInboxItem, LendingLenderRuleValue

LENDER = "central_lending"


@pytest.fixture
def rules_db():
    from config.settings import get_settings

    url = get_settings().database_url
    if not url:
        pytest.skip("requires a throwaway Postgres (TEST_DATABASE_URL)")
    engine = create_engine(str(url), connect_args={"connect_timeout": 20})
    try:
        conn = engine.connect()
    except Exception:
        engine.dispose()
        pytest.skip("Postgres is not reachable")
    schema = f"r107_{uuid.uuid4().hex[:10]}"
    tx = conn.begin()
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text("SET LOCAL statement_timeout = '30s'"))
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

    @event.listens_for(conn, "before_cursor_execute", retval=True)
    def _to_schema(_conn, _cursor, statement, parameters, _context, _many):
        return statement.replace(f"{LENDING_SCHEMA}.", f"{schema}."), parameters

    LendingBase.metadata.create_all(conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema}),
                                    tables=[LendingLenderRuleValue.__table__, LendingCoraInboxItem.__table__])
    apply_to(conn, schema)
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    session.info["schema"] = schema
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _value(db, field="max_ltv"):
    return db.execute(text("SELECT value FROM lending.lender_rule_values WHERE lender_key = :l AND field = :f"),
                      {"l": LENDER, "f": field}).scalar_one_or_none()


def _versions(db):
    return db.execute(text("SELECT version_no FROM lending.lender_rule_snapshots ORDER BY version_no")).scalars().all()


def _snapshot_rows(db):
    return db.execute(text("SELECT version_no, rules::text, created_by, reason FROM lending.lender_rule_snapshots "
                           "ORDER BY version_no")).all()


def _set(db, value, field="max_ltv", actor="josh"):
    return store.set_rule_value(db, LENDER, field, value, status="confirmed", actor=actor, reason="test edit")


def test_the_migration_writes_version_one_and_is_rerunnable(rules_db):
    assert _versions(rules_db) == [1]
    apply_to(rules_db.connection(), rules_db.info["schema"])
    assert _versions(rules_db) == [1]


def test_a_hand_edited_sql_change_is_versioned(rules_db):
    rules_db.execute(text("INSERT INTO lending.lender_rule_values (lender_key, field, value, status) "
                          "VALUES ('central_lending', 'max_ltv', '0.9'::jsonb, 'confirmed')"))
    assert _versions(rules_db) == [1, 2]
    row = rules_db.execute(text("SELECT created_by, reason FROM lending.lender_rule_snapshots WHERE version_no = 2")).one()
    assert row.reason == "table edit" and row.created_by


def test_who_and_why_come_from_the_session_settings(rules_db):
    rules_db.execute(text("SELECT set_config('lending.rule_actor', 'josh', true), set_config('lending.rule_reason', 'confirmed on call', true)"))
    rules_db.execute(text("INSERT INTO lending.lender_rule_values (lender_key, field, value, status) "
                          "VALUES ('central_lending', 'max_ltv', '0.9'::jsonb, 'confirmed')"))
    row = rules_db.execute(text("SELECT created_by, reason FROM lending.lender_rule_snapshots WHERE version_no = 2")).one()
    assert tuple(row) == ("josh", "confirmed on call")


def test_an_edit_that_changes_nothing_writes_no_version(rules_db):
    _set(rules_db, 0.9)
    before = _versions(rules_db)
    rules_db.execute(text("INSERT INTO lending.lender_rule_values (lender_key, field, value, status) "
                          "VALUES ('central_lending', 'max_ltv', '0.9'::jsonb, 'confirmed') ON CONFLICT DO NOTHING"))
    assert _versions(rules_db) == before


@pytest.mark.parametrize("statement", ["UPDATE lending.lender_rule_snapshots SET reason = 'x'",
                                       "DELETE FROM lending.lender_rule_snapshots"])
def test_snapshots_are_append_only(rules_db, statement):
    with pytest.raises(DBAPIError, match="append-only"):
        with rules_db.begin_nested():
            rules_db.execute(text(statement))


def test_a_valid_write_returns_the_new_version_and_an_invalid_one_writes_nothing(rules_db):
    assert _set(rules_db, 0.9) == 2
    before = _versions(rules_db)
    with pytest.raises(store.RuleValueError):
        _set(rules_db, 9.0)
    assert _versions(rules_db) == before and _value(rules_db) == 0.9


def test_done_when_a_wrong_leverage_is_rolled_back_and_history_is_kept(rules_db):
    _set(rules_db, 0.9)                                  # v2: the good rule
    _set(rules_db, 0.99)                                 # v3: valid but wrong (quotes come out too generous)
    assert _value(rules_db) == 0.99
    history = _snapshot_rows(rules_db)

    result = store.rollback_to(rules_db, 2, actor="josh")

    assert (result.from_version, result.to_version, result.new_version, result.changed) == (3, 2, 4, True)
    assert _value(rules_db) == 0.9                       # the rule is back
    assert _versions(rules_db) == [1, 2, 3, 4]           # nothing rewritten, the rollback is its own version
    assert _snapshot_rows(rules_db)[:3] == history       # earlier snapshots byte-identical
    last = rules_db.execute(text("SELECT created_by, reason FROM lending.lender_rule_snapshots WHERE version_no = 4")).one()
    assert tuple(last) == ("josh", "rollback to v2")
    posted = rules_db.execute(text("SELECT summary FROM lending.cora_inbox WHERE event_type = 'lender_rules_rolled_back'")).scalars().all()
    assert len(posted) == 1 and "josh" in posted[0] and "v3" in posted[0] and "v2" in posted[0]


def test_rolling_back_to_the_rules_already_in_force_does_nothing(rules_db):
    _set(rules_db, 0.9)
    result = store.rollback_to(rules_db, 2, actor="josh")
    assert not result.changed and _versions(rules_db) == [1, 2]
    assert rules_db.execute(text("SELECT count(*) FROM lending.cora_inbox")).scalar_one() == 0


def test_restoring_the_version_in_force_as_another_operator_does_nothing(rules_db):
    """PR #474 re-review 1: updated_by differs, the rules do not; no new version and no alert."""
    _set(rules_db, 0.9, actor="josh")                    # v2
    result = store.rollback_to(rules_db, 2, actor="lesly")
    assert not result.changed and _versions(rules_db) == [1, 2]
    assert rules_db.execute(text("SELECT count(*) FROM lending.cora_inbox")).scalar_one() == 0


def test_rollback_takes_the_write_lock_before_reading_the_version_it_replaces(rules_db):
    """PR #474 re-review 2: an edit committed while rollback waits must be the version it reports replacing."""
    _set(rules_db, 0.9)
    seen = []

    @event.listens_for(rules_db.connection(), "before_cursor_execute")
    def _log(_conn, _cursor, statement, *_):
        seen.append(statement)

    store.rollback_to(rules_db, 1, actor="josh")
    lock = next(i for i, s in enumerate(seen) if "pg_advisory_xact_lock" in s)
    read = next(i for i, s in enumerate(seen) if "max(version_no)" in s)
    assert lock < read


def test_a_rollback_restores_a_deleted_rule_and_removes_a_later_one(rules_db):
    _set(rules_db, 0.9)                                              # v2
    _set(rules_db, 680, field="credit_floor")                        # v3
    rules_db.execute(text("DELETE FROM lending.lender_rule_values WHERE field = 'max_ltv'"))  # v4: the 0.9 rule is gone
    assert _value(rules_db) is None
    store.rollback_to(rules_db, 2, actor="josh")
    assert _value(rules_db) == 0.9 and _value(rules_db, "credit_floor") is None


def test_an_unknown_version_is_refused(rules_db):
    with pytest.raises(store.SnapshotNotFound):
        store.rollback_to(rules_db, 99, actor="josh")


def test_an_export_loads_into_another_database_as_one_version(rules_db):
    _set(rules_db, 0.9)
    exported = json.loads(json.dumps(store.get_snapshot(rules_db, 2)))
    # The target's newest snapshot must differ from the import, as another database's would (v3: table emptied).
    rules_db.execute(text("DELETE FROM lending.lender_rule_values"))
    assert _versions(rules_db) == [1, 2, 3]
    version = store.replace_rules(rules_db, exported, actor="josh", reason="imported from prod v2")
    assert _value(rules_db) == 0.9 and version == 4 and _versions(rules_db) == [1, 2, 3, 4]
    assert rules_db.execute(text("SELECT reason FROM lending.lender_rule_snapshots WHERE version_no = 4")).scalar_one() == "imported from prod v2"


def test_the_current_version_is_the_newest_snapshot(rules_db):
    assert store.current_rule_version(rules_db) == 1
    _set(rules_db, 0.9)
    assert store.current_rule_version(rules_db) == 2
    assert [s["version_no"] for s in store.list_snapshots(rules_db)] == [2, 1]

def test_rollback_and_import_preserve_seeded_json_null(rules_db):
    from migrations.apply_lending_lender_rule_values import apply_to as seed_rules

    seed_rules(rules_db.connection(), rules_db.info["schema"])
    version = store.current_rule_version(rules_db)
    exported = json.loads(json.dumps(store.get_snapshot(rules_db, version)))
    assert any(row["value"] is None for row in exported)
    _set(rules_db, 680, field="credit_floor")
    store.rollback_to(rules_db, version, actor="test")
    assert store.current_rules(rules_db) and any(row["value"] is None for row in store.current_rules(rules_db))
    rules_db.execute(text("DELETE FROM lending.lender_rule_values"))
    store.replace_rules(rules_db, exported, actor="test", reason="import")
    assert any(row["value"] is None for row in store.current_rules(rules_db))

def test_manual_writes_acquire_advisory_lock_before_statement(rules_db):
    trigger = rules_db.execute(text("SELECT pg_get_triggerdef(oid) FROM pg_trigger "
                                    "WHERE tgrelid = CAST(:table AS regclass) "
                                    "AND tgname = 'lender_rule_values_lock'"),
                               {"table": f'{rules_db.info["schema"]}.lender_rule_values'}).scalar_one()
    assert "BEFORE INSERT OR DELETE OR UPDATE" in trigger and "FOR EACH STATEMENT" in trigger
