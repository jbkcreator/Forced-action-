"""R1 0.5 T1: the inbound journal table and the SQL journal store, on a throwaway Postgres (skips without one)."""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import inspect

from migrations.apply_lending_inbound_events import apply_to
from src.lending.inbound import journal


def _store(lending_db):
    @contextmanager
    def session():
        yield lending_db  # commits are savepoints inside the fixture's rolled-back transaction

    return journal.SqlJournalStore(session_factory=session)


def test_apply_creates_inbound_events_and_is_safe_to_rerun(lending_db):
    conn = lending_db.get_bind()
    apply_to(conn)
    apply_to(conn)
    columns = {c["name"] for c in inspect(conn).get_columns("inbound_events", schema="lending")}
    assert {"id", "source", "dedup_key", "raw", "received_at", "status", "attempts", "processed_at",
            "failure_id", "last_error_class"} <= columns


def test_the_sql_store_saves_once_per_key_and_marks_rows(lending_db):
    apply_to(lending_db.get_bind())
    store = _store(lending_db)
    event_id = store.insert_raw("test_source", "key-1", {"a": 1})
    assert event_id is not None
    assert store.insert_raw("test_source", "key-1", {"a": 2}) is None
    assert store.find_id("test_source", "key-1") == event_id
    with store.transaction() as tx:
        row = tx.lock(event_id)
        assert (row.status, row.raw) == ("received", {"a": 1})
        tx.mark_processed(event_id)
    with store.transaction() as tx:
        assert tx.lock(event_id).status == "processed"
    other = store.insert_raw("test_source", "key-2", {"b": 1})
    with store.transaction() as tx:
        assert tx.lock(other).status == "received"
        tx.mark_failed(other, None, "DatabaseDown")  # without a DLQ row, leave it available for reclaim
    with store.transaction() as tx:
        assert tx.lock(other).status == "received"
