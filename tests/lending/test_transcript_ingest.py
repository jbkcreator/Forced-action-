"""T-12 audio fallback: fetch each finished call's transcript and extract it, in the transcript worker."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from config import lending_dispositions as cfg
from src.lending.dialer_port import DialerRequestError
from src.lending.transcript_ingest import DbCallStore, PendingCall, ingest_calls

NOW = datetime(2026, 10, 19, 15, 0, tzinfo=timezone.utc)


def _call(call_id="c1", minutes_ago=1):
    return PendingCall(dialer_call_id=call_id, phone="+18135550101", call_ended_at=NOW - timedelta(minutes=minutes_ago))


class FakeDialer:
    def __init__(self, transcripts=None, errors=None):
        self.transcripts, self.errors, self.asked = transcripts or {}, errors or {}, []

    def call_transcript(self, call_id):
        self.asked.append(call_id)
        if call_id in self.errors:
            raise DialerRequestError("boom", self.errors[call_id])
        return self.transcripts.get(call_id)


class FakeStore:
    def __init__(self, fail_extract=False):
        self.done, self.failed, self.attempted, self.fail_extract = [], [], [], fail_extract

    def extract_and_mark_done(self, call, transcript, *, now):
        if self.fail_extract:
            raise RuntimeError("claude down")
        self.done.append((call.dialer_call_id, transcript, now))

    def mark_failed(self, dialer_call_id):
        self.failed.append(dialer_call_id)

    def mark_attempted(self, dialer_call_ids, *, now):
        self.attempted.extend(dialer_call_ids)


def _run(calls, dialer, store):
    return ingest_calls(calls, dialer, store, now=NOW)


def test_ready_transcript_is_extracted_and_marked_done():
    store = FakeStore()
    counts = _run([_call()], FakeDialer({"c1": "agent: hi"}), store)
    assert store.done == [("c1", "agent: hi", NOW)] and store.attempted == []
    assert counts == {cfg.TRANSCRIPT_DONE: 1}


def test_not_ready_yet_is_stamped_attempted_for_a_later_retry():
    store = FakeStore()
    counts = _run([_call()], FakeDialer({}), store)
    assert store.done == [] and store.attempted == ["c1"]
    assert counts == {cfg.TRANSCRIPT_NOT_READY: 1}


def test_fetch_error_is_stamped_attempted_and_does_not_stop_the_batch():
    store = FakeStore()
    counts = _run([_call("bad"), _call("ok")], FakeDialer({"ok": "agent: hi"}, errors={"bad": 500}), store)
    assert [d[0] for d in store.done] == ["ok"] and store.attempted == ["bad"]
    assert counts == {cfg.TRANSCRIPT_FETCH_FAILED: 1, cfg.TRANSCRIPT_DONE: 1}


def test_extraction_error_is_terminal_failed_never_rebilled():
    """A Claude failure marks the call failed once; it is not retried every 5 s for 30 minutes."""
    store = FakeStore(fail_extract=True)
    counts = _run([_call()], FakeDialer({"c1": "agent: hi"}), store)
    assert store.failed == ["c1"] and store.attempted == []
    assert counts == {cfg.TRANSCRIPT_FAILED: 1}


def test_rate_limit_stops_the_batch_at_the_first_429():
    dialer = FakeDialer({"c2": "agent: hi"}, errors={"c1": 429})
    store = FakeStore()
    counts = _run([_call("c1"), _call("c2")], dialer, store)
    assert dialer.asked == ["c1"] and store.done == []
    assert store.attempted == ["c1"]
    assert counts == {cfg.TRANSCRIPT_RATE_LIMITED: 1}


def test_runbook_5_second_ingestion_interval():
    """Runbook § 3 line 10 / § 5.1: 5-second ingestion."""
    assert cfg.CDR_POLL_SECONDS == 5


def test_pending_predicate_is_shared_by_the_migration_index():
    from migrations.apply_lending_call_transcript_status import index_ddl
    assert cfg.TRANSCRIPT_PENDING_PREDICATE in index_ddl("lending")


class FakeSession:
    def __init__(self, fail_on=None):
        self.events, self.fail_on = [], fail_on

    @contextmanager
    def begin_nested(self):
        self.events.append("savepoint")
        try:
            yield
        except Exception:
            self.events.append("rollback_savepoint")
            raise

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.events.append(("sql", sql.split()[0], (params or {}).get("status")))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("db error")

    def commit(self):
        self.events.append("commit")


def test_db_store_commits_each_extraction_inside_a_savepoint():
    """Billing safety: a later DB error can never roll back an extraction already paid for."""
    db = FakeSession()
    calls = []
    store = DbCallStore(db, extract=lambda db_, **kw: calls.append(kw["phone"]))
    store.extract_and_mark_done(_call(), "agent: hi", now=NOW)
    assert db.events[0] == "savepoint" and db.events[-1] == "commit"
    assert ("sql", "UPDATE", cfg.TRANSCRIPT_DONE) in db.events
    assert calls == ["+18135550101"]


def test_db_store_extraction_failure_rolls_back_only_its_savepoint():
    db = FakeSession()

    def boom(db_, **kw):
        raise RuntimeError("claude down")
    store = DbCallStore(db, extract=boom)
    with pytest.raises(RuntimeError):
        store.extract_and_mark_done(_call(), "agent: hi", now=NOW)
    assert "rollback_savepoint" in db.events and "commit" not in db.events


def test_pending_query_order_retry_wait_and_expiry(lending_db):
    from sqlalchemy import text
    from src.lending.transcript_ingest import expire_stale, fetch_pending

    def add(call_id, *, talk, ended_minutes_ago, status=None, attempted_seconds_ago=None):
        attempted = NOW - timedelta(seconds=attempted_seconds_ago) if attempted_seconds_ago is not None else None
        lending_db.execute(text(
            "INSERT INTO lending.call_dispositions (dialer_call_id, phone, talk_duration_sec, call_ended_at, "
            "transcript_status, transcript_attempted_at, raw_event) "
            "VALUES (:id, '8135550101', :talk, :ended, :status, :attempted, '{}'::jsonb)"),
            {"id": call_id, "talk": talk, "ended": NOW - timedelta(minutes=ended_minutes_ago),
             "status": status, "attempted": attempted})

    add("t12-tried-long-ago", talk=60, ended_minutes_ago=5, attempted_seconds_ago=120)
    add("t12-fresh", talk=60, ended_minutes_ago=1)
    add("t12-just-tried", talk=60, ended_minutes_ago=2, attempted_seconds_ago=5)
    add("t12-unanswered", talk=0, ended_minutes_ago=1)
    add("t12-done", talk=60, ended_minutes_ago=1, status=cfg.TRANSCRIPT_DONE)
    add("t12-old", talk=60, ended_minutes_ago=600)
    assert expire_stale(lending_db, now=NOW) >= 1
    pending = [c for c in fetch_pending(lending_db, now=NOW, limit=50) if c.dialer_call_id.startswith("t12-")]
    assert [c.dialer_call_id for c in pending] == ["t12-fresh", "t12-tried-long-ago"]
    assert pending[0].phone == "+18135550101"
