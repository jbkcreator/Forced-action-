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
    def __init__(self, fail_extract=False, error=None, paused=False, pause_after=None):
        self.done, self.failed, self.attempted, self.deferred = [], [], [], []
        self.error = error or (RuntimeError("invalid output") if fail_extract else None)
        self.paused, self.pause_after, self.extract_calls = paused, pause_after, 0

    def extraction_paused(self):
        return self.paused

    def extract_and_mark_done(self, call, transcript, *, now):
        self.extract_calls += 1
        if self.pause_after is not None and self.extract_calls > self.pause_after:
            self.paused = True
            raise RuntimeError("call extraction returned no structured output")
        if self.error is not None:
            raise self.error
        self.done.append((call.dialer_call_id, transcript, now))

    def mark_failed(self, dialer_call_id):
        self.failed.append(dialer_call_id)

    def mark_deferred(self, dialer_call_id, *, now, count_attempt):
        self.deferred.append((dialer_call_id, count_attempt))

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


def test_invalid_extraction_output_is_terminal_failed_never_rebilled():
    """Invalid output (not a pause, not an API outage) marks the call failed once; never re-billed."""
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


def _api_error(kind):
    import anthropic
    import httpx
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    if kind == "connection":
        return anthropic.APIConnectionError(request=request)
    response = httpx.Response(529 if kind == "overloaded" else 500, request=request)
    return anthropic.APIStatusError("server error", response=response, body=None)


@pytest.mark.parametrize("kind", ["connection", "server", "overloaded"])
def test_temporary_api_error_defers_the_call_and_counts_an_attempt(kind):
    store = FakeStore(error=_api_error(kind))
    counts = _run([_call()], FakeDialer({"c1": "agent: hi"}), store)
    assert store.deferred == [("c1", True)] and store.failed == []
    assert counts == {cfg.TRANSCRIPT_EXTRACT_RETRY: 1}


def test_outage_then_recovery_extracts_the_deferred_call():
    dialer = FakeDialer({"c1": "agent: hi"})
    outage = FakeStore(error=_api_error("connection"))
    _run([_call()], dialer, outage)
    assert outage.deferred == [("c1", True)]
    recovered = FakeStore()
    assert _run([_call()], dialer, recovered) == {cfg.TRANSCRIPT_DONE: 1}
    assert [d[0] for d in recovered.done] == ["c1"]


def test_pause_defers_without_a_claude_call_then_resumes():
    dialer = FakeDialer({"c1": "agent: hi", "c2": "agent: yo"})
    paused = FakeStore(paused=True)
    counts = _run([_call("c1"), _call("c2")], dialer, paused)
    assert paused.extract_calls == 0 and paused.failed == []
    assert paused.deferred == [("c1", False), ("c2", False)]  # pauses never use up retries
    assert counts == {cfg.TRANSCRIPT_PAUSED: 2}
    resumed = FakeStore()
    assert _run([_call("c1"), _call("c2")], dialer, resumed) == {cfg.TRANSCRIPT_DONE: 2}


def test_a_pause_that_starts_mid_batch_defers_the_rest_instead_of_failing_them():
    store = FakeStore(pause_after=1)
    dialer = FakeDialer({"c1": "a", "c2": "b", "c3": "c"})
    counts = _run([_call("c1"), _call("c2"), _call("c3")], dialer, store)
    assert [d[0] for d in store.done] == ["c1"] and store.failed == []
    assert store.deferred == [("c2", False), ("c3", False)] and store.extract_calls == 2
    assert counts == {cfg.TRANSCRIPT_DONE: 1, cfg.TRANSCRIPT_PAUSED: 2}


def _store(db, extract=None, saved=None):
    saved = saved if saved is not None else []
    return DbCallStore(db, extract=extract or (lambda transcript, *, today, db: {"fields": transcript}),
                       save=lambda db_, **kw: saved.append(kw))


def test_db_store_saves_inside_a_savepoint_after_the_claude_call():
    """Billing safety: the Claude call is outside the savepoint; save + status inside; one commit."""
    db = FakeSession()
    saved = []
    _store(db, saved=saved).extract_and_mark_done(_call(), "agent: hi", now=NOW)
    assert db.events[0] == "savepoint" and db.events[-1] == "commit"
    assert ("sql", "UPDATE", cfg.TRANSCRIPT_DONE) in db.events
    assert saved[0]["phone"] == "+18135550101" and saved[0]["extraction"] == {"fields": "agent: hi"}


def test_billed_answer_that_fails_validation_keeps_its_usage_record():
    """The router flushes the usage row before validation; the rejection must not roll it back."""
    db = FakeSession()

    def billed_then_invalid(transcript, *, today, db):
        db.events.append("usage_logged")
        raise ValueError("output failed validation")

    store = _store(db, extract=billed_then_invalid)
    with pytest.raises(ValueError):
        store.extract_and_mark_done(_call(), "agent: hi", now=NOW)
    assert "savepoint" not in db.events and "rollback_savepoint" not in db.events
    store.mark_failed("c1")
    assert db.events[0] == "usage_logged" and db.events[-1] == "commit"
    assert ("sql", "UPDATE", cfg.TRANSCRIPT_FAILED) in db.events


@pytest.fixture
def transcript_db(lending_db):
    """The shared DB, only once the transcript migration is applied: no DDL inside the test
    transaction (an ALTER would lock the live call_dispositions table until rollback)."""
    from sqlalchemy import text
    have = lending_db.execute(text(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'lending' "
        "AND table_name = 'call_dispositions' AND column_name IN ('transcript_status', 'transcript_attempts')"
    )).scalar()
    if have < 2:
        pytest.skip("apply migrations/apply_lending_call_transcript_status.py first")
    return lending_db


def test_pending_query_order_retry_wait_and_expiry(transcript_db):
    lending_db = transcript_db
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


def test_deferred_calls_outlive_the_wait_window_and_stop_after_the_attempt_cap(transcript_db):
    lending_db = transcript_db
    from sqlalchemy import text
    from src.lending.transcript_ingest import expire_stale, fetch_pending

    lending_db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, phone, talk_duration_sec, call_ended_at, "
        "transcript_status, transcript_attempted_at, raw_event) "
        "VALUES ('t12-deferred', '8135550101', 60, :ended, :deferred, :attempted, '{}'::jsonb)"),
        {"ended": NOW - timedelta(hours=5), "deferred": cfg.TRANSCRIPT_DEFERRED,
         "attempted": NOW - timedelta(minutes=10)})
    expire_stale(lending_db, now=NOW)

    def ids(**kw):
        return [c.dialer_call_id for c in fetch_pending(lending_db, now=NOW, limit=50, **kw)
                if c.dialer_call_id == "t12-deferred"]
    assert ids() == ["t12-deferred"]                   # not expired, still picked up
    assert ids(include_deferred=False) == []            # skipped while extraction is paused

    store = DbCallStore(lending_db, extract=None, save=None)
    for _ in range(cfg.TRANSCRIPT_MAX_EXTRACT_ATTEMPTS):
        store.mark_deferred("t12-deferred", now=NOW, count_attempt=True)
    status, attempts = lending_db.execute(text(
        "SELECT transcript_status, transcript_attempts FROM lending.call_dispositions "
        "WHERE dialer_call_id = 't12-deferred'")).one()
    assert (status, attempts) == (cfg.TRANSCRIPT_FAILED, cfg.TRANSCRIPT_MAX_EXTRACT_ATTEMPTS)
