"""R1 0.5 T1: the inbound journal saves each webhook raw first, then runs its handler once.

No database: the journal store is an in-memory fake with the same contract as the Postgres one
(unique (source, dedup_key), a transaction whose writes are dropped when the handler raises).
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from src.lending.inbound import journal

SOURCE = "test_source"
RAW = {"contact_id": "ghl-c-42", "phone": "+18135559301", "message": "hello"}


class MemoryTx:
    def __init__(self, store: "MemoryStore"):
        self.store = store
        self.db = SimpleNamespace(name="fake-db")
        self.processed: list[int] = []

    def lock(self, event_id: int, *, skip_locked: bool = False):
        if skip_locked and event_id in self.store.held_by_another_worker:
            return None
        row = self.store.rows[event_id]
        return SimpleNamespace(id=row.id, source=row.source, status=row.status, raw=dict(row.raw))

    def mark_processed(self, event_id: int) -> None:
        self.processed.append(event_id)


class MemoryStore:
    """Raw rows by id. Writes made inside ``transaction`` apply only when it exits cleanly (like a rollback)."""

    def __init__(self):
        self.rows: dict[int, SimpleNamespace] = {}
        self.insert_error: Exception | None = None
        self.held_by_another_worker: set[int] = set()

    def insert_raw(self, source, dedup_key, raw):
        if self.insert_error is not None:
            raise self.insert_error
        if any(r.source == source and r.dedup_key == dedup_key for r in self.rows.values()):
            return None
        event_id = len(self.rows) + 1
        self.rows[event_id] = SimpleNamespace(id=event_id, source=source, dedup_key=dedup_key, raw=dict(raw),
                                              status="received", attempts=0, failure_id=None,
                                              last_error_class=None, minutes_old=0)
        return event_id

    def find_id(self, source, dedup_key):
        return next(r.id for r in self.rows.values() if r.source == source and r.dedup_key == dedup_key)

    @contextmanager
    def transaction(self):
        tx = MemoryTx(self)
        yield tx
        for event_id in tx.processed:
            self.rows[event_id].status = "processed"
            self.rows[event_id].attempts += 1

    def mark_failed(self, event_id, failure_id, error_class):
        row = self.rows[event_id]
        row.attempts += 1
        row.last_error_class = error_class
        if failure_id is not None:
            row.status, row.failure_id = "failed", failure_id

    def stuck_ids(self, *, older_than_seconds, max_attempts, limit):
        return [r.id for r in self.rows.values()
                if r.status == "received" and r.minutes_old * 60 >= older_than_seconds and r.attempts < max_attempts
                ][:limit]


@pytest.fixture
def store():
    return MemoryStore()


@pytest.fixture
def registries():
    """Every registry register_source writes to, restored after the test."""
    from src.lending import webhook_failures

    dicts = (journal._HANDLERS, webhook_failures.ENDPOINT_HANDLERS, webhook_failures.ENDPOINT_LABELS,
             webhook_failures.ENDPOINT_RESEND_HINTS, webhook_failures.ON_RESOLVED)
    saved = [dict(d) for d in dicts]
    yield
    for d, before in zip(dicts, saved):
        d.clear()
        d.update(before)


@pytest.fixture
def calls(registries):
    """SOURCE registered with a handler that records each run and returns "done"."""
    seen: list[dict] = []
    journal.register_source(SOURCE, lambda db, raw: seen.append(raw) or "done", label="test event")
    return seen


def test_the_same_event_delivered_ten_times_runs_its_handler_once(store, calls):
    results = [journal.ingest(SOURCE, "key-1", RAW, store=store) for _ in range(10)]
    assert calls == [RAW]
    assert results[0].outcome == "processed" and results[0].value == "done"
    assert {r.outcome for r in results[1:]} == {"duplicate"}
    assert len({r.event_id for r in results}) == 1
    assert store.rows[results[0].event_id].status == "processed"


class DatabaseDown(Exception):
    pass


def _failing(registries, error: Exception):
    runs: list[dict] = []

    def handler(db, raw):
        runs.append(raw)
        raise error

    journal.register_source(SOURCE, handler, label="test event")
    return runs


def test_a_handler_that_fails_is_saved_for_retry_and_the_result_is_still_ok(store, registries):
    _failing(registries, DatabaseDown("conn refused +18135559301"))
    reported: list[tuple] = []
    result = journal.ingest(SOURCE, "key-1", RAW, store=store,
                            report=lambda source, raw, exc: reported.append((source, raw, type(exc))) or 17)
    assert result.outcome == "failed" and result.failure_id == 17
    assert reported == [(SOURCE, RAW, DatabaseDown)]
    row = store.rows[result.event_id]
    assert (row.status, row.failure_id, row.last_error_class) == ("failed", 17, "DatabaseDown")


def test_a_failed_event_delivered_again_is_left_to_the_retry(store, registries):
    runs = _failing(registries, DatabaseDown())
    journal.ingest(SOURCE, "key-1", RAW, store=store, report=lambda *args: 17)
    assert journal.ingest(SOURCE, "key-1", RAW, store=store, report=lambda *args: 18).outcome == "duplicate"
    assert len(runs) == 1


def test_when_the_raw_body_cannot_be_saved_ingest_raises_and_nothing_runs(store, calls):
    store.insert_error = DatabaseDown("no connection slots")
    with pytest.raises(DatabaseDown):
        journal.ingest(SOURCE, "key-1", RAW, store=store)
    assert calls == [] and store.rows == {}


def test_an_unregistered_source_is_refused_before_anything_is_saved(store, registries):
    with pytest.raises(LookupError):
        journal.ingest("nobody_registered_this", "key-1", RAW, store=store)
    assert store.rows == {}


def test_rerun_duplicates_runs_the_handler_on_every_delivery(store, calls):
    results = [journal.ingest(SOURCE, "key-1", RAW, store=store, rerun_duplicates=True) for _ in range(3)]
    assert len(calls) == 3 and {r.outcome for r in results} == {"processed"}
    assert len({r.event_id for r in results}) == 1


def test_different_keys_are_different_events(store, calls):
    journal.ingest(SOURCE, "key-1", RAW, store=store)
    journal.ingest(SOURCE, "key-2", {**RAW, "message": "second"}, store=store)
    assert len(calls) == 2


def test_an_overlong_key_is_hashed_to_the_same_stored_key_every_time(store, calls):
    long_key = "x" * 600
    first = journal.ingest(SOURCE, long_key, RAW, store=store)
    again = journal.ingest(SOURCE, long_key, RAW, store=store)
    assert again.outcome == "duplicate" and again.event_id == first.event_id
    assert len(store.rows[first.event_id].dedup_key) <= journal.MAX_DEDUP_KEY_LENGTH


def test_when_the_failure_cannot_be_saved_either_the_row_waits_for_reclaim(store, registries):
    _failing(registries, DatabaseDown())
    result = journal.ingest(SOURCE, "key-1", RAW, store=store, report=lambda *args: None)
    assert result.outcome == "failed" and result.failure_id is None
    assert store.rows[result.event_id].status == "received"


# ---- a worker killed between "saved" and "processed" ---------------------------------------------------------

def _saved_but_never_processed(store, minutes_old: int = 3) -> int:
    event_id = store.insert_raw(SOURCE, "key-1", RAW)  # step 1 committed, then the worker died
    store.rows[event_id].minutes_old = minutes_old
    return event_id


def test_a_row_left_received_is_processed_exactly_once_by_the_reclaim(store, calls):
    event_id = _saved_but_never_processed(store)
    first = journal.reclaim_stuck(store=store)
    second = journal.reclaim_stuck(store=store)
    assert calls == [RAW] and first.processed == 1 and second.processed == 0
    assert store.rows[event_id].status == "processed"


def test_a_row_saved_moments_ago_is_left_to_its_own_request(store, calls):
    _saved_but_never_processed(store, minutes_old=1)
    assert journal.reclaim_stuck(store=store).processed == 0 and calls == []


def test_a_row_another_worker_is_processing_is_skipped(store, calls):
    store.held_by_another_worker.add(_saved_but_never_processed(store))
    result = journal.reclaim_stuck(store=store)
    assert result.skipped == 1 and calls == []


def test_a_reclaimed_row_whose_handler_fails_goes_to_the_retry_queue(store, registries):
    _failing(registries, DatabaseDown())
    event_id = _saved_but_never_processed(store)
    result = journal.reclaim_stuck(store=store, report=lambda *args: 21)
    assert result.failed == 1 and store.rows[event_id].status == "failed"
    assert journal.reclaim_stuck(store=store, report=lambda *args: 22).failed == 0


# ---- the 5-minute retry (src.lending.webhook_failures) ---------------------------------------------------------

class RetryDb:
    """Scripted session for retry_pending: one open failure row, every write recorded."""

    def __init__(self, rows):
        self.rows = rows
        self.writes: list[tuple[str, dict]] = []

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, statement, params=None):
        sql = str(statement)
        if sql.lstrip().startswith("SELECT"):
            return SimpleNamespace(all=lambda: self.rows)
        self.writes.append((sql, params or {}))
        return SimpleNamespace()


def _retry(rows):
    from src.lending import webhook_failures

    db = RetryDb(rows)

    @contextmanager
    def session():
        yield db

    alerts: list[str] = []
    result = webhook_failures.retry_pending(session_factory=session, notify=alerts.append)
    return result, db, alerts


def _failure_row(attempts=1):
    return SimpleNamespace(id=31, endpoint=SOURCE, payload=dict(RAW), ghl_contact_id=None, attempts=attempts,
                           created_at=None)


def test_a_replayed_journal_failure_also_marks_its_journal_row_processed(calls):
    result, db, _ = _retry([_failure_row()])
    assert result.resolved == 1 and calls == [RAW]
    journal_updates = [params for sql, params in db.writes if "inbound_events" in sql]
    assert journal_updates == [{"failure_id": 31}]


def test_a_journal_source_that_gives_up_is_alerted_with_its_own_label_and_hint(registries):
    from src.lending import webhook_failures

    def still_down(db, raw):
        raise DatabaseDown()

    journal.register_source(SOURCE, still_down, label="LendingFlow lead",
                            resend_hint="ask LendingFlow to resend the lead")
    result, db, alerts = _retry([_failure_row(attempts=webhook_failures.MAX_RETRY_ATTEMPTS - 1)])
    assert result.gave_up == 1 and not any("inbound_events" in sql for sql, _ in db.writes)
    assert alerts[0].startswith(":rotating_light: LendingFlow lead webhook #31")
    assert "ask LendingFlow to resend the lead" in alerts[0] and "GHL" not in alerts[0]
    assert "+18135559301" not in alerts[0]


def test_the_cron_reclaims_stuck_rows_only_while_the_journal_is_enabled(monkeypatch):
    from src.lending import webhook_failures

    monkeypatch.setattr(webhook_failures, "retry_pending", lambda: webhook_failures.RetryResult())
    reclaimed: list[bool] = []
    monkeypatch.setattr(journal, "reclaim_stuck", lambda: reclaimed.append(True) or journal.ReclaimResult())
    monkeypatch.setattr(journal, "journal_enabled", lambda: False)
    assert webhook_failures.main() == 0 and reclaimed == []
    monkeypatch.setattr(journal, "journal_enabled", lambda: True)
    assert webhook_failures.main() == 0 and reclaimed == [True]


def test_the_journal_is_off_by_default():
    from config.settings import AppSettings

    assert AppSettings.model_fields["lending_inbound_journal_enabled"].default is False
