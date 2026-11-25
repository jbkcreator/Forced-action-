"""R1 0.5 T5: /webhooks/lending/ghl-reply on the inbound journal (LENDING_INBOUND_JOURNAL_ENABLED on).

No database: the journal store and the lending session are in-memory fakes. The fake session keeps
``lending.reply_handoffs`` by message id and refuses ``commit``: the handler runs inside the journal's
transaction (and the replay's savepoint), so it must never commit on its own.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.lending import reply_guard, reply_webhook, webhook_failures
from src.lending.inbound import journal

SECRET = "s3cret"
RATE_QUESTION = {"messageId": "m-1", "body": "What's your rate?", "direction": "inbound",
                 "contact": {"id": "c-1", "firstName": "Marcus"}}


class _Result:
    def __init__(self, value=None, rowcount=0):
        self._value, self.rowcount = value, rowcount

    def scalar(self):
        return self._value

    def scalar_one_or_none(self):
        return self._value


class FakeLendingDb:
    """``lending.reply_handoffs`` as {message_id: posted}; writes made in a transaction that fails are dropped."""

    def __init__(self):
        self.handoffs: dict[str, bool] = {}
        self.pending: dict[str, bool] = {}

    def execute(self, statement, params=None):
        sql, params = str(statement), params or {}
        if "INSERT INTO lending.reply_handoffs" in sql:
            key = params["message_id"]
            if key not in self.handoffs and key not in self.pending:
                self.pending[key] = False
            return _Result()
        if "SELECT posted_at IS NOT NULL" in sql:
            key = params["message_id"]
            return _Result(self.pending.get(key, self.handoffs.get(key)))
        if "SET posted_at = now()" in sql:
            self.pending[params["message_id"]] = True
            return _Result(rowcount=1)
        if "lending.cora_inbox" in sql:
            return _Result(1)
        return _Result(rowcount=0)

    @contextmanager
    def begin_nested(self):
        yield

    def commit(self):
        raise AssertionError("the reply handler must not commit inside the journal's transaction")

    def rollback(self):
        raise AssertionError("the reply handler must not roll back the journal's transaction")

    # the transaction boundary, driven by the fake journal store / the replay
    def finish(self, ok: bool):
        if ok:
            self.handoffs.update(self.pending)
        self.pending = {}


class FakeStore:
    """The journal store contract (see tests/lending/test_inbound_journal.py) over one FakeLendingDb."""

    def __init__(self, db: FakeLendingDb):
        self.db, self.rows = db, {}

    def insert_raw(self, source, dedup_key, raw):
        if any(r.source == source and r.dedup_key == dedup_key for r in self.rows.values()):
            return None
        event_id = len(self.rows) + 1
        self.rows[event_id] = SimpleNamespace(id=event_id, source=source, dedup_key=dedup_key, raw=dict(raw),
                                              status="received", failure_id=None)
        return event_id

    def find_id(self, source, dedup_key):
        return next(r.id for r in self.rows.values() if r.source == source and r.dedup_key == dedup_key)

    @contextmanager
    def transaction(self):
        processed = []
        tx = SimpleNamespace(db=self.db, mark_processed=processed.append, mark_failed=self.mark_failed,
                             lock=lambda event_id, skip_locked=False: self.rows[event_id])
        try:
            yield tx
        except Exception:
            self.db.finish(False)
            raise
        self.db.finish(True)
        for event_id in processed:
            self.rows[event_id].status = "processed"

    def mark_failed(self, event_id, failure_id, error_class):
        if failure_id is not None:
            self.rows[event_id].status, self.rows[event_id].failure_id = "failed", failure_id

    def stuck_ids(self, **_):
        return []


class Poster:
    def __init__(self):
        self.messages, self.error = [], None

    def __call__(self, message):
        if self.error is not None:
            raise self.error
        self.messages.append(message)


@pytest.fixture
def lending_db():
    return FakeLendingDb()


@pytest.fixture
def store(lending_db, monkeypatch):
    fake = FakeStore(lending_db)
    monkeypatch.setattr(journal, "SqlJournalStore", lambda: fake)
    return fake


@pytest.fixture
def saved(monkeypatch):
    """webhook_failures rows the journal saved: (endpoint, body, error class)."""
    rows: list[tuple[str, dict, str]] = []

    def insert(db, endpoint, body, error):
        rows.append((endpoint, body, error))
        return len(rows)

    def report(endpoint, body, error, *, save=None):
        if save is not None:
            return save()
        rows.append((endpoint, body, type(error).__name__))
        return len(rows)

    monkeypatch.setattr(webhook_failures, "insert_failure", insert)
    monkeypatch.setattr(webhook_failures, "report_webhook_failure", report)
    return rows


@pytest.fixture
def poster(monkeypatch):
    poster = Poster()
    monkeypatch.setattr(reply_webhook, "slack_poster", lambda: poster)
    monkeypatch.setattr(reply_guard, "hidden_ghl_contacts", lambda db, ids: set())
    monkeypatch.setattr(reply_guard, "names_by_ghl_contact", lambda db, ids: {})
    return poster


@pytest.fixture
def send_cycles(monkeypatch):
    runs: list[str] = []
    monkeypatch.setattr(reply_webhook, "run_send_cycle_now", lambda: runs.append("run"))
    return runs


@pytest.fixture
def client(store, saved, poster, send_cycles, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db

    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(settings, "lending_inbound_journal_enabled", True, raising=False)
    monkeypatch.setattr(reply_webhook, "conversations_enabled", lambda: False)
    app = FastAPI()
    app.include_router(reply_webhook.router)
    app.dependency_overrides[get_db] = lambda: SimpleNamespace()  # flag on: the endpoint never uses it
    return TestClient(app)


def post(client, body):
    return client.post("/webhooks/lending/ghl-reply", headers={"X-Webhook-Secret": SECRET}, json=body)


def test_a_duplicate_reply_posts_the_handoff_once(client, poster, store):
    responses = [post(client, RATE_QUESTION) for _ in range(3)]
    assert [r.status_code for r in responses] == [200, 200, 200]
    assert [r.json()["status"] for r in responses] == ["posted", "duplicate", "duplicate"]
    assert len(poster.messages) == 1
    assert [(r.source, r.dedup_key, r.status) for r in store.rows.values()] == [("ghl_reply", "m-1", "processed")]


def test_a_failed_slack_post_answers_200_and_saves_only_the_handoff(client, poster, saved, store):
    poster.error = RuntimeError("slack down")
    response = post(client, RATE_QUESTION)
    assert response.status_code == 200 and response.json() == {"status": "handoff_saved_for_retry"}
    assert [(endpoint, body["messageId"], error) for endpoint, body, error in saved] == [
        ("ghl_reply_handoff", "m-1", "RuntimeError")]
    assert [r.status for r in store.rows.values()] == ["processed"]
    assert poster.messages == []


def test_an_unset_replies_channel_answers_200_and_saves_the_handoff(client, saved, monkeypatch):
    monkeypatch.setattr(reply_webhook, "slack_poster", lambda: None)
    response = post(client, RATE_QUESTION)
    assert response.status_code == 200 and response.json() == {"status": "handoff_saved_for_retry"}
    assert [(endpoint, error) for endpoint, _, error in saved] == [
        ("ghl_reply_handoff", "ReplyHandoffNotConfigured")]


def test_a_failed_handoff_keeps_the_after_hours_answer(client, poster, saved, send_cycles, monkeypatch):
    from src.lending.after_hours_conversation import OUTCOME_ANSWERED

    monkeypatch.setattr(reply_webhook, "conversations_enabled", lambda: True)
    monkeypatch.setattr(reply_webhook, "record_reply", lambda db, **kwargs: OUTCOME_ANSWERED)
    poster.error = RuntimeError("slack down")
    response = post(client, RATE_QUESTION)
    assert response.status_code == 200
    assert send_cycles == ["run"]  # the answer was applied: the next question goes out
    assert [endpoint for endpoint, _, _ in saved] == ["ghl_reply_handoff"]


def test_a_handoff_that_cannot_be_saved_retries_the_whole_message(client, poster, store, monkeypatch):
    poster.error = RuntimeError("slack down")
    monkeypatch.setattr(webhook_failures, "report_webhook_failure", lambda endpoint, body, error, **kwargs: None)
    def fail_insert(*args):
        raise ConnectionError("db down")

    monkeypatch.setattr(webhook_failures, "insert_failure", fail_insert)
    response = post(client, RATE_QUESTION)
    assert response.status_code == 200 and response.json() == {"status": "failed"}
    assert [r.status for r in store.rows.values()] == ["received"]  # the 5-minute reclaim runs it again


def test_an_after_hours_answer_that_cannot_be_saved_answers_200_and_is_saved(client, saved, poster, send_cycles,
                                                                           monkeypatch):
    def database_down(db, **kwargs):
        raise ConnectionError("db down")

    monkeypatch.setattr(reply_webhook, "conversations_enabled", lambda: True)
    monkeypatch.setattr(reply_webhook, "record_reply", database_down)
    response = post(client, RATE_QUESTION)
    assert response.status_code == 200 and response.json() == {"status": "failed"}
    assert [(endpoint, error) for endpoint, _, error in saved] == [("ghl_reply", "ConnectionError")]
    assert poster.messages == [] and send_cycles == []


def test_the_replay_of_a_saved_reply_posts_the_handoff_once(client, poster, saved, lending_db):
    poster.error = RuntimeError("slack down")
    post(client, RATE_QUESTION)
    poster.error = None
    (_, body, _), = saved
    replay = webhook_failures.ENDPOINT_HANDLERS["ghl_reply_handoff"]
    for _ in range(2):  # the 5-minute retry, then a manual --replay of the same row
        replay(lending_db, body)
        lending_db.finish(True)
    assert len(poster.messages) == 1


def test_the_next_question_is_sent_only_after_an_answer_was_applied(client, send_cycles, monkeypatch):
    monkeypatch.setattr(reply_webhook, "conversations_enabled", lambda: True)
    monkeypatch.setattr(reply_webhook, "record_reply", lambda db, **kwargs: reply_webhook.OUTCOME_ANSWERED)
    answer = {"messageId": "m-2", "body": "Fix and flip", "direction": "inbound", "contact": {"id": "c-1"}}
    assert post(client, answer).json() == {"status": "ignored"}
    assert post(client, answer).json() == {"status": "duplicate"}
    assert send_cycles == ["run"]


def test_a_message_without_text_is_not_journaled(client, store):
    assert post(client, {"messageId": "m-3"}).json() == {"status": "ignored", "reason": "no_message"}
    assert store.rows == {}


def test_a_message_without_a_ghl_id_keeps_its_fallback_id_for_the_replay(client, saved, poster, store):
    poster.error = RuntimeError("slack down")
    body = {"body": "What's your rate?", "direction": "inbound", "contactId": "c-9"}
    post(client, body)
    (row,) = store.rows.values()
    assert row.dedup_key.startswith("auto-") and row.raw["messageId"] == row.dedup_key
    assert saved[0][1]["messageId"] == row.dedup_key


def test_a_raw_save_failure_answers_503_without_running_the_handler(client, store, poster, monkeypatch):
    def down(*args):
        raise ConnectionError("db down")

    monkeypatch.setattr(store, "insert_raw", down)
    response = post(client, RATE_QUESTION)
    assert response.status_code == 503 and "db down" not in response.text
    assert poster.messages == []
