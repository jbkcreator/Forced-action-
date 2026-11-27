"""R1 1.5: GHL texts and emails into the unified inbox, and who answered them (reply agent or a person)."""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from config.lending_compliance import OptOutChannel
from config.lending_unified_inbox import REPLY_AGENT_HANDOFF_MESSAGES, REPLY_AGENT_OPT_OUT_MESSAGES
from src.lending import reply_webhook
from src.lending.inbox import ghl_adapter, unified_triage
from src.lending.inbox.ghl_adapter import enqueue_ghl_message, ghl_message_item, mark_answered
from src.lending.reply_guard import parse_event

SECRET = "s3cret"


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    settings = SimpleNamespace(lending_unified_inbox_enabled=True)
    monkeypatch.setattr(unified_triage, "get_settings", lambda: settings)
    monkeypatch.setattr(ghl_adapter, "get_settings", lambda: settings)


def raw_msg(body="I have a flip in Tampa", message_id="m-1", **extra):
    base = {"messageId": message_id, "body": body, "direction": "inbound", "phone": "(813) 555-0101",
            "contact": {"id": "c-1", "firstName": "Marcus"}}
    base.update(extra)
    return base


def item_for(raw):
    return ghl_message_item(parse_event(raw), raw)


def test_an_inbound_text_becomes_a_ghl_sms_item():
    item = item_for(raw_msg())
    assert (item.source, item.channel, item.dedup_key) == ("ghl", "ghl_sms", "ghl_msg:m-1")
    assert item.ghl_contact_id == "c-1" and item.first_name == "Marcus" and item.stop is False


def test_an_email_type_becomes_a_ghl_email_item_with_subject():
    raw = raw_msg(messageType="TYPE_EMAIL", subject="Re: funding", email="Marcus@Example.com")
    item = item_for(raw)
    assert item.channel == "ghl_email" and item.subject == "Re: funding" and item.email == "Marcus@Example.com"


def test_a_nested_message_type_is_read_too():
    assert item_for(raw_msg(message={"type": "Email", "body": "hello there"})).channel == "ghl_email"


def test_an_unknown_type_is_ghl_other():
    assert item_for(raw_msg(messageType="TYPE_FACEBOOK")).channel == "ghl_other"


def test_outbound_and_ai_messages_are_not_recorded():
    assert item_for(raw_msg(direction="outbound")) is None
    assert item_for(raw_msg(direction="outbound", userId="u1")) is None


def test_an_event_with_no_phone_and_no_contact_is_not_recorded():
    raw = {"messageId": "m-9", "body": "hello", "direction": "inbound"}
    assert item_for(raw) is None


def test_a_ghl_stop_text_is_not_recorded():
    assert item_for(raw_msg(body="STOP")) is None
    assert item_for(raw_msg(body="Please stop")) is None


def test_a_stop_phrase_text_is_opted_out_by_us_because_ghl_only_handles_keywords():
    item = item_for(raw_msg(body="please remove me from your list"))
    assert item.stop is True and item.channel == "ghl_sms"


def test_a_stop_email_is_a_stop_item():
    item = item_for(raw_msg(messageType="TYPE_EMAIL", body="Please remove me from your list.",
                            email="m@example.com"))
    assert item.stop is True and item.channel == "ghl_email"


class Db:
    def __init__(self):
        self.inserted, self.params = set(), []

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, stmt, params=None):
        self.params.append(params)
        key = (params or {}).get("dedup_key")
        first = key not in self.inserted
        self.inserted.add(key)
        return SimpleNamespace(scalar_one_or_none=lambda: 1 if first else None, rowcount=0)


def test_a_ghl_stop_email_is_opted_out_at_once_and_once(monkeypatch):
    calls = []
    monkeypatch.setattr(ghl_adapter, "propagate_opt_out", lambda db, **kw: calls.append(kw) or 5)
    raw = raw_msg(body="Please remove me from your list.", messageType="TYPE_EMAIL", email="m@example.com")
    db = Db()
    for _ in range(2):  # a redelivery
        enqueue_ghl_message(db, parse_event(raw), raw)
    assert len(calls) == 1
    assert calls[0]["email"] == "m@example.com" and calls[0]["channel"] == OptOutChannel.EMAIL
    assert calls[0]["source_ref"] == "inbox:ghl_msg:m-1" and calls[0].get("phone") is None
    assert db.params[0]["status"] == "done" and db.params[0]["intent"] == "stop"


def test_a_stop_text_without_an_email_is_opted_out_by_phone(monkeypatch):
    calls = []
    monkeypatch.setattr(ghl_adapter, "propagate_opt_out", lambda db, **kw: calls.append(kw) or 5)
    raw = raw_msg(body="don't text me again, remove me")
    enqueue_ghl_message(Db(), parse_event(raw), raw)
    assert calls[0]["channel"] == OptOutChannel.SMS and calls[0]["phone"] and calls[0].get("email") is None


def test_a_stop_text_opts_the_texting_phone_out_even_when_an_email_is_known(monkeypatch):
    """Review finding 2: the number that texted must be suppressed directly, not only through a linked email."""
    calls = []
    monkeypatch.setattr(ghl_adapter, "propagate_opt_out", lambda db, **kw: calls.append(kw) or 5)
    raw = raw_msg(body="please remove me from your list", email="m@example.com")
    enqueue_ghl_message(Db(), parse_event(raw), raw)
    assert calls[0]["channel"] == OptOutChannel.SMS and calls[0]["phone"] and calls[0]["email"] == "m@example.com"


def test_a_stop_with_nothing_to_opt_out_goes_to_triage_instead(monkeypatch):
    monkeypatch.setattr(ghl_adapter, "propagate_opt_out", lambda db, **kw: pytest.fail("nothing to opt out"))
    raw = {"messageId": "m-2", "body": "remove me from your list", "direction": "inbound", "contact": {"id": "c-1"}}
    db = Db()
    enqueue_ghl_message(db, parse_event(raw), raw)
    assert db.params[0]["status"] == "new"


def test_a_failed_opt_out_raises_so_the_journal_retries(monkeypatch):
    def boom(db, **kw):
        raise RuntimeError("db down")

    monkeypatch.setattr(ghl_adapter, "propagate_opt_out", boom)
    raw = raw_msg(body="please remove me from your list")
    with pytest.raises(RuntimeError):
        enqueue_ghl_message(Db(), parse_event(raw), raw)


# ── who answered the text: the reply agent or a person (a real SQL run on SQLite) ─────────────────────────────

@pytest.fixture
def sql():
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "connect")
    def attach(conn, _):
        conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE lending.inbox_events (id INTEGER PRIMARY KEY, dedup_key TEXT, channel TEXT, status TEXT, "
            "ghl_contact_id TEXT, answered_by TEXT, answered_at TEXT, ai_handoff BOOLEAN DEFAULT 0)")
        conn.exec_driver_sql("INSERT INTO lending.inbox_events (dedup_key, channel, status, ghl_contact_id) "
                             "VALUES ('ghl_msg:m-1', 'ghl_sms', 'new', 'c-1'), "
                             "('ghl_msg:m-2', 'ghl_sms', 'done', 'c-1'), "
                             "('ghl_msg:m-3', 'ghl_email', 'new', 'c-1'), "
                             "('ghl_msg:m-4', 'ghl_sms', 'new', 'c-2')")
    with Session(engine) as db:
        yield db


def row(db, key):
    return db.execute(text("SELECT answered_by, ai_handoff, answered_at FROM lending.inbox_events "
                           "WHERE dedup_key = :k"), {"k": key}).first()


def outbound(body, person=False):
    raw = {"messageId": "o-1", "body": body, "direction": "outbound", "contact": {"id": "c-1"}}
    if person:
        raw["userId"] = "u-josh"
    return parse_event(raw)


def test_an_ai_answer_marks_the_open_text_answered_by_the_reply_agent(sql):
    assert mark_answered(sql, outbound("Sure, what day works for a call?")) == 1
    answered_by, handoff, at = row(sql, "ghl_msg:m-1")
    assert answered_by == "reply_agent" and not handoff and at is not None
    assert row(sql, "ghl_msg:m-2")[0] is None  # already triaged
    assert row(sql, "ghl_msg:m-3")[0] is None  # an email is never answered by the agent
    assert row(sql, "ghl_msg:m-4")[0] is None  # another contact


def test_an_ai_handoff_message_sets_ai_handoff_even_with_odd_spacing_and_case(sql):
    shouted = "  " + REPLY_AGENT_HANDOFF_MESSAGES[1].upper().replace(" ", "  ") + " "
    mark_answered(sql, outbound(shouted))
    assert row(sql, "ghl_msg:m-1")[:2] == ("reply_agent", 1)


def test_a_curly_apostrophe_still_matches_a_handoff_message(sql):
    mark_answered(sql, outbound(REPLY_AGENT_HANDOFF_MESSAGES[0].replace("'", "’")))
    assert row(sql, "ghl_msg:m-1")[1] == 1


def test_a_person_reply_in_ghl_marks_the_open_text_answered_by_a_person(sql):
    mark_answered(sql, outbound("Hi Marcus, Josh here", person=True))
    assert row(sql, "ghl_msg:m-1")[0] == "person"


def test_an_agent_answer_never_overwrites_a_person_or_clears_a_handoff(sql):
    mark_answered(sql, outbound("Josh here", person=True))
    mark_answered(sql, outbound(REPLY_AGENT_HANDOFF_MESSAGES[1]))
    mark_answered(sql, outbound("Sure, what day works?"))
    assert row(sql, "ghl_msg:m-1")[:2] == ("person", 1)


def test_mark_answered_ignores_inbound_events_and_does_nothing_while_disabled(sql, monkeypatch):
    assert mark_answered(sql, parse_event(raw_msg())) == 0
    monkeypatch.setattr(ghl_adapter, "get_settings", lambda: SimpleNamespace(lending_unified_inbox_enabled=False))
    assert mark_answered(sql, outbound("hello")) == 0
    assert row(sql, "ghl_msg:m-1")[0] is None


# ── wiring in the reply webhook ───────────────────────────────────────────────────────────────────────────────

def test_a_redelivered_ghl_message_is_one_item(monkeypatch):
    monkeypatch.setattr(reply_webhook, "conversations_enabled", lambda: False)
    monkeypatch.setattr(reply_webhook, "handle_reply_handoff", lambda db, raw: "ignored")
    db = Db()
    raw = raw_msg()
    for _ in range(3):
        reply_webhook.handle_ghl_reply(db, raw)
    assert len([p for p in db.params if p and p.get("dedup_key")]) == 3  # three attempts to insert...
    assert len(db.inserted) == 1  # ...one key, and the repeats came back as duplicates


def test_the_journal_handler_saves_the_item_before_the_handoff(monkeypatch):
    monkeypatch.setattr(reply_webhook, "conversations_enabled", lambda: False)
    order = []
    monkeypatch.setattr(reply_webhook, "record_ghl_event", lambda db, event, raw: order.append("inbox"))
    monkeypatch.setattr(reply_webhook, "handle_reply_handoff", lambda db, raw: order.append("handoff") or "posted")
    reply_webhook.handle_ghl_reply(Db(), raw_msg())
    assert order == ["inbox", "handoff"]


def _client(monkeypatch, db):
    from config.settings import get_settings
    from src.api.deps import get_db

    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(settings, "lending_inbound_journal_enabled", False, raising=False)
    monkeypatch.setattr(reply_webhook, "conversations_enabled", lambda: False)
    app = FastAPI()
    app.include_router(reply_webhook.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def test_the_non_journal_path_answers_503_when_the_item_cannot_be_saved(monkeypatch):
    class Failing(Db):
        rolled_back = False

        def execute(self, stmt, params=None):
            raise RuntimeError("db down")

        def rollback(self):
            self.rolled_back = True

        def commit(self):
            raise AssertionError("nothing to commit")

    db = Failing()
    monkeypatch.setattr(reply_webhook, "handle_reply_event", lambda *a, **k: pytest.fail("handoff must not run"))
    response = _client(monkeypatch, db).post("/webhooks/lending/ghl-reply", headers={"X-Webhook-Secret": SECRET},
                                             json=raw_msg())
    assert response.status_code == 503 and "retried" in response.json()["detail"] and db.rolled_back
    assert "db down" not in response.text
