"""Night responder triggers (team decisions Q8 and Q38): an inbound text or a website form outside Josh's hours
opens a conversation, and the GHL contact carries the night-conversation tag while it is open."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from config.lending_after_hours_conversation import (CHANNEL_SMS, GHL_TAG_NIGHT_CONVERSATION, QUESTION_STATES,
                                                     SOURCE_INBOUND_TEXT, SOURCE_WEB_FORM)
from config.lending_occupancy import OWNER_OCCUPIED
from src.lending import after_hours_ghl_tag, reply_webhook, web_leads
from src.lending.after_hours_ghl_tag import GhlTagError, sync_night_tags
from src.lending.reply_guard import ReplyEvent

PHONE = "+18135550199"
NIGHT = datetime(2026, 10, 21, 1, 0, tzinfo=timezone.utc)  # Tue 9:00 pm ET
DAY = datetime(2026, 10, 21, 15, 0, tzinfo=timezone.utc)   # Wed 11:00 am ET


class _Scalar:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeDb:
    def __init__(self, *, lendingflow=False):
        self.lendingflow = lendingflow

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, _statement, _params=None):
        return _Scalar(self.lendingflow)


@pytest.fixture
def opened(monkeypatch):
    calls = []
    monkeypatch.setattr(reply_webhook, "open_conversation", lambda db, **kwargs: calls.append(kwargs) or 7)
    return calls


def _text(body="Hi, I have a flip in Tampa", phone=PHONE):
    return ReplyEvent(message_id="m-1", direction="inbound", body=body, contact_id="c-1", first_name="Mike",
                      phone=phone, handoff=False)


def test_an_inbound_text_at_night_opens_a_conversation_by_text(opened):
    assert reply_webhook._open_for_inbound_text(_FakeDb(), _text(), NIGHT)
    (call,) = opened
    assert (call["source"], call["channel"], call["phone"], call["ghl_contact_id"], call["first_name"]) == (
        SOURCE_INBOUND_TEXT, CHANNEL_SMS, PHONE, "c-1", "Mike")
    assert call["inquiry_key"] == f"after_hours:{SOURCE_INBOUND_TEXT}:m-1"


@pytest.mark.parametrize("now, event, db", [
    (DAY, _text(), _FakeDb()),                                  # inside Josh's hours
    (NIGHT, _text("STOP"), _FakeDb()),                          # a stop request
    (NIGHT, _text(phone=None), _FakeDb()),                      # no phone to text
    (NIGHT, _text(), _FakeDb(lendingflow=True)),                # a LendingFlow lead
])
def test_no_conversation_for_a_text_in_hours_a_stop_no_phone_or_a_lendingflow_lead(opened, now, event, db):
    assert not reply_webhook._open_for_inbound_text(db, event, now)
    assert opened == []


def test_the_text_opens_a_conversation_before_its_consent_webhook_has_arrived(opened):
    # GHL reports the consent this text carries on its own webhook, which can come second: the conversation opens
    # now and the responder holds the first question for the consent (test_after_hours_responder).
    assert reply_webhook._open_for_inbound_text(_FakeDb(), _text(), NIGHT)
    assert len(opened) == 1


class _AdvanceDb(_FakeDb):
    def __init__(self):
        super().__init__()
        self.commits = self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_a_failed_open_is_refused_so_ghl_redelivers_the_text(monkeypatch):
    from src.lending.after_hours_conversation import OUTCOME_NO_CONVERSATION

    def broken(db, **kwargs):
        raise RuntimeError("table missing")

    monkeypatch.setattr(reply_webhook, "conversations_enabled", lambda: True)
    monkeypatch.setattr(reply_webhook, "record_reply", lambda db, **kwargs: OUTCOME_NO_CONVERSATION)
    monkeypatch.setattr(reply_webhook, "in_reply_hours", lambda now: False)
    monkeypatch.setattr(reply_webhook, "open_conversation", broken)
    db = _AdvanceDb()
    # None: the endpoint answers 503 and GHL sends the text again; the message id keys the open, so the
    # redelivery opens exactly one conversation.
    assert reply_webhook._advance_after_hours_conversation(db, _text()) is None
    assert (db.commits, db.rollbacks) == (0, 1)


# ── website form ─────────────────────────────────────────────────────────────

def _lead(**changes):
    values = dict(name="Mike Jones", phone=PHONE, email=None, property_city=None, deal_type=None,
                  completed_projects_3y=None, loan_amount=None, property_address=None, property_state=None,
                  credit_band=None, sms_consent=True, call_consent=False, deal_drop_optin=False, consent_text=None,
                  call_consent_text=None, page_url=None, ip_address=None, user_agent=None)
    values.update(changes)
    return web_leads.WebLeadInput(**values)


@pytest.fixture
def web_opened(monkeypatch):
    from src.lending import after_hours_conversation

    calls = []
    monkeypatch.setattr(after_hours_conversation, "conversations_enabled", lambda: True)
    monkeypatch.setattr(after_hours_conversation, "open_conversation", lambda db, **kwargs: calls.append(kwargs) or 3)
    monkeypatch.setattr(web_leads, "has_text_consent", lambda db, phone: True)
    return calls


def test_a_website_lead_with_the_text_box_ticked_at_night_gets_the_questions(web_opened):
    assert web_leads.open_night_conversation(_FakeDb(), 42, _lead(), NIGHT)
    (call,) = web_opened
    assert (call["source"], call["channel"], call["first_name"], call["inquiry_key"]) == (
        SOURCE_WEB_FORM, CHANNEL_SMS, "Mike", f"after_hours:{SOURCE_WEB_FORM}:42")


@pytest.mark.parametrize("lead, now", [
    (_lead(sms_consent=False), NIGHT),          # text box not ticked
    (_lead(occupancy=OWNER_OCCUPIED), NIGHT),   # owner-occupied leads are never contacted
    (_lead(), DAY),                             # inside Josh's hours
])
def test_no_website_conversation_without_the_text_box_for_owner_occupied_or_in_hours(web_opened, lead, now):
    assert not web_leads.open_night_conversation(_FakeDb(), 42, lead, now)
    assert web_opened == []


def test_no_website_conversation_while_the_night_responder_is_off(web_opened, monkeypatch):
    from src.lending import after_hours_conversation

    monkeypatch.setattr(after_hours_conversation, "conversations_enabled", lambda: False)
    assert not web_leads.open_night_conversation(_FakeDb(), 42, _lead(), NIGHT)


# ── the GHL night-conversation tag ───────────────────────────────────────────

class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _TagDb:
    def __init__(self, to_tag, to_untag):
        self.selects = [to_tag, to_untag]
        self.writes, self.commits = [], 0

    def execute(self, statement, params):
        if str(statement).lstrip().startswith("SELECT"):
            return _Rows(self.selects.pop(0))
        self.writes.append((str(statement).split()[3] if "untagged" in str(statement) else "tagged", params["id"]))
        return None

    def commit(self):
        self.commits += 1


class _Client:
    def __init__(self, found=None, fail=()):
        self.found, self.fail, self.calls = found or {}, set(fail), []

    def find_contact_id(self, phone):
        self.calls.append(("find", phone))
        return self.found.get(phone)

    def add_tag(self, contact_id, tag):
        if contact_id in self.fail:
            raise GhlTagError("add tag: HTTP 500")
        self.calls.append(("add", contact_id, tag))

    def remove_tag(self, contact_id, tag):
        if contact_id in self.fail:
            raise GhlTagError("remove tag: HTTP 500")
        self.calls.append(("remove", contact_id, tag))


def test_open_conversations_are_tagged_and_finished_ones_untagged():
    db = _TagDb(to_tag=[{"id": 1, "phone": PHONE, "ghl_contact_id": None},
                        {"id": 2, "phone": None, "ghl_contact_id": "c-2"}],
                to_untag=[{"id": 3, "ghl_contact_id": "c-3", "still_needed": False}])
    client = _Client(found={PHONE: "c-1"})
    counts = sync_night_tags(db, client, NIGHT)
    assert counts == {"tagged": 2, "untagged": 1}
    assert client.calls == [("find", PHONE), ("add", "c-1", GHL_TAG_NIGHT_CONVERSATION),
                            ("add", "c-2", GHL_TAG_NIGHT_CONVERSATION),
                            ("remove", "c-3", GHL_TAG_NIGHT_CONVERSATION)]
    assert [row_id for _, row_id in db.writes] == [1, 2, 3] and db.commits == 3


def test_a_contact_not_in_ghl_or_a_failed_call_is_left_for_the_next_pass():
    db = _TagDb(to_tag=[{"id": 1, "phone": PHONE, "ghl_contact_id": None},
                        {"id": 2, "phone": None, "ghl_contact_id": "c-2"}],
                to_untag=[{"id": 3, "ghl_contact_id": "c-3", "still_needed": False}])
    counts = sync_night_tags(db, _Client(fail={"c-2", "c-3"}), NIGHT)
    assert counts == {"not_in_ghl": 1, "failed": 2}
    assert db.writes == [] and db.commits == 0


def test_a_finished_conversation_leaves_the_tag_to_the_same_contacts_open_one():
    # Pass order: the new conversation is tagged first, then the old one is cleaned up. The tag is the contact's,
    # so removing it for the old one would leave the new one untagged and GHL Conversation AI answering too.
    db = _TagDb(to_tag=[{"id": 5, "phone": PHONE, "ghl_contact_id": "c-1"}],
                to_untag=[{"id": 4, "ghl_contact_id": "c-1", "still_needed": True}])
    client = _Client()
    assert sync_night_tags(db, client, NIGHT) == {"tagged": 1, "kept_for_open": 1}
    assert client.calls == [("add", "c-1", GHL_TAG_NIGHT_CONVERSATION)]  # never removed
    assert [row_id for _, row_id in db.writes] == [5, 4]  # the old row is settled without touching GHL


def test_the_tag_sync_queries_run_on_postgres(lending_db):
    from migrations.apply_lending_after_hours_conversations import apply_to

    conn = lending_db.connection()
    apply_to(conn)
    apply_to(conn)  # idempotent, columns included
    insert = text("""
        INSERT INTO lending.after_hours_conversations (inquiry_key, source, channel, contact_key, phone, ghl_contact_id,
                                                       state, expires_at, ghl_tagged_at)
        VALUES (:key, 'test', 'sms', :key, :phone, :contact, :state, :expires, :tagged) RETURNING id
    """)
    later = NIGHT + timedelta(hours=6)
    open_id = lending_db.execute(insert, {"key": "TAG-TEST-open", "phone": "+18135550101", "contact": None,
                                          "state": QUESTION_STATES[0], "expires": later, "tagged": None}).scalar()
    booked_id = lending_db.execute(insert, {"key": "TAG-TEST-booked", "phone": "+18135550102", "contact": "c-b",
                                            "state": "booked", "expires": later, "tagged": NIGHT}).scalar()
    client = _Client(found={"+18135550101": "c-o"})
    sync_night_tags(lending_db, client, NIGHT)
    rows = {row["id"]: row for row in lending_db.execute(text(
        "SELECT id, ghl_contact_id, ghl_tagged_at, ghl_untagged_at FROM lending.after_hours_conversations "
        "WHERE id IN (:a, :b)"), {"a": open_id, "b": booked_id}).mappings()}
    assert rows[open_id]["ghl_contact_id"] == "c-o" and rows[open_id]["ghl_tagged_at"] is not None
    assert rows[booked_id]["ghl_untagged_at"] is not None
    assert sync_night_tags(lending_db, _Client(), NIGHT) == {}


def test_on_postgres_an_old_conversation_keeps_the_tag_while_the_contacts_new_one_is_open(lending_db):
    from migrations.apply_lending_after_hours_conversations import apply_to

    apply_to(lending_db.connection())
    insert = text("""
        INSERT INTO lending.after_hours_conversations (inquiry_key, source, channel, contact_key, phone, ghl_contact_id,
                                                       state, expires_at, ghl_tagged_at)
        VALUES (:key, 'test', 'sms', :contact_key, :phone, 'c-same', :state, :expires, :tagged) RETURNING id
    """)
    later = NIGHT + timedelta(hours=6)
    lending_db.execute(insert, {"key": "TAG-TEST-old", "contact_key": "TAG-TEST-old", "phone": PHONE,
                                "state": "booked", "expires": later, "tagged": NIGHT})
    new_id = lending_db.execute(insert, {"key": "TAG-TEST-new", "contact_key": PHONE, "phone": PHONE,
                                         "state": QUESTION_STATES[0], "expires": later, "tagged": None}).scalar()
    client = _Client()
    assert sync_night_tags(lending_db, client, NIGHT) == {"tagged": 1, "kept_for_open": 1}
    assert client.calls == [("add", "c-same", GHL_TAG_NIGHT_CONVERSATION)]
    # Once the new one finishes too, the tag comes off.
    lending_db.execute(text("UPDATE lending.after_hours_conversations SET state = 'closed' WHERE id = :id"),
                       {"id": new_id})
    assert sync_night_tags(lending_db, client, NIGHT) == {"untagged": 1}
    assert client.calls[-1] == ("remove", "c-same", GHL_TAG_NIGHT_CONVERSATION)
