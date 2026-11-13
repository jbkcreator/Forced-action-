"""Night responder conversation: three qualifying questions, then a slot pick; opened by an after-hours voicemail
and advanced by the GHL reply webhook."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from config.lending_after_hours_conversation import (ANSWER_MAX_CHARS, CHANNEL_EMAIL, CHANNEL_SMS, CLOSED,
                                                     CLOSED_OPTED_OUT, CLOSED_UNCLEAR_SLOT, CONVERSATION_TTL_HOURS,
                                                     MAX_SLOT_REPROMPTS, QUESTION_STATES, QUESTIONS, SLOT_CHOICE,
                                                     SLOT_CHOSEN)
from src.lending.after_hours_conversation import (mark_prompted, next_turn, open_conversation, question_message,
                                                  record_reply, slot_pick)

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
PHONE = "+18135550199"
CONTACT_ID = "ghl-contact-1"
NOW = datetime(2026, 10, 21, 1, 0, tzinfo=timezone.utc)  # 9:00 pm ET
SLOTS = [datetime(2026, 10, 21, 13, 0, tzinfo=timezone.utc) + timedelta(minutes=30 * n) for n in range(3)]
ANSWERS = ("Fix and flip", "412 Oak St, Tampa FL", "About 250k, closing on the 30th")


# --- the rules (pure) ---

def test_three_answers_walk_the_questions_in_order_and_end_at_the_slot_offer():
    state, answers = QUESTION_STATES[0], ()
    for expected, answer in zip((*QUESTION_STATES[1:], SLOT_CHOICE), ANSWERS):
        turn = next_turn(state, answers, f"  {answer}  ")
        assert (turn.outcome, turn.state) == ("answered", expected)
        state, answers = turn.state, turn.answers
    assert answers == ANSWERS


def test_a_long_answer_is_cut_and_a_blank_message_changes_nothing():
    assert next_turn(QUESTION_STATES[0], (), "x" * 900).answers == ("x" * ANSWER_MAX_CHARS,)
    blank = next_turn(QUESTION_STATES[1], ("a",), "   ")
    assert (blank.outcome, blank.state, blank.answers) == ("ignored", QUESTION_STATES[1], ("a",))


@pytest.mark.parametrize("state", [*QUESTION_STATES, SLOT_CHOICE])
@pytest.mark.parametrize("message", ["STOP", "stop", "Unsubscribe", "please stop"])
def test_a_stop_request_closes_the_conversation_from_any_waiting_state(state, message):
    turn = next_turn(state, ("a",), message, offered_slot_count=3)
    assert (turn.outcome, turn.state, turn.closed_reason, turn.answers) == ("opted_out", CLOSED, CLOSED_OPTED_OUT, ("a",))


@pytest.mark.parametrize("message, index", [("2", 1), (" 1. ", 0), ("#3", 2), ("Option 2", 1), ("3)", 2)])
def test_a_message_that_is_only_an_offered_number_picks_that_slot(message, index):
    assert slot_pick(message, 3) == index
    turn = next_turn(SLOT_CHOICE, ANSWERS, message, offered_slot_count=3)
    assert (turn.outcome, turn.state, turn.chosen_slot_index) == ("slot_chosen", SLOT_CHOSEN, index)


@pytest.mark.parametrize("message", ["4", "0", "2 units in Tampa", "tomorrow at 9", "12", "the second one"])
def test_anything_else_is_not_a_pick(message):
    assert slot_pick(message, 3) is None


def test_an_unclear_pick_repeats_the_offer_then_closes():
    again = next_turn(SLOT_CHOICE, ANSWERS, "whenever works", offered_slot_count=3, slot_reprompts=MAX_SLOT_REPROMPTS - 1)
    assert (again.outcome, again.state) == ("reprompt", SLOT_CHOICE)
    closed = next_turn(SLOT_CHOICE, ANSWERS, "whenever works", offered_slot_count=3, slot_reprompts=MAX_SLOT_REPROMPTS)
    assert (closed.outcome, closed.state, closed.closed_reason) == ("unclear_slot", CLOSED, CLOSED_UNCLEAR_SLOT)


@pytest.mark.parametrize("state", [SLOT_CHOSEN, "booked", CLOSED])
def test_a_message_after_the_pick_or_the_close_changes_nothing(state):
    turn = next_turn(state, ANSWERS, "1", offered_slot_count=3)
    assert (turn.outcome, turn.state, turn.answers) == ("ignored", state, ANSWERS)


def test_only_the_first_question_carries_the_greeting():
    first = question_message(QUESTION_STATES[0], "Mike")
    assert first.startswith("Hi Mike, this is Next Deal Lending.") and first.endswith(QUESTIONS[0])
    assert question_message(QUESTION_STATES[0], None).startswith("Hi there,")
    assert question_message(QUESTION_STATES[2], "Mike") == QUESTIONS[2]


# --- the stored conversation ---

@pytest.fixture
def db(conversations_db):
    lending_db = conversations_db
    lending_db.execute(text("DELETE FROM lending.after_hours_conversations WHERE contact_key = :p"), {"p": PHONE})
    lending_db.execute(text("DELETE FROM lending.contacts WHERE phone = :p"), {"p": PHONE})
    lending_db.execute(text("DELETE FROM lending.text_consents WHERE phone = :p"), {"p": PHONE})
    return lending_db


def _open(db, inquiry_key="after_hours:ghl_voicemail:vm-1", **overrides):
    fields = {"source": "ghl_voicemail", "channel": CHANNEL_SMS, "phone": "(813) 555-0199", "first_name": "Mike",
              "now": NOW, **overrides}
    return open_conversation(db, inquiry_key=inquiry_key, **fields)


def _row(db, conversation_id):
    return db.execute(text("SELECT * FROM lending.after_hours_conversations WHERE id = :id"),
                      {"id": conversation_id}).mappings().one()


def _reply(db, message_id, body, *, minutes=1, **who):
    who = who or {"phone": PHONE}
    return record_reply(db, message_id=message_id, body=body, now=NOW + timedelta(minutes=minutes), **who)


def _answer_all(db, conversation_id):
    for position, answer in enumerate(ANSWERS):
        mark_prompted(db, conversation_id, QUESTION_STATES[position], now=NOW)
        assert _reply(db, f"m-{position}", answer) == "answered"


def test_open_stores_the_normalized_phone_and_starts_at_the_first_question(db):
    row = _row(db, _open(db))
    assert (row["state"], row["prompted_state"], row["contact_key"], row["phone"]) == (QUESTION_STATES[0], None, PHONE, PHONE)
    assert row["answers"] == [] and row["expires_at"] == NOW + timedelta(hours=CONVERSATION_TTL_HOURS)


def test_the_same_inquiry_or_a_second_inquiry_from_the_same_contact_opens_nothing(db):
    assert _open(db) is not None
    assert _open(db) is None
    assert _open(db, inquiry_key="after_hours:ghl_voicemail:vm-2") is None


def test_a_contact_can_open_a_new_conversation_once_the_last_one_is_closed(db):
    first = _open(db)
    mark_prompted(db, first, QUESTION_STATES[0], now=NOW)
    assert _reply(db, "m-stop", "STOP") == "opted_out"
    assert _open(db, inquiry_key="after_hours:ghl_voicemail:vm-2") is not None


def test_an_email_conversation_is_keyed_on_the_lower_cased_address(db):
    row = _row(db, _open(db, channel=CHANNEL_EMAIL, phone=None, email=" Mike@Example.com "))
    assert (row["contact_key"], row["email"], row["phone"]) == ("mike@example.com", "mike@example.com", None)
    with pytest.raises(ValueError):
        _open(db, inquiry_key="k-2", channel=CHANNEL_SMS, phone=None, email="mike@example.com")


def test_a_reply_before_the_question_went_out_is_not_an_answer(db):
    conversation_id = _open(db)
    assert _reply(db, "m-early", "Fix and flip") == "not_awaiting"
    assert _row(db, conversation_id)["answers"] == []


def test_two_quick_texts_answer_one_question_only(db):
    conversation_id = _open(db)
    mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)
    assert _reply(db, "m-1", "Fix and flip") == "answered"
    assert _reply(db, "m-2", "in Tampa") == "not_awaiting"
    row = _row(db, conversation_id)
    assert (row["state"], row["answers"]) == (QUESTION_STATES[1], ["Fix and flip"])


def test_a_redelivered_message_is_applied_once(db):
    conversation_id = _open(db)
    mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)
    assert _reply(db, "m-1", "Fix and flip") == "answered"
    mark_prompted(db, conversation_id, QUESTION_STATES[1], now=NOW)
    assert _reply(db, "m-1", "Fix and flip") == "duplicate"
    assert _row(db, conversation_id)["answers"] == ["Fix and flip"]


def test_three_answers_then_a_pick_store_the_chosen_slot(db):
    conversation_id = _open(db)
    _answer_all(db, conversation_id)
    assert _row(db, conversation_id)["state"] == SLOT_CHOICE
    assert mark_prompted(db, conversation_id, SLOT_CHOICE, now=NOW, offered_slots=SLOTS)
    assert _reply(db, "m-pick", "2") == "slot_chosen"
    row = _row(db, conversation_id)
    assert (row["state"], row["chosen_slot_start"], row["answers"]) == (SLOT_CHOSEN, SLOTS[1], list(ANSWERS))
    assert row["closed_at"] is None


def test_an_unclear_pick_queues_the_offer_again_and_closes_after_the_limit(db):
    conversation_id = _open(db)
    _answer_all(db, conversation_id)
    for attempt in range(MAX_SLOT_REPROMPTS):
        mark_prompted(db, conversation_id, SLOT_CHOICE, now=NOW, offered_slots=SLOTS)
        assert _reply(db, f"m-unclear-{attempt}", "whenever works") == "reprompt"
        row = _row(db, conversation_id)
        assert (row["prompted_state"], row["slot_reprompts"]) == (None, attempt + 1)
    mark_prompted(db, conversation_id, SLOT_CHOICE, now=NOW, offered_slots=SLOTS)
    assert _reply(db, "m-unclear-last", "whenever works") == "unclear_slot"
    row = _row(db, conversation_id)
    assert (row["state"], row["closed_reason"]) == (CLOSED, CLOSED_UNCLEAR_SLOT) and row["closed_at"] is not None


def test_a_stop_reply_closes_the_conversation(db):
    conversation_id = _open(db)
    mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)
    assert _reply(db, "m-stop", "STOP") == "opted_out"
    row = _row(db, conversation_id)
    assert (row["state"], row["closed_reason"], row["answers"]) == (CLOSED, CLOSED_OPTED_OUT, [])
    [opt_out] = db.opt_outs  # written to the opt-out lists too, so it stops every channel
    assert (opt_out["phone"], opt_out["channel"].value) == (PHONE, "sms")
    assert opt_out["source_ref"] == f"after_hours_conversation:{conversation_id}"
    assert _reply(db, "m-after", "actually, fix and flip") == "no_conversation"


def test_a_reply_after_the_conversation_expired_or_from_an_unknown_sender_finds_nothing(db):
    conversation_id = _open(db)
    mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)
    assert _reply(db, "m-late", "Fix and flip", minutes=CONVERSATION_TTL_HOURS * 60 + 1) == "no_conversation"
    assert _reply(db, "m-other", "Fix and flip", phone="+18135550111") == "no_conversation"
    assert record_reply(db, message_id="m-nobody", body="hi", now=NOW) == "no_conversation"


def test_a_reply_matched_by_phone_keeps_the_ghl_contact_for_later_replies(db):
    conversation_id = _open(db)
    mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)
    assert _reply(db, "m-1", "Fix and flip", phone=PHONE, ghl_contact_id=CONTACT_ID) == "answered"
    mark_prompted(db, conversation_id, QUESTION_STATES[1], now=NOW)
    assert _reply(db, "m-2", "Tampa", ghl_contact_id=CONTACT_ID) == "answered"


def test_a_late_send_never_marks_an_earlier_question_as_asked(db):
    conversation_id = _open(db)
    mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)
    _reply(db, "m-1", "Fix and flip")
    assert not mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)


# --- opened by the voicemail intake, advanced by the reply webhook ---

VOICEMAIL = {"phone": "(813) 555-0199", "first_name": "Mike Rivera", "call_id": "vm-1", "message": "Call me back.",
             "received_at": "2026-10-21T01:00:00Z"}


def _conversations(db):
    return db.execute(text("SELECT inquiry_key, channel, first_name, state FROM lending.after_hours_conversations "
                           "WHERE contact_key = :p"), {"p": PHONE}).all()


@pytest.fixture
def enabled(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_after_hours_conversation_enabled", True, raising=False)


def _consent(db):
    db.execute(text("INSERT INTO lending.text_consents (phone, source) VALUES (:p, 'web_form')"), {"p": PHONE})


def test_a_voicemail_from_a_consented_caller_opens_a_text_conversation(db, enabled):
    from src.lending.after_hours_intake import record_intake

    _consent(db)
    assert record_intake(db, VOICEMAIL, now=NOW)
    assert [tuple(row) for row in _conversations(db)] == [
        ("after_hours:ghl_voicemail:vm-1", CHANNEL_SMS, "Mike", QUESTION_STATES[0])]
    assert not record_intake(db, VOICEMAIL, now=NOW + timedelta(minutes=1))
    assert len(_conversations(db)) == 1


def test_no_conversation_without_text_consent_or_while_the_setting_is_off(db, monkeypatch):
    from config.settings import get_settings
    from src.lending.after_hours_intake import record_intake

    _consent(db)
    assert record_intake(db, VOICEMAIL, now=NOW)  # setting off (default)
    assert _conversations(db) == []
    monkeypatch.setattr(get_settings(), "lending_after_hours_conversation_enabled", True, raising=False)
    db.execute(text("DELETE FROM lending.text_consents WHERE phone = :p"), {"p": PHONE})
    assert record_intake(db, {**VOICEMAIL, "call_id": "vm-2"}, now=NOW)
    assert _conversations(db) == []


@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.lending import reply_webhook

    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(reply_webhook, "slack_poster", lambda: None)
    app = FastAPI()
    app.include_router(reply_webhook.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def _inbound(message_id, body):
    return {"messageId": message_id, "direction": "inbound", "body": body, "contactId": CONTACT_ID, "phone": PHONE}


def test_the_reply_webhook_advances_the_open_conversation(db, client, enabled, night_responder_send_passes):
    conversation_id = _open(db, now=datetime.now(timezone.utc))
    mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)
    response = client.post("/webhooks/lending/ghl-reply", headers=HEADERS, json=_inbound("m-1", "Fix and flip"))
    assert response.status_code == 200 and night_responder_send_passes == ["reply"]  # the next question goes at once
    row = _row(db, conversation_id)
    assert (row["state"], row["answers"], row["ghl_contact_id"]) == (QUESTION_STATES[1], ["Fix and flip"], CONTACT_ID)


def test_the_reply_webhook_leaves_conversations_alone_while_off_and_for_outbound_messages(db, client, monkeypatch):
    from config.settings import get_settings

    conversation_id = _open(db, now=datetime.now(timezone.utc))
    mark_prompted(db, conversation_id, QUESTION_STATES[0], now=NOW)
    client.post("/webhooks/lending/ghl-reply", headers=HEADERS, json=_inbound("m-1", "Fix and flip"))
    assert _row(db, conversation_id)["answers"] == []
    monkeypatch.setattr(get_settings(), "lending_after_hours_conversation_enabled", True, raising=False)
    client.post("/webhooks/lending/ghl-reply", headers=HEADERS,
                json={**_inbound("m-2", "Thanks, we will call you."), "direction": "outbound"})
    assert _row(db, conversation_id)["answers"] == []
