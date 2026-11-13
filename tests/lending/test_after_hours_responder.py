"""Night responder steps: each due message goes once, only when every check passes, and leaves a receipt; the
slot the lead picks is booked once and confirmed."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest
from sqlalchemy import text

from config.lending_after_hours_conversation import (BOOKED, CHANNEL_EMAIL, CHANNEL_SMS, CLOSED,
                                                     CONVERSATION_TTL_HOURS, EMAIL_SUBJECT, MAX_SEND_ATTEMPTS,
                                                     QUESTION_STATES, QUESTIONS, SLOT_CHOICE, SLOT_CHOSEN,
                                                     SLOT_OFFER, SLOT_OFFER_AFTER_TAKEN, SLOT_OFFER_AGAIN,
                                                     STALE_CLAIM_SECONDS)
from src.lending import after_hours_responder
from src.lending.after_hours_conversation import (booked_message, next_morning_window, open_conversation,
                                                  question_message, record_reply, slot_label, slot_offer_message)
from src.lending.after_hours_responder import BookingOutcome, process_due, run_send_cycle
from src.lending.ghl_sms import GhlSendRefused, GhlSmsError

PHONE = "+18135550199"
EMAIL = "mike@example.com"
NIGHT = datetime(2026, 10, 21, 1, 0, tzinfo=timezone.utc)   # 9:00 pm ET
MORNING = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)  # 8:00 am ET
DAY = datetime(2026, 10, 21, 15, 0, tzinfo=timezone.utc)    # 11:00 am ET


class FakeSender:
    """Records each send and returns a message id, or raises the error it was given."""

    def __init__(self, error: Optional[Exception] = None) -> None:
        self.sent: list[tuple[Any, ...]] = []
        self.error = error

    def __call__(self, *args: Any, **kwargs: Any) -> str:
        if self.error is not None:
            raise self.error
        self.sent.append((*args, kwargs))
        return f"msg-{len(self.sent)}"


@pytest.fixture
def db(conversations_db):
    conversations_db.execute(text("DELETE FROM lending.after_hours_conversations WHERE contact_key IN (:p, :e)"),
                             {"p": PHONE, "e": EMAIL})
    conversations_db.execute(text("DELETE FROM lending.text_consents WHERE phone = :p"), {"p": PHONE})
    conversations_db.execute(text("INSERT INTO lending.text_consents (phone, source) VALUES (:p, 'web_form')"),
                             {"p": PHONE})
    return conversations_db


def _open(db, *, opened_at=DAY, **overrides):
    fields = {"inquiry_key": "after_hours:ghl_voicemail:vm-1", "source": "ghl_voicemail", "channel": CHANNEL_SMS,
              "phone": PHONE, "first_name": "Mike", "now": opened_at, **overrides}
    return open_conversation(db, **fields)


def _row(db, conversation_id):
    return db.execute(text("SELECT * FROM lending.after_hours_conversations WHERE id = :id"),
                      {"id": conversation_id}).mappings().one()


def _pass(db, sender, *, now=DAY, carrier_approved=True, email_sender=None, slot_finder=None, booker=None):
    return process_due(db, text_sender=sender, email_sender=email_sender, carrier_approved=carrier_approved,
                       slot_finder=slot_finder, booker=booker, now=now)


def test_the_first_question_goes_by_text_once_and_leaves_a_receipt(db):
    conversation_id, sender = _open(db), FakeSender()
    assert _pass(db, sender) == {"sent": 1}
    assert sender.sent == [(PHONE, question_message(QUESTION_STATES[0], "Mike"), "Mike", {})]
    row = _row(db, conversation_id)
    assert (row["prompted_state"], row["sending_claimed_at"]) == (QUESTION_STATES[0], None)
    assert row["sent_messages"] == [{"state": QUESTION_STATES[0], "template_version": "NIGHT-Q1-SMS-v1",
                                     "status": "sent", "provider_message_id": "msg-1", "at": DAY.isoformat()}]
    assert _pass(db, sender) == {} and len(sender.sent) == 1


def test_each_answer_brings_the_next_question_and_the_third_ends_the_questions(db):
    conversation_id, sender = _open(db), FakeSender()
    for position, answer in enumerate(("Fix and flip", "Tampa, FL", "250k by the 30th")):
        assert _pass(db, sender) == {"sent": 1}
        assert record_reply(db, message_id=f"m-{position}", body=answer, phone=PHONE,
                            now=DAY + timedelta(minutes=position + 1)) == "answered"
    assert [body for _, body, _, _ in sender.sent][1:] == list(QUESTIONS[1:])
    assert _row(db, conversation_id)["state"] == SLOT_CHOICE
    assert _pass(db, sender) == {"no_slots": 1} and len(sender.sent) == 3  # no calendar: no offer is sent
    assert _row(db, conversation_id)["closed_reason"] == "no_slots"
    assert [m["template_version"] for m in _row(db, conversation_id)["sent_messages"]] == [
        "NIGHT-Q1-SMS-v1", "NIGHT-Q2-SMS-v1", "NIGHT-Q3-SMS-v1"]


def test_a_nine_pm_text_is_refused_and_goes_when_the_window_opens(db, caplog):
    conversation_id, sender = _open(db, opened_at=NIGHT), FakeSender()
    with caplog.at_level("INFO"):
        assert _pass(db, sender, now=NIGHT) == {"waiting_hours": 1}
    assert sender.sent == [] and "BLOCKED_HOURS text refused" in caplog.text
    row = _row(db, conversation_id)
    assert (row["prompted_state"], row["sending_claimed_at"], row["sent_messages"]) == (None, None, [])
    assert _pass(db, sender, now=MORNING - timedelta(minutes=1)) == {"waiting_hours": 1}
    assert _pass(db, sender, now=MORNING) == {"sent": 1}


def test_the_time_to_reply_runs_from_the_first_message_not_from_the_inquiry(db):
    conversation_id, sender = _open(db, opened_at=NIGHT), FakeSender()
    assert _row(db, conversation_id)["expires_at"] == NIGHT + timedelta(hours=CONVERSATION_TTL_HOURS)
    assert _pass(db, sender, now=MORNING) == {"sent": 1}
    assert _row(db, conversation_id)["expires_at"] == MORNING + timedelta(hours=CONVERSATION_TTL_HOURS)
    record_reply(db, message_id="m-0", body="Fix and flip", phone=PHONE, now=MORNING)
    assert _pass(db, sender, now=MORNING + timedelta(hours=1)) == {"sent": 1}
    assert _row(db, conversation_id)["expires_at"] == MORNING + timedelta(hours=CONVERSATION_TTL_HOURS)


def test_a_team_test_number_on_the_allowlist_gets_the_nine_pm_text(db, monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "qa_test_phone_allowlist", PHONE, raising=False)
    _open(db, opened_at=NIGHT)
    sender = FakeSender()
    assert _pass(db, sender, now=NIGHT) == {"sent": 1} and len(sender.sent) == 1


def test_a_text_waits_for_carrier_approval_and_for_a_configured_sender(db):
    conversation_id, sender = _open(db), FakeSender()
    assert _pass(db, sender, carrier_approved=False) == {"waiting_carrier": 1}
    assert _pass(db, None) == {"not_configured": 1}
    assert sender.sent == [] and _row(db, conversation_id)["prompted_state"] is None
    assert _pass(db, sender) == {"sent": 1}


def test_no_text_once_consent_is_gone(db):
    conversation_id, sender = _open(db), FakeSender()
    db.execute(text("UPDATE lending.text_consents SET revoked_at = now() WHERE phone = :p"), {"p": PHONE})
    assert _pass(db, sender) == {"no_consent": 1}
    row = _row(db, conversation_id)
    assert sender.sent == [] and (row["state"], row["closed_reason"]) == (CLOSED, "no_consent")


def test_a_message_carrying_a_backflip_name_is_held(db):
    conversation_id, sender = _open(db, first_name="Backflip"), FakeSender()
    assert _pass(db, sender) == {"blocked_lane": 1}
    assert sender.sent == [] and _row(db, conversation_id)["closed_reason"] == "blocked_lane"


def test_a_refused_recipient_closes_the_conversation(db):
    conversation_id = _open(db)
    assert _pass(db, FakeSender(GhlSendRefused("cold_lead"))) == {"send_refused": 1}
    assert _row(db, conversation_id)["closed_reason"] == "send_refused"


def test_a_send_with_no_clear_result_is_recorded_as_unknown_and_never_resent(db):
    conversation_id = _open(db)
    assert _pass(db, FakeSender(GhlSmsError("timeout", ambiguous=True))) == {"send_unknown": 1}
    row = _row(db, conversation_id)
    assert row["prompted_state"] == QUESTION_STATES[0] and row["sent_messages"][0]["status"] == "send_unknown"
    sender = FakeSender()
    assert _pass(db, sender) == {} and sender.sent == []


def test_a_rejected_send_is_retried_then_the_conversation_closes(db):
    conversation_id, failing = _open(db), FakeSender(GhlSmsError("HTTP 400"))
    for _ in range(MAX_SEND_ATTEMPTS - 1):
        assert _pass(db, failing) == {"retry_later": 1}
    assert _row(db, conversation_id)["send_attempts"] == MAX_SEND_ATTEMPTS - 1
    assert _pass(db, failing) == {"send_failed": 1}
    assert _row(db, conversation_id)["closed_reason"] == "send_failed"


def test_an_expired_conversation_is_closed_and_nothing_is_sent(db):
    conversation_id, sender = _open(db), FakeSender()
    assert _pass(db, sender, now=DAY + timedelta(hours=CONVERSATION_TTL_HOURS)) == {"expired": 1}
    row = _row(db, conversation_id)
    assert sender.sent == [] and (row["state"], row["closed_reason"]) == (CLOSED, "expired")


def test_a_send_that_crashed_midway_settles_as_unknown_and_is_not_sent_again(db):
    conversation_id, sender = _open(db), FakeSender()
    db.execute(text("UPDATE lending.after_hours_conversations SET sending_claimed_at = :at WHERE id = :id"),
               {"at": DAY, "id": conversation_id})
    assert _pass(db, sender, now=DAY + timedelta(seconds=STALE_CLAIM_SECONDS - 1)) == {}  # another worker has it
    assert _pass(db, sender, now=DAY + timedelta(seconds=STALE_CLAIM_SECONDS + 1)) == {}
    row = _row(db, conversation_id)
    assert sender.sent == [] and row["prompted_state"] == QUESTION_STATES[0]
    assert row["sent_messages"][0]["status"] == "send_unknown"


def test_an_email_conversation_goes_by_email_and_never_to_a_suppressed_address(db, monkeypatch):
    monkeypatch.setattr(after_hours_responder, "is_email_suppressed", lambda db, email: False)
    conversation_id = _open(db, inquiry_key="web:1", channel=CHANNEL_EMAIL, phone=None, email=EMAIL)
    texts, emails = FakeSender(), FakeSender()
    assert _pass(db, texts, email_sender=emails, carrier_approved=False) == {"sent": 1}
    assert texts.sent == [] and emails.sent == [
        (EMAIL, EMAIL_SUBJECT, question_message(QUESTION_STATES[0], "Mike"), {"phone": None, "first_name": "Mike"})]
    assert _row(db, conversation_id)["sent_messages"][0]["template_version"] == "NIGHT-Q1-EMAIL-v1"
    assert _pass(db, texts, email_sender=emails, now=DAY + timedelta(hours=CONVERSATION_TTL_HOURS)) == {"expired": 1}

    monkeypatch.setattr(after_hours_responder, "is_email_suppressed", lambda db, email: True)
    suppressed = _open(db, inquiry_key="web:2", channel=CHANNEL_EMAIL, phone=None, email=EMAIL)
    assert _pass(db, texts, email_sender=emails) == {"no_consent": 1}
    assert len(emails.sent) == 1 and _row(db, suppressed)["closed_reason"] == "no_consent"


def test_the_real_pass_does_nothing_while_off_or_during_the_emergency_halt(monkeypatch):
    from config.settings import get_settings

    def no_session():
        raise AssertionError("no database session may be opened")

    monkeypatch.setattr("src.lending.db.lending_session", no_session)
    monkeypatch.setattr(get_settings(), "lending_after_hours_conversation_enabled", False, raising=False)
    assert run_send_cycle() == {}
    monkeypatch.setattr(get_settings(), "lending_after_hours_conversation_enabled", True, raising=False)
    monkeypatch.setattr(after_hours_responder, "is_globally_halted", lambda: True)
    assert run_send_cycle() == {}


# --- the after-hours voicemail: first message at once, no second reply draft in the morning ---

VOICEMAIL = {"phone": "(813) 555-0199", "first_name": "Mike Rivera", "call_id": "vm-1", "message": "Call me back.",
             "received_at": "2026-10-21T01:00:00Z"}


@pytest.fixture
def enabled(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_after_hours_conversation_enabled", True, raising=False)


def test_the_voicemail_webhook_starts_a_send_pass_only_for_a_new_voicemail(db, enabled, monkeypatch,
                                                                             night_responder_send_passes):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pydantic import SecretStr

    from config.settings import get_settings
    from src.api import lending_ghl_router
    from src.api.deps import get_db

    db.execute(text("DELETE FROM lending.contacts WHERE phone = :p"), {"p": PHONE})
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr("s"), raising=False)
    monkeypatch.setattr(get_settings(), "lending_after_hours_intake_enabled", True, raising=False)
    app = FastAPI()
    app.include_router(lending_ghl_router.router)
    app.dependency_overrides[get_db] = lambda: db
    client = TestClient(app)
    for _ in range(2):  # the redelivery records nothing and starts nothing
        assert client.post("/webhooks/lending/after-hours-intake", headers={"X-Webhook-Secret": "s"},
                           json=VOICEMAIL).status_code == 200
    assert night_responder_send_passes == ["intake"]


def test_a_caller_the_night_responder_texted_gets_no_morning_reply_draft(db, enabled):
    from src.lending.after_hours_intake import draft_morning_replies, pins_to_call, record_intake
    from src.tasks.lending_pinned_callbacks import format_slack

    class NoDrafts:
        def enqueue(self, **kwargs: Any) -> int:
            raise AssertionError("no reply may be drafted")

    db.execute(text("DELETE FROM lending.contacts WHERE phone = :p"), {"p": PHONE})
    assert record_intake(db, VOICEMAIL, now=NIGHT)
    assert _pass(db, FakeSender(), now=MORNING) == {"sent": 1}
    pins = pins_to_call(db, MORNING + timedelta(minutes=55))
    [reply] = draft_morning_replies(db, pins, NoDrafts(), lambda first_name, transcript: "hi")
    assert reply.action_id is None and reply.night_responder_texted
    assert "texted overnight by the night responder" in format_slack([reply])


# --- after the third answer: slots on the next business morning, the booking, the confirmation ---

EVENING = datetime(2026, 10, 21, 23, 0, tzinfo=timezone.utc)        # Wed 7:00 pm ET
THURSDAY_9AM = datetime(2026, 10, 22, 13, 0, tzinfo=timezone.utc)   # Thu 9:00 am ET
OPEN_SLOTS = [THURSDAY_9AM + timedelta(minutes=30 * position) for position in range(3)]
ANSWERS = ("Fix and flip", "Tampa, FL", "250k by the 30th")


class FakeCalendar:
    """Answers the slot lookup with the slots it was given and records each booking request."""

    def __init__(self, slots=OPEN_SLOTS, status: str = "booked", error: Optional[Exception] = None) -> None:
        self.slots, self.status, self.error = list(slots), status, error
        self.lookups: list[tuple[datetime, datetime]] = []
        self.requests: list[Any] = []

    def find(self, window_start: datetime, window_end: datetime, now: datetime) -> list[datetime]:
        self.lookups.append((window_start, window_end))
        return self.slots

    def book(self, request: Any) -> BookingOutcome:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return BookingOutcome(self.status, "bk-1" if self.status == "booked" else None)


def _calendar_pass(db, sender, calendar, *, now=EVENING):
    return _pass(db, sender, now=now, slot_finder=calendar.find, booker=calendar.book)


def _answer_all(db, sender):
    """A conversation with all three questions asked and answered; the slot offer is due."""
    conversation_id = _open(db, opened_at=EVENING)
    for position, answer in enumerate(ANSWERS):
        assert _pass(db, sender, now=EVENING) == {"sent": 1}
        assert record_reply(db, message_id=f"m-{position}", body=answer, phone=PHONE, now=EVENING) == "answered"
    return conversation_id


def _offered(db, sender, calendar):
    """A conversation whose slot offer has gone out."""
    conversation_id = _answer_all(db, sender)
    assert _calendar_pass(db, sender, calendar) == {"sent": 1}
    return conversation_id


def test_the_next_business_morning_skips_the_weekend_and_never_starts_in_the_past():
    friday_9pm = datetime(2026, 10, 24, 1, 0, tzinfo=timezone.utc)
    monday = (datetime(2026, 10, 26, 13, 0, tzinfo=timezone.utc), datetime(2026, 10, 26, 16, 0, tzinfo=timezone.utc))
    assert next_morning_window(friday_9pm) == monday
    assert next_morning_window(EVENING) == (THURSDAY_9AM, THURSDAY_9AM + timedelta(hours=3))
    after_midnight = datetime(2026, 10, 22, 5, 0, tzinfo=timezone.utc)  # Thu 1:00 am ET
    assert next_morning_window(after_midnight)[0] == THURSDAY_9AM
    mid_morning = THURSDAY_9AM + timedelta(hours=1)
    assert next_morning_window(mid_morning) == (mid_morning, THURSDAY_9AM + timedelta(hours=3))
    assert next_morning_window(THURSDAY_9AM + timedelta(hours=3))[0] == THURSDAY_9AM + timedelta(days=1)
    assert slot_label(THURSDAY_9AM + timedelta(minutes=30)) == "Thu Oct 22, 9:30 AM ET"


def test_the_third_answer_brings_an_offer_of_slots_on_the_next_business_morning(db):
    later_slots = [THURSDAY_9AM + timedelta(hours=2), THURSDAY_9AM + timedelta(hours=5)]
    sender, calendar = FakeSender(), FakeCalendar(slots=[*reversed(OPEN_SLOTS), *later_slots])
    conversation_id = _offered(db, sender, calendar)
    assert calendar.lookups == [(THURSDAY_9AM, THURSDAY_9AM + timedelta(hours=3))]
    assert sender.sent[-1][1] == slot_offer_message(OPEN_SLOTS, wording=SLOT_OFFER)  # the earliest three, in order
    assert "1) Thu Oct 22, 9:00 AM ET" in sender.sent[-1][1]
    row = _row(db, conversation_id)
    assert (row["state"], row["prompted_state"]) == (SLOT_CHOICE, SLOT_CHOICE)
    assert row["offered_slots"] == [slot.isoformat() for slot in OPEN_SLOTS]
    assert row["sent_messages"][-1]["template_version"] == "NIGHT-SLOTS-SMS-v1"


def test_the_picked_slot_is_booked_once_and_confirmed_in_the_same_pass(db):
    sender, calendar = FakeSender(), FakeCalendar()
    conversation_id = _offered(db, sender, calendar)
    assert record_reply(db, message_id="pick", body="2", phone=PHONE, ghl_contact_id="c-1",
                        now=EVENING) == "slot_chosen"
    assert _calendar_pass(db, sender, calendar) == {"booked": 1, "sent": 1}
    [request] = calendar.requests
    assert (request.conversation_id, request.slot_start, request.phone, request.first_name,
            request.ghl_contact_id, request.answers) == (conversation_id, OPEN_SLOTS[1], PHONE, "Mike", "c-1",
                                                         ANSWERS)
    assert sender.sent[-1][1] == booked_message(OPEN_SLOTS[1]) and "9:30 AM ET" in sender.sent[-1][1]
    row = _row(db, conversation_id)
    assert (row["state"], row["booking_ref"], row["chosen_slot_start"]) == (BOOKED, "bk-1", OPEN_SLOTS[1])
    assert row["sent_messages"][-1]["template_version"] == "NIGHT-BOOKED-SMS-v1"
    assert _calendar_pass(db, sender, calendar) == {} and len(calendar.requests) == 1
    assert record_reply(db, message_id="later", body="thanks", phone=PHONE, now=EVENING) == "no_conversation"


@pytest.mark.parametrize("slots, with_booker", [([], True), (OPEN_SLOTS, False)])
def test_no_open_slot_or_no_calendar_to_book_on_closes_the_conversation_without_an_offer(db, slots, with_booker):
    sender, calendar = FakeSender(), FakeCalendar(slots=slots)
    conversation_id = _answer_all(db, sender)
    assert _pass(db, sender, now=EVENING, slot_finder=calendar.find,
                 booker=calendar.book if with_booker else None) == {"no_slots": 1}
    assert len(sender.sent) == 3 and _row(db, conversation_id)["closed_reason"] == "no_slots"


def test_a_reply_that_names_no_slot_gets_the_same_slots_again(db):
    sender, calendar = FakeSender(), FakeCalendar()
    conversation_id = _offered(db, sender, calendar)
    assert record_reply(db, message_id="unclear", body="what about noon?", phone=PHONE, now=EVENING) == "reprompt"
    calendar.slots = []  # the offer must not be rebuilt: the numbers keep their meaning
    assert _calendar_pass(db, sender, calendar) == {"sent": 1}
    assert sender.sent[-1][1] == slot_offer_message(OPEN_SLOTS, wording=SLOT_OFFER_AGAIN)
    assert len(calendar.lookups) == 1
    assert _row(db, conversation_id)["offered_slots"] == [slot.isoformat() for slot in OPEN_SLOTS]


def test_a_slot_taken_since_the_offer_brings_a_fresh_offer(db):
    sender, calendar = FakeSender(), FakeCalendar(status="slot_taken")
    conversation_id = _offered(db, sender, calendar)
    record_reply(db, message_id="pick", body="1", phone=PHONE, now=EVENING)
    calendar.slots = OPEN_SLOTS[1:]
    assert _calendar_pass(db, sender, calendar) == {"slot_taken": 1, "sent": 1}
    assert sender.sent[-1][1] == slot_offer_message(OPEN_SLOTS[1:], wording=SLOT_OFFER_AFTER_TAKEN)
    row = _row(db, conversation_id)
    assert (row["state"], row["prompted_state"], row["chosen_slot_start"]) == (SLOT_CHOICE, SLOT_CHOICE, None)
    assert row["offered_slots"] == [slot.isoformat() for slot in OPEN_SLOTS[1:]]


@pytest.mark.parametrize("status, error, reason", [("refused", None, "booking_refused"),
                                                   ("booked", TimeoutError(), "booking_unknown")])
def test_a_refused_or_unclear_booking_closes_the_conversation_and_is_never_tried_again(db, status, error, reason):
    sender, calendar = FakeSender(), FakeCalendar(status=status, error=error)
    conversation_id = _offered(db, sender, calendar)
    record_reply(db, message_id="pick", body="1", phone=PHONE, now=EVENING)
    assert _calendar_pass(db, sender, calendar) == {reason: 1}
    row = _row(db, conversation_id)
    assert (row["state"], row["closed_reason"], row["booking_ref"]) == (CLOSED, reason, None)
    assert _calendar_pass(db, sender, calendar) == {} and len(calendar.requests) == 1 and len(sender.sent) == 4


def test_a_booking_that_crashed_midway_is_closed_as_unknown_and_not_booked_again(db):
    sender, calendar = FakeSender(), FakeCalendar()
    conversation_id = _offered(db, sender, calendar)
    record_reply(db, message_id="pick", body="1", phone=PHONE, now=EVENING)
    db.execute(text("UPDATE lending.after_hours_conversations SET sending_claimed_at = :at WHERE id = :id"),
               {"at": EVENING, "id": conversation_id})
    assert _row(db, conversation_id)["state"] == SLOT_CHOSEN
    assert _calendar_pass(db, sender, calendar, now=EVENING + timedelta(seconds=STALE_CLAIM_SECONDS + 1)) == {}
    row = _row(db, conversation_id)
    assert calendar.requests == [] and (row["state"], row["closed_reason"]) == (CLOSED, "booking_unknown")


def test_a_booking_stands_when_its_confirmation_cannot_be_sent(db):
    sender, calendar = FakeSender(), FakeCalendar()
    conversation_id = _offered(db, sender, calendar)
    record_reply(db, message_id="pick", body="3", phone=PHONE, now=EVENING)
    db.execute(text("UPDATE lending.text_consents SET revoked_at = now() WHERE phone = :p"), {"p": PHONE})
    assert _calendar_pass(db, sender, calendar) == {"booked": 1, "no_consent": 1}
    row = _row(db, conversation_id)
    assert (row["state"], row["booking_ref"], row["closed_reason"]) == (BOOKED, "bk-1", None)
    assert row["sent_messages"][-1]["status"] == "no_consent" and len(sender.sent) == 4
    assert _calendar_pass(db, sender, calendar) == {}
