"""Night responder calendar: open morning slots are read from the calendar, and the picked slot is booked on it."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import text

from config.lending_after_hours_conversation import BOOKED, CHANNEL_SMS, CLOSED
from src.lending import after_hours_calendar
from src.lending.after_hours_calendar import calendar_hooks, open_slots
from src.lending.after_hours_conversation import open_conversation, record_reply
from src.lending.after_hours_responder import BookingRequest, process_due
from src.services.calendar.availability import BusyBlock
from src.services.calendar.client import CalendarOutcomeUnknown, CalendarUnavailable
from src.services.calendar.fakes import FakeCalendar

PHONE = "+18135550188"
EMAIL = "dana@example.com"
CALENDAR_ID = "morning-calendar"
EVENING = datetime(2026, 10, 21, 23, 0, tzinfo=timezone.utc)        # Wed 7:00 pm ET
THURSDAY_9AM = datetime(2026, 10, 22, 13, 0, tzinfo=timezone.utc)   # Thu 9:00 am ET
NOON = THURSDAY_9AM + timedelta(hours=3)
HALF_HOUR = timedelta(minutes=30)


def _busy(start_offset_minutes: int, minutes: int) -> BusyBlock:
    start = THURSDAY_9AM + timedelta(minutes=start_offset_minutes)
    return BusyBlock(start=start, end=start + timedelta(minutes=minutes))


def test_open_slots_sit_on_the_half_hour_and_skip_anything_busy():
    assert open_slots([], THURSDAY_9AM, NOON) == [THURSDAY_9AM + HALF_HOUR * position for position in range(6)]
    # 9:45-10:15 is taken: 9:30 and 10:00 both overlap it.
    assert open_slots([_busy(45, 30)], THURSDAY_9AM, THURSDAY_9AM + timedelta(hours=2)) == [
        THURSDAY_9AM, THURSDAY_9AM + HALF_HOUR * 3]
    # A window that opens at 9:10 starts at 9:30; a slot must end inside the window.
    late_start = THURSDAY_9AM + timedelta(minutes=10)
    assert open_slots([], late_start, THURSDAY_9AM + timedelta(minutes=75)) == [THURSDAY_9AM + HALF_HOUR]
    assert open_slots([_busy(0, 180)], THURSDAY_9AM, NOON) == []


def test_no_calendar_configured_means_no_hooks(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_after_hours_calendar_id", None, raising=False)
    assert calendar_hooks(db=None) == (None, None)


@pytest.fixture
def db(conversations_db, monkeypatch):
    conversations_db.execute(text("DELETE FROM lending.after_hours_conversations WHERE contact_key = :p"),
                             {"p": PHONE})
    conversations_db.execute(text("DELETE FROM lending.text_consents WHERE phone = :p"), {"p": PHONE})
    conversations_db.execute(text("DELETE FROM lending.contacts WHERE phone = :p"), {"p": PHONE})
    conversations_db.execute(text("INSERT INTO lending.text_consents (phone, source) VALUES (:p, 'web_form')"),
                             {"p": PHONE})
    conversations_db.execute(text("INSERT INTO lending.contacts (phone, email) VALUES (:p, :e)"),
                             {"p": PHONE, "e": EMAIL})
    conversations_db.card_moves = []
    monkeypatch.setattr(after_hours_calendar, "is_email_suppressed", lambda db, email: False)
    monkeypatch.setattr("src.services.calendar.ghl_pipeline.push_booking_to_booked_stage",
                        lambda **fields: conversations_db.card_moves.append(fields) or True)
    return conversations_db


def _request(**overrides: Any) -> BookingRequest:
    fields = {"conversation_id": 1, "slot_start": THURSDAY_9AM, "channel": CHANNEL_SMS, "phone": PHONE,
              "email": None, "first_name": "Dana", "ghl_contact_id": None, "answers": ("Rental", "Tampa", "200k"),
              **overrides}
    return BookingRequest(**fields)


def test_the_booking_lands_on_the_calendar_under_the_contacts_email_and_moves_the_card(db):
    calendar = FakeCalendar()
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    outcome = book(_request())
    [event] = calendar.events.values()
    assert (outcome.status, outcome.booking_ref) == ("booked", event.event_id)
    assert (event.start, event.end, event.attendee_email) == (THURSDAY_9AM, THURSDAY_9AM + HALF_HOUR, EMAIL)
    assert event.summary == "Next Deal Lending call: Dana"
    [move] = db.card_moves
    assert (move["phone"], move["email"]) == (PHONE, EMAIL)


def test_a_slot_taken_since_the_offer_is_reported_and_nothing_is_booked(db):
    calendar = FakeCalendar(busy=[_busy(0, 30)])
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    assert book(_request()).status == "slot_taken"
    assert calendar.events == {} and db.card_moves == []


def test_a_lead_with_no_email_or_a_suppressed_one_is_refused(db, monkeypatch):
    calendar = FakeCalendar()
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    monkeypatch.setattr(after_hours_calendar, "is_email_suppressed", lambda db, email: True)
    assert book(_request()).status == "refused"
    monkeypatch.setattr(after_hours_calendar, "is_email_suppressed", lambda db, email: False)
    db.execute(text("UPDATE lending.contacts SET email = NULL WHERE phone = :p"), {"p": PHONE})
    assert book(_request()).status == "refused"
    assert calendar.events == {} and db.card_moves == []
    assert book(_request(email="own@example.com")).status == "booked"  # the conversation's own address is used


class ContactCalendar(FakeCalendar):
    """A calendar that books against a contact id, as GHL does."""

    def create_event(self, *, contact_id: str, **fields: Any):
        self.contact_ids = [*getattr(self, "contact_ids", []), contact_id]
        return super().create_event(**fields)


def test_with_a_contact_resolver_the_booking_is_made_against_the_leads_contact_even_with_no_email(db):
    db.execute(text("UPDATE lending.contacts SET email = NULL WHERE phone = :p"), {"p": PHONE})
    calendar, asked = ContactCalendar(), []

    def resolver(request: BookingRequest, email: Any) -> Any:
        asked.append((request.phone, email))
        return request.ghl_contact_id or "contact-from-lookup"

    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID, contact_resolver=resolver)
    assert book(_request()).status == "booked"
    assert book(_request(slot_start=THURSDAY_9AM + HALF_HOUR, ghl_contact_id="contact-from-reply")).status == "booked"
    assert calendar.contact_ids == ["contact-from-lookup", "contact-from-reply"] and asked[0] == (PHONE, None)
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID, contact_resolver=lambda *_: None)
    assert book(_request(slot_start=THURSDAY_9AM + HALF_HOUR * 2)).status == "refused"


class RefusingCalendar(FakeCalendar):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    def create_event(self, **fields: Any):
        raise self.error


def test_a_calendar_that_says_no_is_a_refusal_and_one_that_does_not_answer_raises(db):
    _, book = calendar_hooks(db, client=RefusingCalendar(CalendarUnavailable("HTTP 422")), calendar_id=CALENDAR_ID)
    assert book(_request()).status == "refused" and db.card_moves == []
    _, book = calendar_hooks(db, client=RefusingCalendar(CalendarOutcomeUnknown("no answer")),
                             calendar_id=CALENDAR_ID)
    with pytest.raises(CalendarOutcomeUnknown):
        book(_request())
    assert db.card_moves == []


def test_a_failed_card_move_never_undoes_the_booking(db, monkeypatch):
    def broken(**fields: Any) -> bool:
        raise RuntimeError("GHL down")

    monkeypatch.setattr("src.services.calendar.ghl_pipeline.push_booking_to_booked_stage", broken)
    calendar = FakeCalendar()
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    assert book(_request()).status == "booked" and len(calendar.events) == 1


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def __call__(self, phone: str, body: str, first_name: Any) -> str:
        self.sent.append(body)
        return f"msg-{len(self.sent)}"


def test_a_night_lead_goes_from_three_answers_to_a_booking_on_the_calendar(db):
    calendar = FakeCalendar(busy=[_busy(0, 30)])  # 9:00 is taken, so the offer starts at 9:30
    slot_finder, booker = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    sender = FakeSender()

    def run_pass() -> dict[str, int]:
        return process_due(db, text_sender=sender, email_sender=None, carrier_approved=True,
                           slot_finder=slot_finder, booker=booker, now=EVENING)

    conversation_id = open_conversation(db, inquiry_key="after_hours:ghl_voicemail:vm-9", source="ghl_voicemail",
                                        channel=CHANNEL_SMS, phone=PHONE, first_name="Dana", now=EVENING)
    for position, answer in enumerate(("Rental", "Tampa, FL", "200k in 30 days")):
        assert run_pass() == {"sent": 1}
        assert record_reply(db, message_id=f"m-{position}", body=answer, phone=PHONE, now=EVENING) == "answered"
    assert run_pass() == {"sent": 1}
    assert "1) Thu Oct 22, 9:30 AM ET" in sender.sent[-1] and "3) Thu Oct 22, 10:30 AM ET" in sender.sent[-1]
    assert record_reply(db, message_id="pick", body="1", phone=PHONE, now=EVENING) == "slot_chosen"
    assert run_pass() == {"booked": 1, "sent": 1}
    [event] = calendar.events.values()
    row = db.execute(text("SELECT state, booking_ref, closed_reason FROM lending.after_hours_conversations "
                          "WHERE id = :id"), {"id": conversation_id}).one()
    assert tuple(row) == (BOOKED, event.event_id, None) and row.state != CLOSED
    assert event.start == THURSDAY_9AM + HALF_HOUR and "9:30 AM ET" in sender.sent[-1]
