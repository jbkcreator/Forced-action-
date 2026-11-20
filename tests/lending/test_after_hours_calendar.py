"""Night responder calendar: open morning slots are read from the calendar, and the picked slot is booked on it."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import text

from config.lending_after_hours_conversation import (BOOKED, CHANNEL_SMS, CLOSED, CLOSED_OPTED_OUT, SLOT_CHOICE,
                                                     SLOT_CHOSEN)
from src.lending import after_hours_calendar
from src.lending.after_hours_calendar import BookableSlotSource, calendar_hooks, open_slots
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


class OpeningHoursCalendar(FakeCalendar):
    """A calendar with booking rules, as GHL has: it accepts bookings only from ``opens`` on, whatever its events
    say, and lists those times through ``get_free_slots``."""

    def __init__(self, opens: datetime, **fields: Any) -> None:
        super().__init__(**fields)
        self.opens = opens
        self.free_slot_reads: list[tuple[datetime, datetime]] = []

    def get_free_slots(self, *, calendar_id: str, start: datetime, end: datetime,
                       timezone_name: str) -> list[datetime]:
        self.free_slot_reads.append((start, end))
        first = max(start, self.opens)
        busy = self.get_busy(calendar_id=calendar_id, start=first, end=end)
        return open_slots(busy, first, end)


def test_slots_come_from_the_calendars_bookable_times_when_it_lists_them():
    # Opens at 10:00 on an empty morning: the gaps between events would offer 9:00 and 9:30, which GHL refuses.
    calendar = OpeningHoursCalendar(opens=THURSDAY_9AM + timedelta(hours=1))
    assert isinstance(calendar, BookableSlotSource) and not isinstance(FakeCalendar(), BookableSlotSource)
    find_slots, _ = calendar_hooks(db=None, client=calendar, calendar_id=CALENDAR_ID)
    assert find_slots(THURSDAY_9AM, NOON, EVENING)[0] == THURSDAY_9AM + timedelta(hours=1)
    assert THURSDAY_9AM not in find_slots(THURSDAY_9AM, NOON, EVENING)


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


@pytest.fixture
def booking(db) -> int:
    """A conversation waiting for its booking (the lead picked a slot): the booker books only for one of these."""
    return db.execute(text("""
        INSERT INTO lending.after_hours_conversations (inquiry_key, source, channel, contact_key, phone, state,
                                                       expires_at)
        VALUES ('after_hours:test:booking', 'test', 'sms', :p, :p, :state, :expires) RETURNING id
    """), {"p": PHONE, "state": SLOT_CHOSEN, "expires": datetime.now(timezone.utc) + timedelta(days=1)}).scalar()


def _request(conversation_id: int, **overrides: Any) -> BookingRequest:
    fields = {"conversation_id": conversation_id, "slot_start": THURSDAY_9AM, "channel": CHANNEL_SMS,
              "phone": PHONE, "email": None, "first_name": "Dana", "ghl_contact_id": None,
              "answers": ("Rental", "Tampa", "200k"), **overrides}
    return BookingRequest(**fields)


def test_the_booking_lands_on_the_calendar_under_the_contacts_email_and_moves_the_card(db, booking):
    calendar = FakeCalendar()
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    outcome = book(_request(booking))
    [event] = calendar.events.values()
    assert (outcome.status, outcome.booking_ref) == ("booked", event.event_id)
    assert (event.start, event.end, event.attendee_email) == (THURSDAY_9AM, THURSDAY_9AM + HALF_HOUR, EMAIL)
    assert event.summary == "Next Deal Lending call: Dana"
    [move] = db.card_moves
    assert (move["phone"], move["email"]) == (PHONE, EMAIL)


def test_a_slot_taken_since_the_offer_is_reported_and_nothing_is_booked(db, booking):
    calendar = FakeCalendar(busy=[_busy(0, 30)])
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    assert book(_request(booking)).status == "slot_taken"
    assert calendar.events == {} and db.card_moves == []


def test_a_slot_the_calendar_no_longer_accepts_is_reported_taken(db, booking):
    # Offered while bookable, then the calendar's own rules took it away (opening moved to 10:00).
    calendar = OpeningHoursCalendar(opens=THURSDAY_9AM + timedelta(hours=1))
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    assert book(_request(booking)).status == "slot_taken"
    assert calendar.events == {}
    # The recheck reads the slot's whole day: a free-slot list is built per day.
    assert calendar.free_slot_reads[-1] == (THURSDAY_9AM, datetime(2026, 10, 23, 4, 0, tzinfo=timezone.utc))


def test_a_lead_with_no_email_or_a_suppressed_one_is_refused(db, booking, monkeypatch):
    calendar = FakeCalendar()
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    monkeypatch.setattr(after_hours_calendar, "is_email_suppressed", lambda db, email: True)
    assert book(_request(booking)).status == "refused"
    monkeypatch.setattr(after_hours_calendar, "is_email_suppressed", lambda db, email: False)
    db.execute(text("UPDATE lending.contacts SET email = NULL WHERE phone = :p"), {"p": PHONE})
    assert book(_request(booking)).status == "refused"
    assert calendar.events == {} and db.card_moves == []
    assert book(_request(booking, email="own@example.com")).status == "booked"  # the conversation's own address


def test_no_booking_once_the_lead_has_withdrawn_text_consent_or_the_conversation_was_closed(db, booking):
    calendar = FakeCalendar()
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    db.execute(text("UPDATE lending.text_consents SET revoked_at = now() WHERE phone = :p"), {"p": PHONE})
    assert book(_request(booking)).status == "refused"
    db.execute(text("UPDATE lending.text_consents SET revoked_at = NULL WHERE phone = :p"), {"p": PHONE})
    db.execute(text("UPDATE lending.after_hours_conversations SET state = 'closed' WHERE id = :id"), {"id": booking})
    assert book(_request(booking)).status == "refused"
    assert calendar.events == {} and db.card_moves == []


class ContactCalendar(FakeCalendar):
    """A calendar that books against a contact id, as GHL does."""

    def create_event(self, *, contact_id: str, **fields: Any):
        self.contact_ids = [*getattr(self, "contact_ids", []), contact_id]
        return super().create_event(**fields)


def test_with_a_contact_resolver_the_booking_is_made_against_the_leads_contact_even_with_no_email(db, booking):
    db.execute(text("UPDATE lending.contacts SET email = NULL WHERE phone = :p"), {"p": PHONE})
    calendar, asked = ContactCalendar(), []

    def resolver(request: BookingRequest, email: Any) -> Any:
        asked.append((request.phone, email))
        return request.ghl_contact_id or "contact-from-lookup"

    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID, contact_resolver=resolver)
    assert book(_request(booking)).status == "booked"
    assert book(_request(booking, slot_start=THURSDAY_9AM + HALF_HOUR,
                         ghl_contact_id="contact-from-reply")).status == "booked"
    assert calendar.contact_ids == ["contact-from-lookup", "contact-from-reply"] and asked[0] == (PHONE, None)
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID, contact_resolver=lambda *_: None)
    assert book(_request(booking, slot_start=THURSDAY_9AM + HALF_HOUR * 2)).status == "refused"


class RefusingCalendar(FakeCalendar):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    def create_event(self, **fields: Any):
        raise self.error


def test_a_calendar_that_says_no_is_a_refusal_and_one_that_does_not_answer_raises(db, booking):
    _, book = calendar_hooks(db, client=RefusingCalendar(CalendarUnavailable("HTTP 422")), calendar_id=CALENDAR_ID)
    assert book(_request(booking)).status == "refused" and db.card_moves == []
    _, book = calendar_hooks(db, client=RefusingCalendar(CalendarOutcomeUnknown("no answer")),
                             calendar_id=CALENDAR_ID)
    with pytest.raises(CalendarOutcomeUnknown):
        book(_request(booking))
    assert db.card_moves == []


def test_a_failed_card_move_never_undoes_the_booking(db, booking, monkeypatch):
    def broken(**fields: Any) -> bool:
        raise RuntimeError("GHL down")

    monkeypatch.setattr("src.services.calendar.ghl_pipeline.push_booking_to_booked_stage", broken)
    calendar = FakeCalendar()
    _, book = calendar_hooks(db, client=calendar, calendar_id=CALENDAR_ID)
    assert book(_request(booking)).status == "booked" and len(calendar.events) == 1


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def __call__(self, phone: str, body: str, first_name: Any) -> str:
        self.sent.append(body)
        return f"msg-{len(self.sent)}"


def _night_lead(db, calendar):
    """A lead opened at 7 pm who answers the three questions and is offered slots; returns the pass and ids."""
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
    return run_pass, sender, conversation_id


def _row(db, conversation_id):
    return db.execute(text("SELECT state, booking_ref, closed_reason, sending_claimed_at "
                           "FROM lending.after_hours_conversations WHERE id = :id"), {"id": conversation_id}).one()


def test_a_night_lead_goes_from_three_answers_to_a_booking_on_the_calendar(db):
    calendar = FakeCalendar(busy=[_busy(0, 30)])  # 9:00 is taken, so the offer starts at 9:30
    run_pass, sender, conversation_id = _night_lead(db, calendar)
    assert "1) Thu Oct 22, 9:30 AM ET" in sender.sent[-1] and "3) Thu Oct 22, 10:30 AM ET" in sender.sent[-1]
    assert record_reply(db, message_id="pick", body="1", phone=PHONE, now=EVENING) == "slot_chosen"
    assert run_pass() == {"booked": 1, "sent": 1}
    [event] = calendar.events.values()
    row = _row(db, conversation_id)
    assert (row.state, row.booking_ref, row.closed_reason) == (BOOKED, event.event_id, None) and row.state != CLOSED
    assert event.start == THURSDAY_9AM + HALF_HOUR and "9:30 AM ET" in sender.sent[-1]


def test_a_stop_after_the_pick_and_before_the_booking_means_no_booking(db):
    calendar = FakeCalendar()
    run_pass, sender, conversation_id = _night_lead(db, calendar)
    assert record_reply(db, message_id="pick", body="2", phone=PHONE, now=EVENING) == "slot_chosen"
    # The stop arrives before the next pass reaches the booking.
    assert record_reply(db, message_id="stop", body="STOP", phone=PHONE, now=EVENING) == "opted_out"
    assert (_row(db, conversation_id).state, _row(db, conversation_id).closed_reason) == (CLOSED, CLOSED_OPTED_OUT)
    assert [opt_out["phone"] for opt_out in db.opt_outs] == [PHONE]
    sent_before = len(sender.sent)
    assert run_pass() == {}
    assert calendar.events == {} and len(sender.sent) == sent_before


class StopDuringBookingCalendar(FakeCalendar):
    """The lead's stop request is applied while the appointment is being made."""

    def __init__(self, db) -> None:
        super().__init__()
        self.db = db

    def create_event(self, **fields: Any):
        event = super().create_event(**fields)
        assert record_reply(self.db, message_id="stop", body="STOP", phone=PHONE, now=EVENING) == "opted_out"
        self.db.commit()  # the reply webhook's own transaction, committed while the booking is in flight
        return event


def test_an_appointment_made_while_the_stop_arrived_is_cancelled_and_nothing_is_confirmed(db):
    calendar = StopDuringBookingCalendar(db)
    run_pass, sender, conversation_id = _night_lead(db, calendar)
    assert record_reply(db, message_id="pick", body="1", phone=PHONE, now=EVENING) == "slot_chosen"
    sent_before = len(sender.sent)
    # The stop released the booking's claim, so the booker's answer is never written as a booking.
    assert run_pass() == {"claim_lost": 1}
    assert calendar.cancelled == [calendar.created[0].event_id]
    row = _row(db, conversation_id)
    assert (row.state, row.closed_reason, row.booking_ref) == (CLOSED, CLOSED_OPTED_OUT, None)
    assert len(sender.sent) == sent_before and db.card_moves == []


def test_a_slot_that_disappears_after_the_offer_brings_a_fresh_offer_of_bookable_times(db):
    calendar = OpeningHoursCalendar(opens=THURSDAY_9AM)
    run_pass, sender, conversation_id = _night_lead(db, calendar)
    assert "1) Thu Oct 22, 9:00 AM ET" in sender.sent[-1]
    calendar.opens = THURSDAY_9AM + timedelta(hours=1)  # the calendar stops taking 9:00 and 9:30
    assert record_reply(db, message_id="pick", body="1", phone=PHONE, now=EVENING) == "slot_chosen"
    assert run_pass() == {"slot_taken": 1, "sent": 1}
    assert calendar.events == {} and _row(db, conversation_id).state == SLOT_CHOICE
    assert "1) Thu Oct 22, 10:00 AM ET" in sender.sent[-1] and "9:00 AM" not in sender.sent[-1]
