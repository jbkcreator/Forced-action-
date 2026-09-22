"""FakeCalendar behaviour and the three public scheduling operations."""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from src.services.calendar import (
    BusyBlock,
    CalendarClient,
    FakeCalendar,
    FakeCalendarError,
    Slot,
    book,
    get_slots,
    reschedule,
)

ET = ZoneInfo("America/New_York")

NOW = datetime(2026, 6, 13, 9, tzinfo=ET)
WINDOW_START = datetime(2026, 6, 15, 0, tzinfo=ET)
WINDOW_END = WINDOW_START + timedelta(days=1)
CALENDAR_ID = "client@example.invalid"
ATTENDEE = "borrower@example.invalid"


def _slot(hour: int, minute: int = 0, minutes: int = 30) -> Slot:
    start = datetime(2026, 6, 15, hour, minute, tzinfo=ET)
    return Slot(start=start, end=start + timedelta(minutes=minutes))


def _allow_all():
    return patch(
        "src.agents.fa_max.tool_registry.check_suppression",
        return_value={"suppressed": False, "reason": None},
    )


def _suppress(reason: str):
    return patch(
        "src.agents.fa_max.tool_registry.check_suppression",
        return_value={"suppressed": True, "reason": reason},
    )


class TestFakeCalendar:
    def test_satisfies_the_client_protocol(self):
        assert isinstance(FakeCalendar(), CalendarClient)

    def test_seeded_busy_is_returned_for_an_overlapping_window(self):
        block = BusyBlock(start=_slot(12).start, end=_slot(12).end)
        calendar = FakeCalendar(busy=[block])
        assert calendar.get_busy(
            calendar_id=CALENDAR_ID, start=WINDOW_START, end=WINDOW_END
        ) == [block]

    def test_busy_outside_the_window_is_not_returned(self):
        block = BusyBlock(start=_slot(12).start, end=_slot(12).end)
        calendar = FakeCalendar(busy=[block])
        assert (
            calendar.get_busy(
                calendar_id=CALENDAR_ID,
                start=WINDOW_START + timedelta(days=5),
                end=WINDOW_END + timedelta(days=5),
            )
            == []
        )

    def test_created_event_immediately_shows_as_busy(self):
        calendar = FakeCalendar()
        slot = _slot(11)
        calendar.create_event(
            calendar_id=CALENDAR_ID,
            start=slot.start,
            end=slot.end,
            summary="Intro call",
            attendee_email=ATTENDEE,
        )
        busy = calendar.get_busy(
            calendar_id=CALENDAR_ID, start=WINDOW_START, end=WINDOW_END
        )
        assert [(b.start, b.end) for b in busy] == [(slot.start, slot.end)]

    def test_double_booking_is_allowed_because_the_provider_allows_it(self):
        calendar = FakeCalendar()
        slot = _slot(11)
        for _ in range(2):
            calendar.create_event(
                calendar_id=CALENDAR_ID,
                start=slot.start,
                end=slot.end,
                summary="Intro call",
                attendee_email=ATTENDEE,
            )
        assert len(calendar.created) == 2

    def test_failing_attendee_raises(self):
        calendar = FakeCalendar(fail_attendees={ATTENDEE})
        slot = _slot(11)
        with pytest.raises(FakeCalendarError):
            calendar.create_event(
                calendar_id=CALENDAR_ID,
                start=slot.start,
                end=slot.end,
                summary="Intro call",
                attendee_email=ATTENDEE,
            )

    def test_cancel_clears_the_busy_span(self):
        calendar = FakeCalendar()
        slot = _slot(11)
        event = calendar.create_event(
            calendar_id=CALENDAR_ID,
            start=slot.start,
            end=slot.end,
            summary="Intro call",
            attendee_email=ATTENDEE,
        )
        calendar.cancel_event(calendar_id=CALENDAR_ID, event_id=event.event_id)

        assert calendar.cancelled == [event.event_id]
        assert calendar.get_event(
            calendar_id=CALENDAR_ID, event_id=event.event_id
        ).status == "cancelled"
        assert (
            calendar.get_busy(calendar_id=CALENDAR_ID, start=WINDOW_START, end=WINDOW_END)
            == []
        )

    def test_cancelling_an_unknown_event_is_silent(self):
        FakeCalendar().cancel_event(calendar_id=CALENDAR_ID, event_id="nope")


class TestGetSlots:
    def test_returns_slots_from_an_empty_calendar(self):
        slots = get_slots(
            client=FakeCalendar(),
            calendar_id=CALENDAR_ID,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            now=NOW,
        )
        assert slots
        assert slots[0].start.astimezone(ET).hour == 9

    def test_existing_booking_removes_its_slot(self):
        taken = _slot(11)
        calendar = FakeCalendar(busy=[BusyBlock(start=taken.start, end=taken.end)])
        starts = {
            slot.start
            for slot in get_slots(
                client=calendar,
                calendar_id=CALENDAR_ID,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
                now=NOW,
            )
        }
        assert taken.start not in starts


class TestBook:
    def test_books_an_open_slot(self):
        calendar = FakeCalendar()
        with _allow_all():
            result = book(
                client=calendar,
                session=None,
                calendar_id=CALENDAR_ID,
                slot=_slot(11),
                attendee_email=ATTENDEE,
                topic="Intro call",
            )
        assert result.booked
        assert result.event is not None
        assert result.event.attendee_email == ATTENDEE
        assert len(calendar.created) == 1

    def test_suppressed_recipient_is_refused_before_anything_is_created(self):
        calendar = FakeCalendar()
        with _suppress("opted_out"):
            result = book(
                client=calendar,
                session=None,
                calendar_id=CALENDAR_ID,
                slot=_slot(11),
                attendee_email=ATTENDEE,
                topic="Intro call",
            )
        assert not result.booked
        assert result.reason == "suppressed"
        assert result.detail == "opted_out"
        assert calendar.created == [], "a suppressed contact must not receive an invite"

    def test_slot_taken_since_it_was_offered_is_refused(self):
        slot = _slot(11)
        calendar = FakeCalendar(busy=[BusyBlock(start=slot.start, end=slot.end)])
        with _allow_all():
            result = book(
                client=calendar,
                session=None,
                calendar_id=CALENDAR_ID,
                slot=slot,
                attendee_email=ATTENDEE,
                topic="Intro call",
            )
        assert not result.booked
        assert result.reason == "slot_taken"
        assert calendar.created == []

    def test_second_booking_of_the_same_slot_is_refused(self):
        calendar = FakeCalendar()
        slot = _slot(11)
        with _allow_all():
            first = book(
                client=calendar, session=None, calendar_id=CALENDAR_ID, slot=slot,
                attendee_email=ATTENDEE, topic="Intro call",
            )
            second = book(
                client=calendar, session=None, calendar_id=CALENDAR_ID, slot=slot,
                attendee_email="other@example.invalid", topic="Intro call",
            )
        assert first.booked
        assert not second.booked
        assert second.reason == "slot_taken"

    def test_provider_failure_propagates_rather_than_returning_a_result(self):
        calendar = FakeCalendar(fail_attendees={ATTENDEE})
        with _allow_all(), pytest.raises(FakeCalendarError):
            book(
                client=calendar,
                session=None,
                calendar_id=CALENDAR_ID,
                slot=_slot(11),
                attendee_email=ATTENDEE,
                topic="Intro call",
            )


class TestReschedule:
    def test_surfaces_the_booking_and_alternatives_without_changing_anything(self):
        calendar = FakeCalendar()
        with _allow_all():
            booked = book(
                client=calendar, session=None, calendar_id=CALENDAR_ID, slot=_slot(11),
                attendee_email=ATTENDEE, topic="Intro call",
            )

        request = reschedule(
            client=calendar,
            calendar_id=CALENDAR_ID,
            event_id=booked.event.event_id,
            now=NOW,
        )

        assert request.event_id == booked.event.event_id
        assert request.current.status == "confirmed", "v1 must not cancel anything"
        assert request.alternatives
        assert calendar.cancelled == []

    def test_alternatives_exclude_the_existing_booking(self):
        calendar = FakeCalendar()
        slot = _slot(11)
        with _allow_all():
            booked = book(
                client=calendar, session=None, calendar_id=CALENDAR_ID, slot=slot,
                attendee_email=ATTENDEE, topic="Intro call",
            )
        request = reschedule(
            client=calendar,
            calendar_id=CALENDAR_ID,
            event_id=booked.event.event_id,
            now=NOW,
        )
        assert slot.start not in {alt.start for alt in request.alternatives}

    def test_unknown_event_still_returns_alternatives(self):
        request = reschedule(
            client=FakeCalendar(), calendar_id=CALENDAR_ID, event_id="nope", now=NOW,
        )
        assert request.current is None
        assert request.alternatives
