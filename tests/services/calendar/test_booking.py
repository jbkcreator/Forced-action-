"""FakeCalendar behaviour and the three public scheduling operations."""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

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


def _no_real_alert():
    """The EXCEPTIONS lane posts to a real Slack channel — never from a test."""
    return patch(
        "src.services.relay.exceptions_alert_queue.enqueue_and_attempt",
        return_value=True,
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


def _book(calendar, session, *, slot=None, attendee=ATTENDEE):
    return book(
        client=calendar,
        session=session,
        calendar_id=CALENDAR_ID,
        slot=slot or _slot(11),
        attendee_email=attendee,
        topic="Intro call",
    )


class TestBook:
    def test_books_an_open_slot(self, bookings_db):
        calendar = FakeCalendar()
        with _allow_all():
            result = _book(calendar, bookings_db)
        assert result.booked
        assert result.event.attendee_email == ATTENDEE
        assert result.booking_ref
        assert len(calendar.created) == 1

    def test_booking_is_persisted(self, bookings_db):
        calendar = FakeCalendar()
        slot = _slot(11)
        with _allow_all():
            result = _book(calendar, bookings_db, slot=slot)

        row = bookings_db.execute(
            text(
                "SELECT attendee_email, topic, status, provider_event_id, starts_at "
                "FROM fa_max_bookings WHERE booking_ref = :ref"
            ),
            {"ref": result.booking_ref},
        ).mappings().one()

        assert row["attendee_email"] == ATTENDEE
        assert row["topic"] == "Intro call"
        assert row["status"] == "confirmed"
        assert row["provider_event_id"] == result.event.event_id
        assert row["starts_at"] == slot.start

    def test_suppressed_recipient_is_refused_before_anything_is_created(self, bookings_db):
        calendar = FakeCalendar()
        with _suppress("opted_out"):
            result = _book(calendar, bookings_db)

        assert not result.booked
        assert result.reason == "suppressed"
        assert result.detail == "opted_out"
        assert result.booking_ref is None
        assert calendar.created == [], "a suppressed contact must not receive an invite"
        assert (
            bookings_db.execute(text("SELECT count(*) FROM fa_max_bookings")).scalar() == 0
        ), "a refused booking must leave no row"

    def test_slot_taken_since_it_was_offered_is_refused(self, bookings_db):
        slot = _slot(11)
        calendar = FakeCalendar(busy=[BusyBlock(start=slot.start, end=slot.end)])
        with _allow_all():
            result = _book(calendar, bookings_db, slot=slot)
        assert not result.booked
        assert result.reason == "slot_taken"
        assert calendar.created == []

    def test_second_booking_of_the_same_slot_is_refused(self, bookings_db):
        calendar = FakeCalendar()
        slot = _slot(11)
        with _allow_all():
            first = _book(calendar, bookings_db, slot=slot)
            second = _book(
                calendar, bookings_db, slot=slot, attendee="other@example.invalid"
            )
        assert first.booked
        assert not second.booked
        assert second.reason == "slot_taken"

    def test_each_booking_gets_a_distinct_reference(self, bookings_db):
        calendar = FakeCalendar()
        with _allow_all():
            first = _book(calendar, bookings_db, slot=_slot(11))
            second = _book(calendar, bookings_db, slot=_slot(14))
        assert first.booking_ref != second.booking_ref

    def test_provider_failure_propagates_rather_than_returning_a_result(self, bookings_db):
        calendar = FakeCalendar(fail_attendees={ATTENDEE})
        with _allow_all(), pytest.raises(FakeCalendarError):
            _book(calendar, bookings_db)


class TestReschedule:
    def _booked(self, calendar, session):
        with _allow_all():
            return _book(calendar, session, slot=_slot(11))

    def test_pages_exceptions_without_moving_the_booking(self, bookings_db):
        calendar = FakeCalendar()
        booked = self._booked(calendar, bookings_db)

        with _no_real_alert() as alert:
            request = reschedule(
                client=calendar,
                session=bookings_db,
                calendar_id=CALENDAR_ID,
                booking_ref=booked.booking_ref,
                now=NOW,
            )

        assert request.booking_ref == booked.booking_ref
        assert request.alerted
        assert request.current.status == "confirmed", "v1 must not cancel anything"
        assert calendar.cancelled == []
        alert.assert_called_once()

        message = alert.call_args.kwargs["message"]
        assert booked.booking_ref in message
        assert ATTENDEE in message

    def test_marks_the_booking_as_in_question(self, bookings_db):
        calendar = FakeCalendar()
        booked = self._booked(calendar, bookings_db)

        with _no_real_alert():
            reschedule(
                client=calendar, session=bookings_db, calendar_id=CALENDAR_ID,
                booking_ref=booked.booking_ref, now=NOW,
            )

        status = bookings_db.execute(
            text("SELECT status FROM fa_max_bookings WHERE booking_ref = :ref"),
            {"ref": booked.booking_ref},
        ).scalar()
        assert status == "reschedule_requested"

    def test_alternatives_exclude_the_existing_booking(self, bookings_db):
        calendar = FakeCalendar()
        booked = self._booked(calendar, bookings_db)

        with _no_real_alert():
            request = reschedule(
                client=calendar, session=bookings_db, calendar_id=CALENDAR_ID,
                booking_ref=booked.booking_ref, now=NOW,
            )
        assert request.alternatives
        assert _slot(11).start not in {alt.start for alt in request.alternatives}

    def test_a_failed_alert_still_marks_the_booking(self, bookings_db):
        calendar = FakeCalendar()
        booked = self._booked(calendar, bookings_db)

        with patch(
            "src.services.relay.exceptions_alert_queue.enqueue_and_attempt",
            side_effect=RuntimeError("slack down"),
        ):
            request = reschedule(
                client=calendar, session=bookings_db, calendar_id=CALENDAR_ID,
                booking_ref=booked.booking_ref, now=NOW,
            )

        assert not request.alerted
        status = bookings_db.execute(
            text("SELECT status FROM fa_max_bookings WHERE booking_ref = :ref"),
            {"ref": booked.booking_ref},
        ).scalar()
        assert status == "reschedule_requested", (
            "the booking is in question whether or not the page got through"
        )

    def test_unknown_booking_reference_raises(self, bookings_db):
        with pytest.raises(ValueError, match="unknown booking_ref"):
            reschedule(
                client=FakeCalendar(), session=bookings_db, calendar_id=CALENDAR_ID,
                booking_ref="does-not-exist", now=NOW,
            )


class _Result:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _RecordingSession:
    """Captures the SQL a call would issue, without a database behind it."""

    def __init__(self, select_row=None):
        self.calls: list[tuple[str, dict]] = []
        self._select_row = select_row

    def execute(self, statement, params=None):
        self.calls.append((" ".join(str(statement).split()), params or {}))
        return _Result(self._select_row)

    def statements_containing(self, fragment: str) -> list[tuple[str, dict]]:
        return [call for call in self.calls if fragment in call[0]]


class TestWrittenSqlWithoutDatabase:
    """What book() and reschedule() write, provable before the table exists.

    These assert the statements and parameters, not that the SQL is valid
    against Postgres — the bookings_db tests cover that, and skip until
    migrations/apply_fa_max_bookings.py has been run.
    """

    def test_book_inserts_the_booking(self):
        session = _RecordingSession()
        slot = _slot(11)
        with _allow_all():
            result = book(
                client=FakeCalendar(), session=session, calendar_id=CALENDAR_ID,
                slot=slot, attendee_email=ATTENDEE, topic="Intro call",
                person_id="11111111-1111-1111-1111-111111111111",
            )

        inserts = session.statements_containing("INSERT INTO fa_max_bookings")
        assert len(inserts) == 1
        params = inserts[0][1]
        assert params["booking_ref"] == result.booking_ref
        assert params["attendee_email"] == ATTENDEE
        assert params["topic"] == "Intro call"
        assert params["starts_at"] == slot.start
        assert params["ends_at"] == slot.end
        assert params["person_id"] == "11111111-1111-1111-1111-111111111111"
        assert params["provider_event_id"] == result.event.event_id

    def test_suppressed_book_writes_nothing(self):
        session = _RecordingSession()
        with _suppress("opted_out"):
            book(
                client=FakeCalendar(), session=session, calendar_id=CALENDAR_ID,
                slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
            )
        assert session.calls == []

    def test_slot_taken_writes_nothing(self):
        slot = _slot(11)
        calendar = FakeCalendar(busy=[BusyBlock(start=slot.start, end=slot.end)])
        session = _RecordingSession()
        with _allow_all():
            book(
                client=calendar, session=session, calendar_id=CALENDAR_ID, slot=slot,
                attendee_email=ATTENDEE, topic="Intro call",
            )
        assert session.calls == []

    def test_reschedule_marks_status_and_pages_exceptions(self):
        session = _RecordingSession(
            select_row={
                "provider_event_id": None,
                "attendee_email": ATTENDEE,
                "topic": "Intro call",
                "starts_at": _slot(11).start,
            }
        )
        with _no_real_alert() as alert:
            request = reschedule(
                client=FakeCalendar(), session=session, calendar_id=CALENDAR_ID,
                booking_ref="abc123", now=NOW,
            )

        updates = session.statements_containing("UPDATE fa_max_bookings")
        assert len(updates) == 1
        assert "status = 'reschedule_requested'" in updates[0][0]
        assert updates[0][1] == {"booking_ref": "abc123"}
        assert request.alerted
        assert alert.call_args.kwargs["rule"] == "calendar_reschedule_requested"

    def test_reschedule_on_unknown_reference_writes_nothing(self):
        session = _RecordingSession(select_row=None)
        with _no_real_alert() as alert, pytest.raises(ValueError, match="unknown booking_ref"):
            reschedule(
                client=FakeCalendar(), session=session, calendar_id=CALENDAR_ID,
                booking_ref="nope", now=NOW,
            )
        assert session.statements_containing("UPDATE fa_max_bookings") == []
        alert.assert_not_called()
