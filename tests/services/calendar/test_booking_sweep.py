"""The sweep that reconciles bookings against the calendar."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text

from src.services.calendar import FakeCalendar, book
from src.services.calendar.client import CalendarEvent
from src.tasks import calendar_booking_sweep as sweep

CALENDAR_ID = "leads@example.invalid"
ATTENDEE = "borrower@example.invalid"


def _allow_all():
    return patch(
        "src.agents.fa_max.tool_registry.check_suppression",
        return_value={"suppressed": False, "reason": None},
    )


def _no_real_alert():
    return patch(
        "src.services.relay.exceptions_alert_queue.enqueue_and_attempt",
        return_value=True,
    )


def _future_slot(days: int = 3):
    from src.services.calendar import Slot

    start = (datetime.now(timezone.utc) + timedelta(days=days)).replace(
        minute=0, second=0, microsecond=0
    )
    return Slot(start=start, end=start + timedelta(minutes=30))


def _booked(session, calendar, slot=None):
    with _allow_all():
        return book(
            client=calendar, session=session, calendar_id=CALENDAR_ID,
            slot=slot or _future_slot(), attendee_email=ATTENDEE, topic="Intro call",
        )


def _not_a_fake(events: dict | None = None):
    """A stand-in live client — the sweep refuses to run against FakeCalendar."""
    client = MagicMock()
    client.get_event.side_effect = lambda *, calendar_id, event_id: (events or {}).get(event_id)
    return client


class TestSafety:
    def test_refuses_to_run_against_a_fake_calendar(self):
        """A fake holds no events, so every booking would read as cancelled."""
        with patch(
            "src.services.calendar.client.get_calendar_client", return_value=FakeCalendar()
        ), patch("src.services.calendar.client.get_calendar_id", return_value=CALENDAR_ID):
            with pytest.raises(RuntimeError, match="refuses to run against FakeCalendar"):
                sweep.run(dry_run=True)

    def test_an_unreadable_event_is_not_treated_as_cancelled(self, bookings_db):
        calendar = FakeCalendar()
        booked = _booked(bookings_db, calendar)

        client = MagicMock()
        client.get_event.side_effect = RuntimeError("calendar unreachable")

        gone = sweep._find_cancelled(bookings_db, client=client, calendar_id=CALENDAR_ID)
        assert gone == [], "a read failure must never be mistaken for a cancellation"

        status = bookings_db.execute(
            text("SELECT status FROM fa_max_bookings WHERE booking_ref = :r"),
            {"r": booked.booking_ref},
        ).scalar()
        assert status == "confirmed"


class TestCancellationDetection:
    def test_a_deleted_event_is_detected(self, bookings_db):
        calendar = FakeCalendar()
        booked = _booked(bookings_db, calendar)

        # get_event returns None — the event is gone from the calendar.
        gone = sweep._find_cancelled(
            bookings_db, client=_not_a_fake(), calendar_id=CALENDAR_ID
        )
        assert [b["booking_ref"] for b in gone] == [booked.booking_ref]

    def test_an_event_marked_cancelled_is_detected(self, bookings_db):
        calendar = FakeCalendar()
        booked = _booked(bookings_db, calendar)

        cancelled = CalendarEvent(
            event_id=booked.event.event_id,
            start=booked.event.start,
            end=booked.event.end,
            summary="Intro call",
            status="cancelled",
        )
        gone = sweep._find_cancelled(
            bookings_db,
            client=_not_a_fake({booked.event.event_id: cancelled}),
            calendar_id=CALENDAR_ID,
        )
        assert [b["booking_ref"] for b in gone] == [booked.booking_ref]

    def test_a_live_event_is_left_alone(self, bookings_db):
        calendar = FakeCalendar()
        booked = _booked(bookings_db, calendar)

        gone = sweep._find_cancelled(
            bookings_db,
            client=_not_a_fake({booked.event.event_id: booked.event}),
            calendar_id=CALENDAR_ID,
        )
        assert gone == []

    def test_past_bookings_are_not_swept(self, bookings_db):
        calendar = FakeCalendar()
        booked = _booked(bookings_db, calendar)
        bookings_db.execute(
            text(
                "UPDATE fa_max_bookings SET starts_at = NOW() - interval '2 days', "
                "ends_at = NOW() - interval '2 days' + interval '30 minutes' "
                "WHERE booking_ref = :r"
            ),
            {"r": booked.booking_ref},
        )

        gone = sweep._find_cancelled(
            bookings_db, client=_not_a_fake(), calendar_id=CALENDAR_ID
        )
        assert gone == [], "a meeting that already happened cannot be cancelled"

    def test_an_already_cancelled_booking_is_not_reported_again(self, bookings_db):
        calendar = FakeCalendar()
        booked = _booked(bookings_db, calendar)
        bookings_db.execute(
            text("UPDATE fa_max_bookings SET status='cancelled' WHERE booking_ref=:r"),
            {"r": booked.booking_ref},
        )

        gone = sweep._find_cancelled(
            bookings_db, client=_not_a_fake(), calendar_id=CALENDAR_ID
        )
        assert gone == []


class TestStaleClaims:
    def _orphan_claim(self, db, age_minutes: int) -> str:
        slot = _future_slot(days=5)
        db.execute(
            text(
                "INSERT INTO fa_max_bookings "
                "(booking_ref, calendar_id, attendee_email, topic, starts_at, ends_at, "
                " status, created_at) "
                "VALUES ('orphan1', :cal, :email, 'x', :s, :e, 'pending', "
                "        NOW() - make_interval(mins => :age))"
            ),
            {
                "cal": CALENDAR_ID, "email": ATTENDEE,
                "s": slot.start, "e": slot.end, "age": age_minutes,
            },
        )
        return "orphan1"

    def test_an_old_claim_is_released(self, bookings_db):
        ref = self._orphan_claim(bookings_db, sweep.STALE_CLAIM_MINUTES + 5)
        assert sweep._find_stale_claims(bookings_db) == [ref]

    def test_a_recent_claim_is_left_in_flight(self, bookings_db):
        self._orphan_claim(bookings_db, 1)
        assert sweep._find_stale_claims(bookings_db) == [], (
            "a fresh claim may still be mid-booking"
        )

    def test_releasing_a_claim_frees_its_slot(self, bookings_db):
        slot = _future_slot(days=5)
        bookings_db.execute(
            text(
                "INSERT INTO fa_max_bookings "
                "(booking_ref, calendar_id, attendee_email, topic, starts_at, ends_at, "
                " status, created_at) "
                "VALUES ('orphan2', :cal, :email, 'x', :s, :e, 'pending', "
                "        NOW() - interval '1 hour')"
            ),
            {"cal": CALENDAR_ID, "email": ATTENDEE, "s": slot.start, "e": slot.end},
        )
        sweep._mark_cancelled(bookings_db, "orphan2")

        result = _booked(bookings_db, FakeCalendar(), slot=slot)
        assert result.booked, "the released slot must be bookable again"


class TestRun:
    """End to end through run().

    run() opens its own session, as a cron entry point should. The test
    transaction is never committed to the database, so that fresh session
    would be on another connection and see none of it — hence the override.
    """

    @pytest.fixture
    def swept(self, bookings_db):
        from contextlib import contextmanager

        @contextmanager
        def _reuse_test_session():
            yield bookings_db

        with patch(
            "src.tasks.calendar_booking_sweep.get_db_context", _reuse_test_session
        ), patch(
            "src.services.calendar.client.get_calendar_id", return_value=CALENDAR_ID
        ):
            yield bookings_db

    def _with_client(self, client):
        return patch(
            "src.services.calendar.client.get_calendar_client", return_value=client
        )

    def test_dry_run_reports_without_writing(self, swept):
        booked = _booked(swept, FakeCalendar())

        with self._with_client(_not_a_fake()), _no_real_alert() as alert:
            result = sweep.run(dry_run=True)

        assert booked.booking_ref in result["cancelled"]
        assert result["dry_run"] is True
        alert.assert_not_called()

        status = swept.execute(
            text("SELECT status FROM fa_max_bookings WHERE booking_ref = :r"),
            {"r": booked.booking_ref},
        ).scalar()
        assert status == "confirmed", "a dry run must not write"

    def test_apply_marks_cancelled_and_alerts(self, swept):
        booked = _booked(swept, FakeCalendar())

        with self._with_client(_not_a_fake()), _no_real_alert() as alert:
            result = sweep.run(dry_run=False)

        assert booked.booking_ref in result["cancelled"]

        status = swept.execute(
            text("SELECT status FROM fa_max_bookings WHERE booking_ref = :r"),
            {"r": booked.booking_ref},
        ).scalar()
        assert status == "cancelled"

        alert.assert_called_once()
        message = alert.call_args.kwargs["message"]
        assert booked.booking_ref in message
        assert ATTENDEE in message

    def test_a_live_booking_is_untouched_by_apply(self, swept):
        booked = _booked(swept, FakeCalendar())

        with self._with_client(_not_a_fake({booked.event.event_id: booked.event})), \
             _no_real_alert() as alert:
            result = sweep.run(dry_run=False)

        assert result["cancelled"] == []
        alert.assert_not_called()
        status = swept.execute(
            text("SELECT status FROM fa_max_bookings WHERE booking_ref = :r"),
            {"r": booked.booking_ref},
        ).scalar()
        assert status == "confirmed"

    def test_a_failed_alert_does_not_stop_the_sweep(self, swept):
        booked = _booked(swept, FakeCalendar())

        with self._with_client(_not_a_fake()), patch(
            "src.services.relay.exceptions_alert_queue.enqueue_and_attempt",
            side_effect=RuntimeError("slack down"),
        ):
            result = sweep.run(dry_run=False)

        assert booked.booking_ref in result["cancelled"]
        status = swept.execute(
            text("SELECT status FROM fa_max_bookings WHERE booking_ref = :r"),
            {"r": booked.booking_ref},
        ).scalar()
        assert status == "cancelled", (
            "the meeting is gone whether or not the page got through"
        )
