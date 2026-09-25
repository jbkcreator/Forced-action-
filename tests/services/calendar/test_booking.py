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


class TestAvailabilityCaching:
    """Cache what is shown; never cache what decides a write."""

    @pytest.fixture
    def cache_id(self):
        """A calendar id unique to this test.

        The cache lives in a shared Redis with a 30-second TTL, so a fixed id
        would let one run's entry satisfy the next run's first read and the
        call count would silently come out wrong.
        """
        import uuid

        return f"cache-test-{uuid.uuid4().hex}@example.invalid"

    def _counting_calendar(self):
        calls = []

        class _Counting(FakeCalendar):
            def get_busy(self, **kwargs):
                calls.append(kwargs)
                return super().get_busy(**kwargs)

        return _Counting(), calls

    def test_display_reads_are_served_from_cache(self, cache_id):
        calendar, calls = self._counting_calendar()
        args = dict(
            client=calendar, calendar_id=cache_id, window_start=WINDOW_START,
            window_end=WINDOW_END, now=NOW, use_cache=True,
        )
        get_slots(**args)
        get_slots(**args)

        assert len(calls) == 1, "the second view should not hit the provider again"

    def test_uncached_reads_always_hit_the_provider(self, cache_id):
        calendar, calls = self._counting_calendar()
        args = dict(
            client=calendar, calendar_id=cache_id, window_start=WINDOW_START,
            window_end=WINDOW_END, now=NOW,
        )
        get_slots(**args)
        get_slots(**args)

        assert len(calls) == 2

    def test_the_booking_recheck_never_enters_the_cache_layer(self, cache_id):
        """Caching the safety check would reintroduce the race it prevents.

        Asserted by watching the cache helper rather than counting provider
        calls: on a miss the cache calls through anyway, so a call count
        cannot tell the two paths apart.
        """
        calendar, _ = self._counting_calendar()
        session = _RecordingSession()

        with patch("src.services.calendar.booking._cached_busy") as cached, _allow_all():
            book(
                client=calendar, session=session, calendar_id=cache_id, slot=_slot(11),
                attendee_email=ATTENDEE, topic="Intro call",
            )

        cached.assert_not_called()


class TestIntegrityAgainstPostgres:
    """The guarantees the database enforces, not merely the code.

    A re-check in application code narrows a race; only a constraint closes
    it. These run against the real schema so a missing index fails here
    rather than in front of two borrowers.
    """

    def test_the_slot_index_rejects_a_second_live_booking(self, bookings_db):
        from sqlalchemy.exc import IntegrityError

        slot = _slot(11)
        calendar = FakeCalendar()
        with _allow_all():
            first = _book(calendar, bookings_db, slot=slot)
        assert first.booked

        # Bypass book() entirely — this is the database's job, not the code's.
        # A savepoint contains the expected failure: rolling back the session
        # outright would take the fixture's own transaction with it.
        with pytest.raises(IntegrityError):
            with bookings_db.begin_nested():
                bookings_db.execute(
                    text(
                        "INSERT INTO fa_max_bookings "
                        "(booking_ref, calendar_id, attendee_email, topic, starts_at, ends_at, status) "
                        "VALUES ('sneak', :cal, 'other@example.invalid', 'x', :s, :e, 'confirmed')"
                    ),
                    {"cal": CALENDAR_ID, "s": slot.start, "e": slot.end},
                )

    def test_a_cancelled_booking_releases_its_slot(self, bookings_db):
        slot = _slot(11)
        calendar = FakeCalendar()
        with _allow_all():
            first = _book(calendar, bookings_db, slot=slot)

        bookings_db.execute(
            text("UPDATE fa_max_bookings SET status='cancelled' WHERE booking_ref=:r"),
            {"r": first.booking_ref},
        )
        calendar.cancel_event(
            calendar_id=CALENDAR_ID, event_id=first.event.event_id
        )

        with _allow_all():
            second = _book(
                calendar, bookings_db, slot=slot, attendee="other@example.invalid"
            )
        assert second.booked, "a cancelled booking must not hold its slot forever"

    def test_replaying_the_same_booking_returns_the_original(self, bookings_db):
        slot = _slot(11)
        calendar = FakeCalendar()
        with _allow_all():
            first = _book(calendar, bookings_db, slot=slot)
            replay = _book(calendar, bookings_db, slot=slot)

        assert replay.booked
        assert replay.booking_ref == first.booking_ref
        assert replay.reason == "already_booked"
        assert len(calendar.created) == 1, "a replay must not create a second meeting"

    def test_live_booking_lookup_tracks_the_link(self, bookings_db):
        from src.services.calendar import has_live_booking

        link_id = bookings_db.execute(
            text("SELECT id FROM tracked_links ORDER BY id LIMIT 1")
        ).scalar()
        if link_id is None:
            pytest.skip("no tracked_links row to attach to")

        assert not has_live_booking(bookings_db, tracked_link_id=link_id)

        with _allow_all():
            booked = book(
                client=FakeCalendar(), session=bookings_db, calendar_id=CALENDAR_ID,
                slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
                tracked_link_id=link_id,
            )
        assert booked.booked
        assert has_live_booking(bookings_db, tracked_link_id=link_id)

        bookings_db.execute(
            text("UPDATE fa_max_bookings SET status='cancelled' WHERE booking_ref=:r"),
            {"r": booked.booking_ref},
        )
        assert not has_live_booking(bookings_db, tracked_link_id=link_id), (
            "a cancelled booking must let the borrower use the link again"
        )


class _Result:
    rowcount = 1

    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _RecordingSession:
    """Captures the SQL a call would issue, without a database behind it.

    `rows` maps a SQL fragment to the row that query should return, so the
    idempotency lookup and the reschedule lookup can answer differently
    within one call.
    """

    def __init__(self, select_row=None, rows=None):
        self.calls: list[tuple[str, dict]] = []
        self.commits = 0
        self.rollbacks = 0
        self._select_row = select_row
        self._rows = rows or {}

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.calls.append((sql, params or {}))
        for fragment, row in self._rows.items():
            if fragment in sql:
                return _Result(row)
        return _Result(self._select_row)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def statements_containing(self, fragment: str) -> list[tuple[str, dict]]:
        return [call for call in self.calls if fragment in call[0]]


class TestWrittenSqlWithoutDatabase:
    """What book() and reschedule() write, provable before the table exists.

    These assert the statements and parameters, not that the SQL is valid
    against Postgres — the bookings_db tests cover that, and skip until
    migrations/apply_fa_max_bookings.py has been run.
    """

    def test_book_claims_the_slot_then_confirms_it(self):
        session = _RecordingSession()
        slot = _slot(11)
        with _allow_all():
            result = book(
                client=FakeCalendar(), session=session, calendar_id=CALENDAR_ID,
                slot=slot, attendee_email=ATTENDEE, topic="Intro call",
                person_id="11111111-1111-1111-1111-111111111111",
                tracked_link_id=99,
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
        assert params["tracked_link_id"] == 99
        assert len(params["idempotency_key"]) == 64
        assert "'pending'" in inserts[0][0], "the slot is claimed before the event exists"

        confirms = session.statements_containing("SET status = 'confirmed'")
        assert len(confirms) == 1
        assert confirms[0][1]["event_id"] == result.event.event_id

    def test_the_claim_is_committed_before_the_provider_is_called(self):
        """A crash between the two must leave a stray row, never a stray meeting."""
        commits_at_create = []

        class _WatchingCalendar(FakeCalendar):
            def create_event(self, **kwargs):
                commits_at_create.append(session.commits)
                return super().create_event(**kwargs)

        session = _RecordingSession()
        with _allow_all():
            book(
                client=_WatchingCalendar(), session=session, calendar_id=CALENDAR_ID,
                slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
            )

        assert commits_at_create == [1], "the claim was not durable before the event"

    def test_idempotency_key_is_stable_for_the_same_booking(self):
        keys = []
        for _ in range(2):
            session = _RecordingSession()
            with _allow_all():
                book(
                    client=FakeCalendar(), session=session, calendar_id=CALENDAR_ID,
                    slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
                )
            keys.append(
                session.statements_containing("INSERT INTO fa_max_bookings")[0][1][
                    "idempotency_key"
                ]
            )
        assert keys[0] == keys[1]

    def test_idempotency_key_differs_by_attendee_and_slot(self):
        def key_for(slot, attendee):
            session = _RecordingSession()
            with _allow_all():
                book(
                    client=FakeCalendar(), session=session, calendar_id=CALENDAR_ID,
                    slot=slot, attendee_email=attendee, topic="Intro call",
                )
            return session.statements_containing("INSERT INTO fa_max_bookings")[0][1][
                "idempotency_key"
            ]

        base = key_for(_slot(11), ATTENDEE)
        assert key_for(_slot(14), ATTENDEE) != base
        assert key_for(_slot(11), "other@example.invalid") != base

    def test_replay_of_a_confirmed_booking_returns_the_original(self):
        session = _RecordingSession(
            rows={"SELECT booking_ref, status FROM fa_max_bookings":
                  {"booking_ref": "ORIG123", "status": "confirmed"}}
        )
        calendar = FakeCalendar()
        with _allow_all():
            result = book(
                client=calendar, session=session, calendar_id=CALENDAR_ID,
                slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
            )

        assert result.booked
        assert result.booking_ref == "ORIG123"
        assert result.reason == "already_booked"
        assert calendar.created == [], "a replay must not create a second meeting"

    def test_replay_of_a_pending_booking_does_not_claim_success(self):
        session = _RecordingSession(
            rows={"SELECT booking_ref, status FROM fa_max_bookings":
                  {"booking_ref": "ORIG123", "status": "pending"}}
        )
        calendar = FakeCalendar()
        with _allow_all():
            result = book(
                client=calendar, session=session, calendar_id=CALENDAR_ID,
                slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
            )

        assert not result.booked, "a pending claim may have no meeting behind it"
        assert result.reason == "in_progress"
        assert calendar.created == []

    def test_a_cancelled_booking_does_not_block_rebooking(self):
        session = _RecordingSession(
            rows={"SELECT booking_ref, status FROM fa_max_bookings":
                  {"booking_ref": "OLD123", "status": "cancelled"}}
        )
        calendar = FakeCalendar()
        with _allow_all():
            result = book(
                client=calendar, session=session, calendar_id=CALENDAR_ID,
                slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
            )

        assert result.booked
        assert result.booking_ref != "OLD123"
        assert len(calendar.created) == 1

    def test_provider_failure_releases_the_claimed_slot(self):
        session = _RecordingSession()
        calendar = FakeCalendar(fail_attendees={ATTENDEE})
        with _allow_all(), pytest.raises(FakeCalendarError):
            book(
                client=calendar, session=session, calendar_id=CALENDAR_ID,
                slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
            )

        released = session.statements_containing("SET status = 'cancelled'")
        assert len(released) == 1, (
            "a pending row holds the slot against everyone else; a failed "
            "booking must give it back"
        )

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
        # The idempotency lookup still runs; nothing is written.
        assert session.statements_containing("INSERT INTO fa_max_bookings") == []
        assert session.commits == 0

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


class TestPr296ReviewFixes:
    """Regressions for the PR #296 review: overlap, naive times, expired claims."""

    def test_overlapping_slot_with_a_different_start_is_refused(self, bookings_db):
        with _allow_all():
            first = _book(FakeCalendar(), bookings_db, slot=_slot(14, minutes=60))
            # A fresh fake sees no busy time, as when both requests pass the
            # live re-check before either event exists — only the DB can refuse.
            second = _book(
                FakeCalendar(), bookings_db, slot=_slot(14, 30),
                attendee="other@example.invalid",
            )
        assert first.booked
        assert not second.booked
        assert second.reason == "slot_taken"

    def test_back_to_back_slots_do_not_conflict(self, bookings_db):
        with _allow_all():
            first = _book(FakeCalendar(), bookings_db, slot=_slot(14))
            second = _book(
                FakeCalendar(), bookings_db, slot=_slot(14, 30),
                attendee="other@example.invalid",
            )
        assert first.booked and second.booked

    def test_naive_timestamps_are_rejected_before_anything_is_written(self):
        session = _RecordingSession()
        naive = datetime(2026, 6, 15, 14)
        with _allow_all(), pytest.raises(ValueError, match="timezone-aware"):
            book(
                client=FakeCalendar(), session=session, calendar_id=CALENDAR_ID,
                slot=Slot(start=naive, end=naive + timedelta(minutes=30)),
                attendee_email=ATTENDEE, topic="Intro call",
            )
        assert session.calls == []

    def test_a_claim_released_mid_call_cancels_the_new_event(self):
        class _SweptSession(_RecordingSession):
            def execute(self, statement, params=None):
                result = super().execute(statement, params)
                if "SET status = 'confirmed'" in " ".join(str(statement).split()):
                    result.rowcount = 0
                return result

        calendar = FakeCalendar()
        session = _SweptSession()
        with _allow_all():
            result = book(
                client=calendar, session=session, calendar_id=CALENDAR_ID,
                slot=_slot(11), attendee_email=ATTENDEE, topic="Intro call",
            )

        assert not result.booked
        assert result.reason == "claim_expired"
        assert calendar.get_busy(
            calendar_id=CALENDAR_ID, start=_slot(11).start, end=_slot(11).end
        ) == [], "the orphaned meeting must be taken back"
