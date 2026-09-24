"""Availability rules: business hours, notice, horizon, buffer, DST, alignment."""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from config.calendar import (
    BOOKING_HORIZON_DAYS,
    BUSINESS_HOURS_END_HOUR,
    BUSINESS_HOURS_START_HOUR,
    MINIMUM_NOTICE_HOURS,
    validate_calendar_config,
)
from src.services.calendar.availability import BusyBlock, compute_free_slots

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _et(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=ET)


def _local_times(slots) -> list[tuple[int, int]]:
    return [(s.start.astimezone(ET).hour, s.start.astimezone(ET).minute) for s in slots]


# A Monday, comfortably clear of any daylight-saving transition.
MONDAY = _et(2026, 6, 15, 0)
NOW = MONDAY - timedelta(days=2)


def _slots(**overrides):
    params = {
        "busy": [],
        "window_start": MONDAY,
        "window_end": MONDAY + timedelta(days=1),
        "now": NOW,
    }
    params.update(overrides)
    return compute_free_slots(**params)


def test_config_is_internally_consistent():
    validate_calendar_config()


class TestBusinessHours:
    def test_every_slot_falls_inside_business_hours(self):
        for slot in _slots():
            start_local = slot.start.astimezone(ET)
            end_local = slot.end.astimezone(ET)
            assert start_local.hour >= BUSINESS_HOURS_START_HOUR
            assert (end_local.hour, end_local.minute) <= (BUSINESS_HOURS_END_HOUR, 0)

    def test_day_opens_and_closes_on_the_configured_hours(self):
        slots = _slots()
        assert _local_times(slots)[0] == (BUSINESS_HOURS_START_HOUR, 0)
        assert slots[-1].end.astimezone(ET).hour == BUSINESS_HOURS_END_HOUR

    def test_weekends_excluded_by_default(self):
        saturday = _et(2026, 6, 20, 0)
        assert _slots(window_start=saturday, window_end=saturday + timedelta(days=2)) == []

    def test_weekends_included_when_requested(self):
        saturday = _et(2026, 6, 20, 0)
        slots = _slots(
            window_start=saturday,
            window_end=saturday + timedelta(days=2),
            include_weekends=True,
        )
        assert slots


class TestMinimumNotice:
    def test_slots_inside_the_notice_window_are_excluded(self):
        now = _et(2026, 6, 15, 9)
        slots = _slots(now=now, window_end=MONDAY + timedelta(days=3))
        earliest = now + timedelta(hours=MINIMUM_NOTICE_HOURS)
        assert slots
        assert all(slot.start >= earliest for slot in slots)

    def test_no_slots_when_the_whole_window_is_inside_the_notice_period(self):
        now = _et(2026, 6, 15, 8)
        assert _slots(now=now, window_end=MONDAY + timedelta(hours=12)) == []


class TestBookingHorizon:
    def test_slots_beyond_the_horizon_are_excluded(self):
        now = _et(2026, 6, 1, 9)
        slots = _slots(
            now=now,
            window_start=now,
            window_end=now + timedelta(days=BOOKING_HORIZON_DAYS * 3),
        )
        assert slots
        assert all(slot.end <= now + timedelta(days=BOOKING_HORIZON_DAYS) for slot in slots)


class TestBusyBlocks:
    def test_busy_span_removes_its_slots(self):
        busy = [BusyBlock(start=_et(2026, 6, 15, 12), end=_et(2026, 6, 15, 13))]
        assert (12, 0) not in _local_times(_slots(busy=busy))

    def test_buffer_applies_to_both_edges(self):
        busy = [BusyBlock(start=_et(2026, 6, 15, 12), end=_et(2026, 6, 15, 13))]
        times = _local_times(_slots(busy=busy))
        # 15-minute padding blocks 11:45-13:15, so a slot ending at 12:00 and
        # one starting at 13:00 both collide; the next clean start is 13:30.
        assert (11, 30) not in times
        assert (13, 0) not in times
        assert (11, 0) in times
        assert (13, 30) in times

    def test_overlapping_busy_spans_are_merged(self):
        busy = [
            BusyBlock(start=_et(2026, 6, 15, 10), end=_et(2026, 6, 15, 12)),
            BusyBlock(start=_et(2026, 6, 15, 11), end=_et(2026, 6, 15, 14)),
        ]
        times = _local_times(_slots(busy=busy))
        assert not [t for t in times if 10 <= t[0] < 14]
        assert (9, 0) in times

    def test_zero_length_busy_span_is_ignored(self):
        moment = _et(2026, 6, 15, 12)
        busy = [BusyBlock(start=moment, end=moment)]
        assert _local_times(_slots(busy=busy)) == _local_times(_slots())

    def test_fully_booked_day_yields_nothing(self):
        busy = [BusyBlock(start=_et(2026, 6, 15, 0), end=_et(2026, 6, 16, 0))]
        assert _slots(busy=busy) == []


class TestDaylightSaving:
    """Business hours are wall-clock local, so the UTC offset must move with DST."""

    def test_nine_am_stays_nine_am_across_the_spring_transition(self):
        # US DST begins the second Sunday of March; this window straddles it.
        window_start = _et(2026, 3, 2, 0)
        slots = compute_free_slots(
            busy=[],
            window_start=window_start,
            window_end=window_start + timedelta(days=14),
            now=window_start - timedelta(days=1),
        )
        assert slots

        opening_hours = {
            slot.start.astimezone(ET).hour
            for slot in slots
            if slot.start.astimezone(ET).minute == 0
        }
        assert min(opening_hours) == BUSINESS_HOURS_START_HOUR

        # Precondition only: confirms the window really does straddle the
        # transition, so the wall-clock assertion below is testing something.
        offsets = {slot.start.astimezone(ET).utcoffset() for slot in slots}
        assert offsets == {timedelta(hours=-5), timedelta(hours=-4)}

    def test_first_slot_each_day_is_nine_local_on_both_sides_of_the_shift(self):
        window_start = _et(2026, 3, 2, 0)
        slots = compute_free_slots(
            busy=[],
            window_start=window_start,
            window_end=window_start + timedelta(days=14),
            now=window_start - timedelta(days=1),
        )
        first_by_date: dict[object, datetime] = {}
        for slot in slots:
            local = slot.start.astimezone(ET)
            first_by_date.setdefault(local.date(), local)
        assert first_by_date
        assert all(
            (first.hour, first.minute) == (BUSINESS_HOURS_START_HOUR, 0)
            for first in first_by_date.values()
        )


class TestSlotShape:
    def test_starts_align_to_the_grid(self):
        assert all(minute in (0, 30) for _, minute in _local_times(_slots()))

    def test_sixty_minute_slots_are_a_full_hour(self):
        slots = _slots(duration_minutes=60)
        assert slots
        assert all(slot.duration_minutes == 60 for slot in slots)

    def test_longer_slots_may_overlap_because_they_are_alternatives(self):
        starts = [slot.start for slot in _slots(duration_minutes=60)]
        assert starts[1] - starts[0] == timedelta(minutes=30)

    def test_gap_shorter_than_the_duration_yields_nothing(self):
        busy = [
            BusyBlock(start=_et(2026, 6, 15, 0), end=_et(2026, 6, 15, 12)),
            BusyBlock(start=_et(2026, 6, 15, 12, 30), end=_et(2026, 6, 16, 0)),
        ]
        assert _slots(busy=busy, duration_minutes=60) == []


class TestInputValidation:
    def test_naive_datetime_is_rejected(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            _slots(now=datetime(2026, 6, 13, 9))

    def test_unsupported_duration_is_rejected(self):
        with pytest.raises(ValueError, match="Unsupported slot duration"):
            _slots(duration_minutes=45)

    def test_inverted_window_yields_nothing(self):
        assert _slots(window_start=MONDAY + timedelta(days=1), window_end=MONDAY) == []
