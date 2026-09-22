"""Turn busy blocks into bookable slots.

Pure functions over explicit inputs — no clock, no network, no database. `now`
is a parameter rather than a call to datetime.now() so minimum-notice behaviour
is testable without freezing time globally.

Every datetime crossing this module's boundary is timezone-aware. Business
hours are resolved in CALENDAR_TIMEZONE before being converted, so a 9am start
stays 9am local across a daylight-saving transition rather than drifting an
hour twice a year.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterator, Sequence
from zoneinfo import ZoneInfo

from config.calendar import (
    BOOKING_HORIZON_DAYS,
    BUSINESS_HOURS_END_HOUR,
    BUSINESS_HOURS_START_HOUR,
    CALENDAR_TIMEZONE,
    DEFAULT_SLOT_DURATION_MINUTES,
    INTER_MEETING_BUFFER_MINUTES,
    MINIMUM_NOTICE_HOURS,
    SLOT_ALIGNMENT_MINUTES,
    SUPPORTED_SLOT_DURATIONS_MINUTES,
    WEEKEND_WEEKDAYS,
)

_TZ = ZoneInfo(CALENDAR_TIMEZONE)


@dataclass(frozen=True)
class BusyBlock:
    """A span the client is already committed for. Half-open: [start, end)."""

    start: datetime
    end: datetime


@dataclass(frozen=True)
class Slot:
    """A bookable span. Half-open: [start, end)."""

    start: datetime
    end: datetime

    @property
    def duration_minutes(self) -> int:
        return int((self.end - self.start).total_seconds() // 60)


def compute_free_slots(
    *,
    busy: Sequence[BusyBlock],
    window_start: datetime,
    window_end: datetime,
    now: datetime,
    duration_minutes: int = DEFAULT_SLOT_DURATION_MINUTES,
    include_weekends: bool = False,
) -> list[Slot]:
    """Bookable slots in [window_start, window_end), honouring every rule.

    Slots of a duration longer than the alignment grid deliberately overlap
    one another (2:00-3:00 and 2:30-3:30 can both be offered). They are
    alternatives presented to one borrower, not concurrently bookable spans.
    """
    if duration_minutes not in SUPPORTED_SLOT_DURATIONS_MINUTES:
        raise ValueError(
            f"Unsupported slot duration {duration_minutes}m — "
            f"supported: {SUPPORTED_SLOT_DURATIONS_MINUTES}"
        )
    _require_aware(window_start, "window_start")
    _require_aware(window_end, "window_end")
    _require_aware(now, "now")

    earliest = max(window_start, now + timedelta(hours=MINIMUM_NOTICE_HOURS))
    latest = min(window_end, now + timedelta(days=BOOKING_HORIZON_DAYS))
    if earliest >= latest:
        return []

    blocked = _merge_busy(busy)
    blocked_starts = [start for start, _ in blocked]
    span = timedelta(minutes=duration_minutes)

    slots: list[Slot] = []
    for day_start, day_end in _business_windows(earliest, latest, include_weekends):
        for free_start, free_end in _subtract(day_start, day_end, blocked, blocked_starts):
            slots.extend(_aligned_slots(free_start, free_end, span))
    return slots


def _require_aware(moment: datetime, label: str) -> None:
    if moment.tzinfo is None or moment.tzinfo.utcoffset(moment) is None:
        raise ValueError(f"{label} must be timezone-aware")


def _merge_busy(busy: Sequence[BusyBlock]) -> list[tuple[datetime, datetime]]:
    """Buffer-pad every block, then merge overlaps into disjoint sorted spans.

    Merging once here keeps the per-day subtraction a single linear sweep
    instead of re-scanning the raw block list for every business day.
    """
    if not busy:
        return []

    pad = timedelta(minutes=INTER_MEETING_BUFFER_MINUTES)
    padded = sorted(
        ((block.start - pad, block.end + pad) for block in busy if block.end > block.start),
        key=lambda span: span[0],
    )
    if not padded:
        return []

    merged = [padded[0]]
    for start, end in padded[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            if end > last_end:
                merged[-1] = (last_start, end)
        else:
            merged.append((start, end))
    return merged


def _business_windows(
    earliest: datetime, latest: datetime, include_weekends: bool
) -> Iterator[tuple[datetime, datetime]]:
    """Yield each day's business-hours span, clamped to [earliest, latest)."""
    current: date = earliest.astimezone(_TZ).date()
    final: date = latest.astimezone(_TZ).date()

    while current <= final:
        if include_weekends or current.weekday() not in WEEKEND_WEEKDAYS:
            opens = datetime.combine(current, time(BUSINESS_HOURS_START_HOUR), tzinfo=_TZ)
            closes = datetime.combine(current, time(BUSINESS_HOURS_END_HOUR), tzinfo=_TZ)
            start = max(opens, earliest)
            end = min(closes, latest)
            if start < end:
                yield start, end
        current += timedelta(days=1)


def _subtract(
    window_start: datetime,
    window_end: datetime,
    blocked: Sequence[tuple[datetime, datetime]],
    blocked_starts: Sequence[datetime],
) -> Iterator[tuple[datetime, datetime]]:
    """Yield the gaps left in a window after removing the blocked spans."""
    cursor = window_start
    # Blocked spans are disjoint and sorted, so the only span starting at or
    # before the window that can still overlap it is the nearest one.
    first = max(bisect_right(blocked_starts, window_start) - 1, 0)

    for block_start, block_end in blocked[first:]:
        if block_start >= window_end:
            break
        if block_end <= cursor:
            continue
        if block_start > cursor:
            yield cursor, min(block_start, window_end)
        cursor = block_end
        if cursor >= window_end:
            return

    if cursor < window_end:
        yield cursor, window_end


def _aligned_slots(
    free_start: datetime, free_end: datetime, span: timedelta
) -> Iterator[Slot]:
    step = timedelta(minutes=SLOT_ALIGNMENT_MINUTES)
    start = _align_up(free_start)
    while start + span <= free_end:
        yield Slot(start=start, end=start + span)
        start += step


def _align_up(moment: datetime) -> datetime:
    """Round forward to the next alignment boundary in local wall-clock time.

    Aligning locally rather than in UTC keeps the grid meaningful for zones
    whose offset is not a whole number of hours.
    """
    local = moment.astimezone(_TZ)
    past_boundary = timedelta(
        minutes=local.minute % SLOT_ALIGNMENT_MINUTES,
        seconds=local.second,
        microseconds=local.microsecond,
    )
    if not past_boundary:
        return moment
    return moment + (timedelta(minutes=SLOT_ALIGNMENT_MINUTES) - past_boundary)
