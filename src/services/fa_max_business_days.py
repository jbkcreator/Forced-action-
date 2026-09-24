"""src/services/fa_max_business_days.py

Business-day math for WP-T2-6 (Stage Monitoring). Weekdays only (Mon-Fri),
Eastern time -- no US-holiday calendar. No `holidays` package is used
anywhere else in this codebase; adding one for a single feature's slightly
more accurate day-count is not worth the new dependency.

ponytail: weekday-only, no holiday calendar -- a stall/chase threshold can
undercount by up to a handful of days around Thanksgiving/Christmas/July 4.
Upgrade path: swap the `_is_business_day` predicate for the `holidays`
package's `US(state="FL")` calendar if the client reports false negatives
around a federal holiday.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")


def _to_et(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_ET)


def is_weekday(dt: datetime) -> bool:
    return _to_et(dt).weekday() < 5  # Mon=0 .. Fri=4


def business_days_since(reference: datetime, *, now: Optional[datetime] = None) -> float:
    """Whole+fractional business days elapsed between `reference` and `now`.

    Walks day-by-day (bounded at 3650 iterations -- ~10 years -- so a bad
    `reference` far in the past can never spin unbounded) counting weekdays,
    then adds the fractional part of the final partial day. Fine for this
    module's scale (per-file sweep threshold comparisons, not a hot loop
    over large datasets).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    start = _to_et(reference)
    end = _to_et(now)
    if end <= start:
        return 0.0

    # Fraction of the first day
    start_day_end = start.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    if start_day_end >= end:
        # Everything is within the first day
        elapsed = (end - start).total_seconds() / 86400.0
        return elapsed if is_weekday(start) else 0.0

    first_day_fraction = (start_day_end - start).total_seconds() / 86400.0 if is_weekday(start) else 0.0

    # Count whole business days starting from the next day
    whole_days = 0
    cursor = start_day_end
    for _ in range(3650):
        next_day = cursor + timedelta(days=1)
        if next_day > end:
            break
        if is_weekday(cursor):
            whole_days += 1
        cursor = next_day

    # Fraction of the last partial day
    last_day_fraction = 0.0
    if is_weekday(cursor) and end > cursor:
        last_day_fraction = (end - cursor).total_seconds() / 86400.0

    return first_day_fraction + whole_days + last_day_fraction
