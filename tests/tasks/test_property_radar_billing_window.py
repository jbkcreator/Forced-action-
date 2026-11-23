from datetime import datetime, timezone

import pytest

from src.tasks.property_radar_maturity_pull import billing_window_start


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


@pytest.mark.parametrize("now, day, expected", [
    (_utc(2026, 11, 23, 2, 10), 23, _utc(2026, 11, 23)),   # reset day itself: the window starts today
    (_utc(2026, 11, 22, 23, 59), 23, _utc(2026, 10, 23)),  # the day before the reset: still last cycle
    (_utc(2026, 12, 5, 8, 0), 23, _utc(2026, 11, 23)),
    (_utc(2026, 1, 10, 8, 0), 23, _utc(2025, 12, 23)),     # across the year boundary
    (_utc(2026, 3, 1, 0, 0), 28, _utc(2026, 2, 28)),       # short month: day is capped at 28 by the setting
    (_utc(2026, 11, 23, 2, 10), 1, _utc(2026, 11, 1)),     # default: the calendar month
])
def test_window_starts_at_the_latest_reset_day(now, day, expected):
    assert billing_window_start(now, day) == expected
