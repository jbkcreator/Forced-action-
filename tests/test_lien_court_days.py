"""Court-day calendar behind lien_engine's empty-result check.

An empty Pinellas export is only trusted as "no filings" when the span holds no
completed court day — the portal renders an empty search identically to one
that never ran (2026-09-23 silently dropped every 2026-09-22 filing).
"""
from datetime import date

import pytest

from src.scrappers.liens.lien_engine import _clerk_holidays, _past_court_days


@pytest.mark.parametrize(
    "start, end, today, expected",
    [
        (date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 23), [date(2026, 9, 22)]),
        (date(2026, 9, 13), date(2026, 9, 14), date(2026, 9, 14), []),
        (date(2026, 9, 6), date(2026, 9, 7), date(2026, 9, 7), []),
        (date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 8), []),
        (date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 1), [date(2026, 8, 31)]),
        (date(2026, 9, 22), date(2026, 9, 22), date(2026, 9, 24), [date(2026, 9, 22)]),
    ],
    ids=[
        "tue-filings-checked-wed",
        "sun-mon-run-on-monday",
        "labor-day-weekend",
        "labor-day-then-tuesday",
        "monday-filings-checked-tuesday",
        "single-day-backfill",
    ],
)
def test_past_court_days(start, end, today, expected):
    assert _past_court_days(start, end, today=today) == expected


def test_clerk_holidays_2026():
    holidays = _clerk_holidays(2026)
    assert date(2026, 9, 7) in holidays        # Labor Day
    assert date(2026, 7, 3) in holidays        # July 4 on Saturday, observed Friday
    assert date(2026, 11, 27) in holidays      # day after Thanksgiving
    assert date(2026, 9, 22) not in holidays
