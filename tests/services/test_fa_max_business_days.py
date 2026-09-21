"""tests/services/test_fa_max_business_days.py"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from src.services.fa_max_business_days import business_days_since, is_weekday

ET = ZoneInfo("America/New_York")


def test_is_weekday_monday_true():
    monday = datetime(2026, 9, 21, 12, 0, tzinfo=ET)  # 2026-09-21 is a Monday
    assert is_weekday(monday) is True


def test_is_weekday_saturday_false():
    saturday = datetime(2026, 9, 19, 12, 0, tzinfo=ET)
    assert is_weekday(saturday) is False


def test_business_days_since_same_day_is_zero():
    now = datetime(2026, 9, 21, 15, 0, tzinfo=ET)
    reference = datetime(2026, 9, 21, 9, 0, tzinfo=ET)
    assert business_days_since(reference, now=now) == pytest.approx(0.25, abs=0.01)


def test_business_days_since_skips_weekend():
    # Friday 9am -> Monday 9am is one business day elapsed, not three.
    reference = datetime(2026, 9, 18, 9, 0, tzinfo=ET)  # Friday
    now = datetime(2026, 9, 21, 9, 0, tzinfo=ET)  # Monday
    assert business_days_since(reference, now=now) == pytest.approx(1.0, abs=0.01)


def test_business_days_since_five_weekdays():
    reference = datetime(2026, 9, 14, 9, 0, tzinfo=ET)  # Monday
    now = datetime(2026, 9, 21, 9, 0, tzinfo=ET)  # next Monday
    assert business_days_since(reference, now=now) == pytest.approx(5.0, abs=0.01)


def test_business_days_since_accepts_naive_utc_and_localizes():
    reference = datetime(2026, 9, 14, 13, 0, tzinfo=timezone.utc)  # 9am ET
    now = datetime(2026, 9, 21, 13, 0, tzinfo=timezone.utc)
    assert business_days_since(reference, now=now) == pytest.approx(5.0, abs=0.01)


def test_business_days_since_defaults_now_to_current_time():
    reference = datetime.now(timezone.utc)
    assert business_days_since(reference) >= 0
