from datetime import datetime, timedelta, timezone

from src.utils.http_helpers import _parse_retry_after


def test_numeric_seconds():
    assert _parse_retry_after("120", default=5) == 120


def test_http_date():
    future = datetime.now(timezone.utc) + timedelta(seconds=30)
    header = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
    wait = _parse_retry_after(header, default=5)
    assert 25 <= wait <= 30


def test_past_http_date_clamped_to_zero():
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    header = past.strftime("%a, %d %b %Y %H:%M:%S GMT")
    assert _parse_retry_after(header, default=5) == 0


def test_malformed_falls_back_to_default():
    assert _parse_retry_after("not-a-date", default=5) == 5


def test_missing_falls_back_to_default():
    assert _parse_retry_after(None, default=5) == 5
