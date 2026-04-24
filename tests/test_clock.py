"""Unit tests for the freezable clock helper."""

from datetime import datetime, timezone

import pytest

from src.core import clock


@pytest.fixture(autouse=True)
def _reset_after():
	yield
	clock.reset()


def test_now_returns_tz_aware_real_time_when_not_frozen():
	t = clock.now()
	assert t.tzinfo is not None
	assert not clock.is_frozen()


def test_freeze_at_accepts_z_suffix():
	t = clock.freeze_at("2026-05-01T10:00:00Z")
	assert t == datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
	assert clock.now() == t
	assert clock.is_frozen()


def test_freeze_at_accepts_plus_suffix():
	clock.freeze_at("2026-05-01T10:00:00+00:00")
	assert clock.now() == datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)


def test_freeze_at_accepts_datetime_object():
	dt = datetime(2026, 7, 1, tzinfo=timezone.utc)
	clock.freeze_at(dt)
	assert clock.now() == dt


def test_freeze_at_rejects_naive_datetime():
	with pytest.raises(ValueError, match="tz-aware"):
		clock.freeze_at(datetime(2026, 5, 1, 10, 0, 0))


def test_advance_by_without_freeze_raises():
	with pytest.raises(RuntimeError, match="freeze the clock first"):
		clock.advance_by(minutes=5)


def test_advance_by_accumulates():
	clock.freeze_at("2026-05-01T10:00:00Z")
	clock.advance_by(minutes=12)
	assert clock.now() == datetime(2026, 5, 1, 10, 12, 0, tzinfo=timezone.utc)
	clock.advance_by(hours=1, minutes=18)
	assert clock.now() == datetime(2026, 5, 1, 11, 30, 0, tzinfo=timezone.utc)


def test_reset_unfreezes():
	clock.freeze_at("2026-05-01T10:00:00Z")
	assert clock.is_frozen()
	clock.reset()
	assert not clock.is_frozen()
	# Real wall-clock resumes
	t = clock.now()
	assert t.year >= 2025
