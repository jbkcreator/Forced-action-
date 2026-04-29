"""
Freezable UTC clock for scenario testing.

Production code calls `now()` instead of `datetime.now(timezone.utc)`. In
normal operation `now()` returns the real wall clock. Tests can freeze and
advance the clock without using `time.sleep`.

Usage
-----
In production code:
	from src.core.clock import now
	timestamp = now()

In scenario tests:
	from src.core.clock import freeze_at, advance_by, reset
	freeze_at("2026-05-01T10:00:00Z")
	# ... run scenario ...
	advance_by(minutes=12)
	# ... more scenario ...
	reset()

Notes
-----
- When no fixture has called freeze_at(), now() returns real wall clock.
- Thread-safety: a scenario suite is single-threaded in pytest; do not use
  this helper to coordinate across threads or processes.
- All timestamps are tz-aware UTC.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


_FROZEN_AT: Optional[datetime] = None


def now() -> datetime:
	"""Return the current UTC time, or the frozen time if a scenario has frozen the clock."""
	if _FROZEN_AT is not None:
		return _FROZEN_AT
	return datetime.now(timezone.utc)


def freeze_at(iso_utc: str | datetime) -> datetime:
	"""
	Freeze the clock at a specific moment. Accepts either an ISO-8601 string
	(must be UTC — ends in 'Z' or carries +00:00) or a tz-aware datetime.
	Returns the frozen datetime.
	"""
	global _FROZEN_AT
	if isinstance(iso_utc, datetime):
		dt = iso_utc
	else:
		# Accept both 'Z' suffix and explicit +00:00
		normalized = iso_utc.replace("Z", "+00:00")
		dt = datetime.fromisoformat(normalized)

	if dt.tzinfo is None:
		raise ValueError("freeze_at requires a tz-aware datetime (UTC)")
	_FROZEN_AT = dt.astimezone(timezone.utc)
	return _FROZEN_AT


def advance_by(
	*,
	seconds: int = 0,
	minutes: int = 0,
	hours: int = 0,
	days: int = 0,
) -> datetime:
	"""
	Advance the frozen clock. Raises if the clock is not currently frozen —
	scenario tests must freeze first, explicit about the baseline moment.
	"""
	global _FROZEN_AT
	if _FROZEN_AT is None:
		raise RuntimeError(
			"advance_by() called before freeze_at() — freeze the clock first"
		)
	_FROZEN_AT = _FROZEN_AT + timedelta(
		seconds=seconds, minutes=minutes, hours=hours, days=days,
	)
	return _FROZEN_AT


def reset() -> None:
	"""Unfreeze the clock. now() returns real wall time again."""
	global _FROZEN_AT
	_FROZEN_AT = None


def is_frozen() -> bool:
	return _FROZEN_AT is not None
