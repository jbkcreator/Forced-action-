"""
Regression for a PR review finding on the freshness-regression alert:
"Historical baseline includes future runs" — heartbeat_monitor._beat_for()'s
MAX(created_at) lookup wasn't bounded by the `now` it was evaluating, so a
call with a PAST `now` (check_freshness_regression()'s baseline snapshot)
could pick up a success recorded AFTER that point, produce a negative age,
and read the source as fresh at a moment it was actually stale.

Uses two fake run_dates (not real production data) and explicit rows with
directly-set created_at timestamps — record_scraper_stats() has no
created_at override, so this constructs ScraperRunStats rows via the ORM
directly, then cleans them up in a fixture teardown. Same non-destructive
rationale as tests/test_scraper_run_tracking.py: record_scraper_stats() and
the ORM session both commit via get_db_context(), a separate connection
from any transactional pytest fixture, so explicit DELETE is required.
"""
from datetime import date, datetime, timezone

import pytest

from src.core.database import get_db_context
from src.core.models import ScraperRunStats
from src.tasks.heartbeat_monitor import _beat_for

_SOURCE_TYPE = "sunbiz"  # a real, constraint-valid source_type
_OLD_RUN_DATE = date(1900, 1, 1)
_FUTURE_RUN_DATE = date(1900, 1, 5)
_OLD_CREATED_AT = datetime(1900, 1, 1, tzinfo=timezone.utc)
_FUTURE_CREATED_AT = datetime(1900, 1, 5, tzinfo=timezone.utc)
_SNAPSHOT_NOW = datetime(1900, 1, 2, tzinfo=timezone.utc)  # between the two


@pytest.fixture
def _two_success_rows():
    with get_db_context() as s:
        s.add(ScraperRunStats(
            run_date=_OLD_RUN_DATE, source_type=_SOURCE_TYPE, county_id="hillsborough",
            run_success=True, created_at=_OLD_CREATED_AT,
        ))
        s.add(ScraperRunStats(
            run_date=_FUTURE_RUN_DATE, source_type=_SOURCE_TYPE, county_id="hillsborough",
            run_success=True, created_at=_FUTURE_CREATED_AT,
        ))
        s.commit()
    yield
    with get_db_context() as s:
        from sqlalchemy import text
        s.execute(
            text("DELETE FROM scraper_run_stats WHERE run_date IN (:d1, :d2) AND source_type = :st"),
            {"d1": _OLD_RUN_DATE, "d2": _FUTURE_RUN_DATE, "st": _SOURCE_TYPE},
        )
        s.commit()


def test_beat_for_excludes_a_success_recorded_after_the_snapshot_now(_two_success_rows):
    """A baseline snapshot at 1900-01-02 must pick the 1900-01-01 success
    (before the snapshot), never the 1900-01-05 one (after it) — even
    though the latter is the real MAX(created_at) across all history."""
    with get_db_context() as session:
        beat = _beat_for(session, _SOURCE_TYPE, 1500, "hillsborough", _SNAPSHOT_NOW)

    assert beat.last_success_at is not None
    assert beat.last_success_at.replace(tzinfo=None) == _OLD_CREATED_AT.replace(tzinfo=None)
    # The bug this guards against: without the created_at <= now bound, the
    # future row (4 days later) would be picked instead, making this
    # negative. Bounded correctly, age is the ~1-day gap to the OLD row.
    assert beat.age_minutes is not None and beat.age_minutes >= 0


def test_beat_for_at_the_future_timestamp_does_see_the_later_success(_two_success_rows):
    """Sanity check on the fixture itself: evaluated AT (or after) the later
    row's created_at, that row is visible — proving the exclusion above is
    about the `now` bound, not the row being unreachable some other way."""
    with get_db_context() as session:
        beat = _beat_for(
            session, _SOURCE_TYPE, 1500, "hillsborough",
            _FUTURE_CREATED_AT,
        )

    assert beat.last_success_at is not None
    assert beat.last_success_at.replace(tzinfo=None) == _FUTURE_CREATED_AT.replace(tzinfo=None)
