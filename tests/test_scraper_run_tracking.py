"""
src.utils.scraper_run_tracking.scraper_run() — the context-manager wrapper
that stamps a start-of-attempt heartbeat and guarantees exactly one
completion write per scraper invocation.

Regression coverage for two properties the self-review flagged as untested:
  - A `with scraper_run(...) as run:` block that exits cleanly without ever
    calling .success()/.no_data()/.fail() must not silently write nothing —
    it writes a loud fallback UNKNOWN row instead.
  - suppress_completion_write() (used by lien_engine.py, where the real
    per-subtype rows are written elsewhere) must prevent that fallback write
    on BOTH the clean-exit path and the exception path — a bug fixed this
    session (the exception branch didn't check the _suppressed flag).

Uses a fake run_date + explicit cleanup, same rationale as
tests/test_record_scraper_stats_sentinel.py: every write here goes through
record_scraper_stats()'s own internal get_db_context() session, which
commits independently of any test-injected session/transaction.
"""
from datetime import date

import pytest
import requests
from sqlalchemy import text

from src.core.database import get_db_context
from src.utils.scraper_run_tracking import scraper_run

_FAKE_DATE = date(1900, 1, 1)
_SOURCE_TYPE = "sunbiz"  # a real, constraint-valid source_type


@pytest.fixture
def _cleanup():
    yield
    with get_db_context() as s:
        s.execute(
            text("DELETE FROM scraper_run_stats WHERE run_date = :d AND source_type = :st"),
            {"d": _FAKE_DATE, "st": _SOURCE_TYPE},
        )
        s.commit()


def _row(county_id="hillsborough"):
    with get_db_context() as s:
        return s.execute(
            text(
                "SELECT run_success, outcome_category, attempt_started_at, completed_at "
                "FROM scraper_run_stats WHERE run_date = :d AND source_type = :st AND county_id = :c"
            ),
            {"d": _FAKE_DATE, "st": _SOURCE_TYPE, "c": county_id},
        ).mappings().first()


class TestScraperRunContextManager:
    def test_enter_stamps_heartbeat_before_any_completion(self, _cleanup):
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            row = _row()
            assert row is not None
            assert row["attempt_started_at"] is not None
            assert row["completed_at"] is None
            run.success()

    def test_clean_exit_without_recording_writes_fallback_unknown(self, _cleanup):
        """The exact gap this wrapper exists to close: a block that exits
        normally without calling .success()/.no_data()/.fail() must not
        silently write nothing."""
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE):
            pass  # deliberately never call anything
        row = _row()
        assert row is not None
        assert row["run_success"] is False
        assert row["outcome_category"] == "UNKNOWN"
        assert row["completed_at"] is not None

    def test_exception_auto_classifies_and_never_swallows_it(self, _cleanup):
        with pytest.raises(requests.exceptions.Timeout):
            with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE):
                raise requests.exceptions.Timeout("synthetic timeout")
        row = _row()
        assert row["run_success"] is False
        assert row["outcome_category"] == "TIMEOUT"

    def test_suppress_completion_write_on_success_path_writes_nothing(self, _cleanup):
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            run.suppress_completion_write()
        row = _row()
        # Heartbeat still stamped on __enter__, but no completion write.
        assert row is not None
        assert row["attempt_started_at"] is not None
        assert row["completed_at"] is None
        assert row["outcome_category"] is None

    def test_suppress_completion_write_also_honored_on_exception_path(self, _cleanup):
        """Regression for a real gap found in self-review: __exit__'s
        exception branch didn't check self._suppressed, so a caller that
        suppresses before a later fallible operation would still get a
        fallback row written out from under it."""
        with pytest.raises(ValueError):
            with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
                run.suppress_completion_write()
                raise ValueError("synthetic failure after suppress")
        row = _row()
        assert row is not None
        assert row["attempt_started_at"] is not None
        assert row["completed_at"] is None
        assert row["outcome_category"] is None

    def test_no_data_writes_no_data_outcome(self, _cleanup):
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            run.no_data()
        row = _row()
        assert row["run_success"] is True
        assert row["outcome_category"] == "NO_DATA"

    def test_fail_with_explicit_outcome_writes_that_outcome(self, _cleanup):
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            from config.scraper_outcomes import ScraperOutcome
            run.fail(ScraperOutcome.INTERNAL_ERROR.value, error_message="synthetic")
        row = _row()
        assert row["run_success"] is False
        assert row["outcome_category"] == "INTERNAL_ERROR"
