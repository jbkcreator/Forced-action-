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

    def test_suppress_completion_write_stamps_completed_at_without_aggregate(self, _cleanup):
        """Regression for a real bug found in PR review: suppress used to
        leave completed_at permanently NULL, which is indistinguishable
        from a genuine crash to check_crashed_before_completion() — every
        successful suppressed run (e.g. lien_engine.py's scheduled
        --load-to-db path) would eventually get flagged "crashed" once
        enough time passed. suppress_completion_write() must now stamp
        completed_at (via mark_scraper_attempt_completed()) without writing
        a misleading total_scraped/matched aggregate."""
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            run.suppress_completion_write()
        row = _row()
        assert row is not None
        assert row["attempt_started_at"] is not None
        assert row["completed_at"] is not None
        assert row["run_success"] is True
        assert row["outcome_category"] is None

    def test_suppress_completion_write_also_honored_on_exception_path(self, _cleanup):
        """Regression for a real gap found in self-review: __exit__'s
        exception branch didn't check self._suppressed, so a caller that
        suppresses before a later fallible operation would still get a
        fallback row written out from under it. completed_at is still
        stamped (suppress_completion_write() marks it immediately, not
        deferred to __exit__), so this doesn't reopen the crashed-mid-run
        false positive either."""
        with pytest.raises(ValueError):
            with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
                run.suppress_completion_write()
                raise ValueError("synthetic failure after suppress")
        row = _row()
        assert row is not None
        assert row["attempt_started_at"] is not None
        assert row["completed_at"] is not None
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

    def test_suppressed_run_is_never_flagged_crashed_by_vera(self, _cleanup):
        """End-to-end regression tying the fix directly to the symptom a PR
        reviewer reported: lien_engine.py's scheduled --load-to-db path
        calls suppress_completion_write(), and every successful run of it
        was showing up in Vera's live-state report as "crashed mid-run."
        Mirrors check_crashed_before_completion()'s own query directly
        (rather than importing Vera, which needs VERA_DATABASE_URL) to keep
        this test self-contained."""
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            run.suppress_completion_write()
        with get_db_context() as s:
            still_looks_crashed = s.execute(
                text(
                    "SELECT 1 FROM scraper_run_stats WHERE run_date = :d "
                    "AND source_type = :st AND county_id = 'hillsborough' "
                    "AND attempt_started_at IS NOT NULL AND completed_at IS NULL"
                ),
                {"d": _FAKE_DATE, "st": _SOURCE_TYPE},
            ).first()
        assert still_looks_crashed is None


class TestPipelineExitCode:
    """pipeline_exit_code() — shared exit-code logic for the tri-state
    (True/False/"no_data") CLI contract used by evictions_engine.py,
    probate_engine.py, and divorce_engine.py. Regression for a real bug: a
    prior fix correctly derived a pipeline_ok variable in probate/divorce
    but their final sys.exit() still read the stricter success variable, so
    a genuine no-data day exited 1 and run.sh's retry/alert logic paged on
    a clean run."""

    def test_true_exits_zero(self):
        from src.utils.scraper_run_tracking import pipeline_exit_code
        assert pipeline_exit_code(True) == 0

    def test_no_data_exits_zero(self):
        from src.utils.scraper_run_tracking import pipeline_exit_code
        assert pipeline_exit_code("no_data") == 0

    def test_false_exits_one(self):
        from src.utils.scraper_run_tracking import pipeline_exit_code
        assert pipeline_exit_code(False) == 1


class TestMultiRunPerDayCrashDetection:
    """Regression for a Critical finding in PR review:
    mark_scraper_attempt_started()'s heartbeat UPSERT never reset
    completed_at on a re-run, so a source scraped more than once per
    run_date (e.g. permit_engine.py, 3x/day per crontab.txt) kept an
    earlier run's completed_at timestamp on the row even after a later run
    started. If that later run then crashed before its own completion
    write, completed_at IS NOT NULL still held (from the earlier run), so
    check_crashed_before_completion() never matched it — the exact crash it
    exists to catch became invisible."""

    def test_second_heartbeat_clears_a_stale_completed_at(self, _cleanup):
        # Run 1: completes successfully, sets completed_at.
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            run.success()
        row = _row()
        assert row["completed_at"] is not None

        # Run 2 (same run_date/source_type/county_id, simulating a
        # multi-run-per-day source): __enter__'s heartbeat must reset
        # completed_at back to NULL, not leave run 1's stale value.
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            row = _row()
            assert row["attempt_started_at"] is not None
            assert row["completed_at"] is None
            run.success()

    def test_crash_on_second_run_is_now_detectable(self, _cleanup):
        """End-to-end: run 1 completes successfully (sets completed_at).
        Run 2's process then dies right after the heartbeat, before
        __exit__ ever runs — a TRUE hard crash (OOM kill, SIGKILL), not a
        caught Python exception (the wrapper already classifies and
        completes those correctly on its own via __exit__, which is why
        this test calls __enter__() directly with no matching __exit__,
        rather than raising inside a `with` block). The row must look
        exactly like a crashed mid-run (attempt_started_at set,
        completed_at NULL), not a stale success held over from run 1."""
        with scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE) as run:
            run.success()
        row = _row()
        assert row["completed_at"] is not None  # run 1 completed normally

        run2 = scraper_run(_SOURCE_TYPE, "hillsborough", run_date=_FAKE_DATE)
        run2.__enter__()  # heartbeat only — simulates the process dying here

        row = _row()
        assert row["attempt_started_at"] is not None
        assert row["completed_at"] is None
