"""
record_scraper_stats()'s run_success sentinel/mismatch-warning logic
(src/utils/scraper_db_helper.py).

Regression coverage for a real noise bug found and fixed this session: the
old bool default (run_success=True) fired a spurious "passed run_success=...
but outcome=... implies ..." warning on every outcome-based call that
(correctly) omits run_success — i.e. on every single failure/no-data write
from every wrapper-adopted or bespoke-integrated scraper. Fixed by making
run_success default to None (a sentinel meaning "caller didn't pass it"),
so the warning only fires when the caller explicitly passes a run_success
that disagrees with what outcome= derives.

Uses a fake run_date so these tests never touch real production rows, and
cleans up after itself in a fixture teardown — record_scraper_stats()
commits internally via its own get_db_context() call (a separate connection
from whatever session a test might inject), so a transactional pytest
fixture cannot contain or roll back these writes; explicit DELETE is the
only way to keep this test suite non-destructive against the shared DB.
"""
import logging
from datetime import date

import pytest
from sqlalchemy import text

from src.core.database import get_db_context
from src.utils.scraper_db_helper import record_scraper_stats
from config.scraper_outcomes import ScraperOutcome

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
                "SELECT run_success, outcome_category, error_type FROM scraper_run_stats "
                "WHERE run_date = :d AND source_type = :st AND county_id = :c"
            ),
            {"d": _FAKE_DATE, "st": _SOURCE_TYPE, "c": county_id},
        ).mappings().first()


class TestRunSuccessSentinel:
    def test_neither_outcome_nor_run_success_passed_defaults_to_true(self, _cleanup, caplog):
        """Legacy call sites that pass neither kwarg keep today's behavior
        unchanged: run_success defaults to True, no outcome_category set."""
        # config/logging.yaml sets propagate: false on the "src" logger, so
        # caplog's root-attached handler never sees messages from any src.*
        # logger regardless of level — attach caplog's handler directly.
        target_logger = logging.getLogger("src.utils.scraper_db_helper")
        target_logger.addHandler(caplog.handler)
        target_logger.setLevel(logging.WARNING)
        caplog.set_level(logging.WARNING)
        record_scraper_stats(
            source_type=_SOURCE_TYPE, total_scraped=1, matched=1, unmatched=0, skipped=0,
            run_date=_FAKE_DATE, county_id="hillsborough",
        )
        row = _row()
        assert row["run_success"] is True
        assert row["outcome_category"] is None
        assert not any("passed run_success" in r.message for r in caplog.records)

    def test_outcome_only_derives_run_success_without_warning(self, _cleanup, caplog):
        """The common case for every migrated call site: pass outcome=,
        omit run_success entirely. Must NOT warn — this is correct usage,
        not a mismatch."""
        # config/logging.yaml sets propagate: false on the "src" logger, so
        # caplog's root-attached handler never sees messages from any src.*
        # logger regardless of level — attach caplog's handler directly.
        target_logger = logging.getLogger("src.utils.scraper_db_helper")
        target_logger.addHandler(caplog.handler)
        target_logger.setLevel(logging.WARNING)
        caplog.set_level(logging.WARNING)
        record_scraper_stats(
            source_type=_SOURCE_TYPE, total_scraped=0, matched=0, unmatched=0, skipped=0,
            run_date=_FAKE_DATE, county_id="hillsborough",
            outcome=ScraperOutcome.TIMEOUT.value,
        )
        row = _row()
        assert row["run_success"] is False
        assert row["outcome_category"] == "TIMEOUT"
        assert row["error_type"] == "scraper_error"
        assert not any("passed run_success" in r.message for r in caplog.records)

    def test_outcome_and_agreeing_run_success_no_warning(self, _cleanup, caplog):
        # config/logging.yaml sets propagate: false on the "src" logger, so
        # caplog's root-attached handler never sees messages from any src.*
        # logger regardless of level — attach caplog's handler directly.
        target_logger = logging.getLogger("src.utils.scraper_db_helper")
        target_logger.addHandler(caplog.handler)
        target_logger.setLevel(logging.WARNING)
        caplog.set_level(logging.WARNING)
        record_scraper_stats(
            source_type=_SOURCE_TYPE, total_scraped=0, matched=0, unmatched=0, skipped=0,
            run_date=_FAKE_DATE, county_id="hillsborough",
            outcome=ScraperOutcome.NO_DATA.value, run_success=True,
        )
        row = _row()
        assert row["run_success"] is True
        assert row["outcome_category"] == "NO_DATA"
        assert not any("passed run_success" in r.message for r in caplog.records)

    def test_outcome_and_disagreeing_run_success_warns_and_derived_value_wins(self, _cleanup, caplog):
        """The one case that SHOULD warn: a caller explicitly asserts a
        run_success that contradicts what outcome= implies. The derived
        value (from outcome) always wins over the caller's claim — that's
        what makes run_success trustworthy by construction."""
        # config/logging.yaml sets propagate: false on the "src" logger, so
        # caplog's root-attached handler never sees messages from any src.*
        # logger regardless of level — attach caplog's handler directly.
        target_logger = logging.getLogger("src.utils.scraper_db_helper")
        target_logger.addHandler(caplog.handler)
        target_logger.setLevel(logging.WARNING)
        caplog.set_level(logging.WARNING)
        record_scraper_stats(
            source_type=_SOURCE_TYPE, total_scraped=0, matched=0, unmatched=0, skipped=0,
            run_date=_FAKE_DATE, county_id="hillsborough",
            outcome=ScraperOutcome.SOURCE_ERROR.value, run_success=True,
        )
        row = _row()
        assert row["run_success"] is False
        assert row["outcome_category"] == "SOURCE_ERROR"
        assert any("passed run_success" in r.message for r in caplog.records)
