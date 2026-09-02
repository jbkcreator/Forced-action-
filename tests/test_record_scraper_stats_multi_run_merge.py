"""
record_scraper_stats()'s ON CONFLICT DO UPDATE merge semantics for a source
scraped more than once on the same run_date (e.g. permit_engine.py, 3x/day
per crontab.txt).

Documents a real asymmetry flagged in PR review: run_success is OR-merged
across same-day calls ("any successful run today counts" — existing,
pre-this-PR behavior), while outcome_category is unconditionally
overwritten to the latest call's value (this PR's addition, deliberately
NOT coalesced — see the comment above the ON CONFLICT clause in
scraper_db_helper.py). Net effect: an early success followed by a later
same-day failure lands as run_success=True + outcome_category=<the later
failure>, which looks self-contradictory read naively, but both merge
policies are independently correct for what they each mean:
  - run_success=True: real data WAS collected today (matches the same
    "partial success stays success" philosophy applied to storm_engine.py/
    flood_engine.py/insurance_engine.py's multi-source partial-failure
    handling elsewhere in this PR — an early collected batch isn't erased
    by a later, unrelated failure).
  - outcome_category=<latest>: the most recent attempt's problem is what
    actually needs investigating right now, not whatever happened earlier
    in the day.

This is an accepted tradeoff, not a bug: forcing run_success to False on
any later-same-day failure would resurrect the exact "partial data
reported as a total failure" bug class this PR spent most of its effort
closing. This test exists so the tradeoff is documented and intentional,
not silently reintroduced or "fixed" into the opposite bug by a future
edit that doesn't know it was deliberate.
"""
from datetime import date

import pytest
from sqlalchemy import text

from src.core.database import get_db_context
from src.utils.scraper_db_helper import record_scraper_stats
from config.scraper_outcomes import ScraperOutcome

_FAKE_DATE = date(1900, 1, 1)
_SOURCE_TYPE = "permits"  # a real, constraint-valid, genuinely multi-run-per-day source_type


@pytest.fixture
def _cleanup():
    yield
    with get_db_context() as s:
        s.execute(
            text("DELETE FROM scraper_run_stats WHERE run_date = :d AND source_type = :st"),
            {"d": _FAKE_DATE, "st": _SOURCE_TYPE},
        )
        s.commit()


def _row():
    with get_db_context() as s:
        return s.execute(
            text(
                "SELECT run_success, outcome_category, total_scraped FROM scraper_run_stats "
                "WHERE run_date = :d AND source_type = :st AND county_id = 'hillsborough'"
            ),
            {"d": _FAKE_DATE, "st": _SOURCE_TYPE},
        ).mappings().first()


def test_early_success_then_later_failure_keeps_run_success_true(_cleanup):
    # Run 1 (e.g. the 05:45 cron slot): succeeds, real records collected.
    record_scraper_stats(
        source_type=_SOURCE_TYPE, total_scraped=12, matched=12, unmatched=0, skipped=0,
        run_date=_FAKE_DATE, county_id="hillsborough",
    )
    row = _row()
    assert row["run_success"] is True
    assert row["outcome_category"] is None
    assert row["total_scraped"] == 12

    # Run 2 (e.g. the 13:45 cron slot): the portal times out.
    record_scraper_stats(
        source_type=_SOURCE_TYPE, total_scraped=0, matched=0, unmatched=0, skipped=0,
        run_date=_FAKE_DATE, county_id="hillsborough",
        outcome=ScraperOutcome.TIMEOUT.value,
    )
    row = _row()
    # Deliberate, documented asymmetry — see module docstring. run_success
    # stays True (OR-merge: real data was collected today), while
    # outcome_category shows the latest problem (last-write-wins).
    assert row["run_success"] is True
    assert row["outcome_category"] == "TIMEOUT"
    assert row["total_scraped"] == 12  # additive; the failed run added 0


def test_early_failure_then_later_success_clears_outcome_category(_cleanup):
    # The order this session already explicitly tested and fixed for
    # (this is the direction the ON CONFLICT comment's own regression
    # concern describes) — confirming it the other way round too.
    record_scraper_stats(
        source_type=_SOURCE_TYPE, total_scraped=0, matched=0, unmatched=0, skipped=0,
        run_date=_FAKE_DATE, county_id="hillsborough",
        outcome=ScraperOutcome.TIMEOUT.value,
    )
    row = _row()
    assert row["run_success"] is False
    assert row["outcome_category"] == "TIMEOUT"

    record_scraper_stats(
        source_type=_SOURCE_TYPE, total_scraped=8, matched=8, unmatched=0, skipped=0,
        run_date=_FAKE_DATE, county_id="hillsborough",
    )
    row = _row()
    assert row["run_success"] is True
    assert row["outcome_category"] is None  # cleared, not stuck on the earlier TIMEOUT
