"""
Guarantees exactly one scraper_run_stats completion write per scraper
invocation, and stamps a start-of-attempt heartbeat before any work begins.

Fixes two proven bugs at once:
  - violation_engine.py / permit_engine.py call record_scraper_stats() only
    on their success path — every failure or empty-result run writes NO ROW
    at all for that day. A source silently absent from scraper_run_stats is
    a worse failure mode than a mislabeled one.
  - "Genuinely never ran" and "ran and crashed before ever reaching the
    stats-write call" were indistinguishable — no heartbeat existed
    independent of the completion write.

Usage:
    from src.utils.scraper_run_tracking import scraper_run

    with scraper_run("flood_damage", county_id) as run:
        ... do the scrape ...
        if not any_data_found:
            run.no_data()
        else:
            run.success(total_scraped=n, matched=m, ...)

A caught exception inside the `with` block is classified automatically via
src.utils.scraper_outcome_classifier.classify_exception() and re-raised
unchanged — this module only observes, it never swallows.

Not force-fit everywhere: lien_engine.py's genuine per-subtype success rows
are written by scraper_db_helper.load_scraped_data_to_db() on a separate
path — call .suppress_completion_write() before returning on that path so
__exit__ doesn't clobber them with a generic aggregate row. evictions_engine.py
and sunbiz's verdict-delegation pattern integrate by calling
classify_exception()/classify_http_status() directly inside their own
existing call sites instead of adopting this wrapper — see the migration
plan for why.
"""
from __future__ import annotations

import logging
import time
from datetime import date as date_type
from typing import Optional

from src.utils.scraper_db_helper import mark_scraper_attempt_started, record_scraper_stats
from src.utils.scraper_outcome_classifier import classify_exception

logger = logging.getLogger(__name__)


class ScraperRun:
    def __init__(self, source_type: str, county_id: str = "hillsborough", run_date=None):
        self.source_type = source_type
        self.county_id = county_id
        self.run_date = run_date or date_type.today()
        self._t0 = time.monotonic()
        self._recorded = False
        self._suppressed = False

    def suppress_completion_write(self) -> None:
        """Call before returning when the real outcome rows were already
        written elsewhere (lien_engine.py's per-subtype path via
        load_scraped_data_to_db()) — __exit__ then writes nothing rather
        than clobbering them with a generic aggregate."""
        self._suppressed = True

    def _duration(self) -> float:
        return round(time.monotonic() - self._t0, 2)

    def success(self, total_scraped: int = 0, matched: int = 0, unmatched: int = 0,
                skipped: int = 0, scored: int = 0) -> None:
        record_scraper_stats(
            source_type=self.source_type, county_id=self.county_id, run_date=self.run_date,
            total_scraped=total_scraped, matched=matched, unmatched=unmatched,
            skipped=skipped, scored=scored, duration_seconds=self._duration(),
        )
        self._recorded = True

    def no_data(self, total_scraped: int = 0, matched: int = 0, unmatched: int = 0,
                skipped: int = 0, scored: int = 0) -> None:
        from config.scraper_outcomes import ScraperOutcome, derive_run_success
        outcome = ScraperOutcome.NO_DATA.value
        record_scraper_stats(
            source_type=self.source_type, county_id=self.county_id, run_date=self.run_date,
            total_scraped=total_scraped, matched=matched, unmatched=unmatched,
            skipped=skipped, scored=scored, duration_seconds=self._duration(),
            run_success=derive_run_success(outcome), outcome=outcome,
        )
        self._recorded = True

    def fail(self, outcome: str, error_message: Optional[str] = None,
              total_scraped: int = 0, matched: int = 0, unmatched: int = 0,
              skipped: int = 0, scored: int = 0) -> None:
        from config.scraper_outcomes import derive_run_success
        record_scraper_stats(
            source_type=self.source_type, county_id=self.county_id, run_date=self.run_date,
            total_scraped=total_scraped, matched=matched, unmatched=unmatched,
            skipped=skipped, scored=scored, duration_seconds=self._duration(),
            run_success=derive_run_success(outcome), outcome=outcome, error_message=error_message,
        )
        self._recorded = True

    def __enter__(self) -> "ScraperRun":
        mark_scraper_attempt_started(self.source_type, self.county_id, self.run_date)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None:
            if not self._recorded:
                outcome = classify_exception(exc)
                self.fail(outcome, error_message=str(exc)[:500])
            return False  # never swallow the original exception

        if not self._recorded and not self._suppressed:
            logger.warning(
                "[scraper_run] %s/%s exited cleanly without recording an outcome — "
                "writing a fallback UNKNOWN row instead of silently writing nothing",
                self.source_type, self.county_id,
            )
            from config.scraper_outcomes import ScraperOutcome
            self.fail(ScraperOutcome.UNKNOWN.value, error_message="no explicit outcome recorded")

        return False


def scraper_run(source_type: str, county_id: str = "hillsborough", run_date=None) -> ScraperRun:
    return ScraperRun(source_type, county_id, run_date)
