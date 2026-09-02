"""
Scraper outcome vocabulary — replaces the free-text `error_type` convention
on scraper_run_stats ('none'|'no_data'|'scraper_error', never enforced by a
CheckConstraint, already drifted in production: 'export_unavailable' in
evictions_engine.py, 'connector_error' in connectors/runner.py).

Five categories, deliberately not conflatable with each other:
  NO_DATA        — confirmed successful query, genuinely zero matching records.
  TIMEOUT        — request didn't complete in time. Never a synonym for
                    NO_DATA — that conflation is the exact bug that put a real
                    FEMA read-timeout into scraper_run_stats as run_success=True.
  SOURCE_ERROR   — vendor responded, but with a server-side failure (5xx) or
                    rate-limit (429). Not us, not a timeout — they answered
                    and said they're broken.
  INTERNAL_ERROR — our fault: either we sent a bad request (4xx other than
                    429 — stale key, wrong URL/params) or our own parsing/
                    persistence code threw after a good response.
  UNKNOWN        — genuinely unclassifiable. Should be rare; every occurrence
                    means src.utils.scraper_outcome_classifier has a gap, not
                    that the run is fine.

NULL (no outcome_category at all) means clean success with real data — the
same role error_type=NULL/'none' already played. There is no explicit
"SUCCESS" member.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional


class ScraperOutcome(str, Enum):
    NO_DATA = "NO_DATA"
    TIMEOUT = "TIMEOUT"
    SOURCE_ERROR = "SOURCE_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    UNKNOWN = "UNKNOWN"


OUTCOME_VALUES = {o.value for o in ScraperOutcome}


def derive_run_success(outcome: Optional[str]) -> bool:
    """Only a clean success (outcome=None) or a confirmed-empty query
    (NO_DATA) count as 'this run completed correctly' for freshness
    purposes. Every other category — including UNKNOWN — means the run did
    not cleanly complete, and must not silently reset a staleness clock."""
    return outcome is None or outcome == ScraperOutcome.NO_DATA.value


# What a caller sees via the legacy `error_type` column until it migrates to
# reading `outcome_category` directly (Vera's check_silent_failures(),
# src/api/main.py:_classify_scraper_issues). Keeps every un-migrated reader
# working unchanged during the phased rollout.
LEGACY_ERROR_TYPE_MAP = {
    ScraperOutcome.NO_DATA.value: "no_data",
    ScraperOutcome.TIMEOUT.value: "scraper_error",
    ScraperOutcome.SOURCE_ERROR.value: "scraper_error",
    ScraperOutcome.INTERNAL_ERROR.value: "scraper_error",
    ScraperOutcome.UNKNOWN.value: "scraper_error",
}
