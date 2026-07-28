"""
Run-wrapper for Lifecycle Data Engine outcome connectors.

Cron already gives failure isolation (every scraper/task is its own line in
scripts/cron/crontab.txt, run as a separate OS process via
scripts/cron/run.sh — one process crashing cannot affect another). This
module does not add a second scheduler or a second retry/alerting layer on
top of that. What it standardizes is the boring bookkeeping every connector
needs: open one session, time the run, record a ScraperRunStats row via the
existing record_scraper_stats() (src/utils/scraper_db_helper.py) on success
or failure, and return a process exit code so run.sh's existing retry/alert
logic fires exactly as it does for every other scraper today.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable

from sqlalchemy.orm import Session

from src.connectors.registry import get_spec
from src.core.database import get_db_context
from src.utils.scraper_db_helper import record_scraper_stats

logger = logging.getLogger(__name__)


@dataclass
class ConnectorRunResult:
    total_read: int = 0
    staged: int = 0        # OutcomeCandidate rows upserted
    quarantined: int = 0   # rows sent to resolve_or_quarantine's review queue
    skipped: int = 0
    errors: int = 0


class _DryRunRollback(Exception):
    """Internal signal to unwind the session's transaction after a --dry-run work_fn call."""


WorkFn = Callable[[Session, str], ConnectorRunResult]


def run_connector(source_type: str, county_id: str, work_fn: WorkFn, dry_run: bool = False) -> int:
    """
    Run one outcome connector's work_fn under a single DB session, recording
    a ScraperRunStats row on completion (success or failure).

    work_fn is expected to catch its own per-record errors internally
    (mirroring BaseLoader.safe_add/quarantine_unmatched's pattern of catching
    per-row and incrementing a counter rather than aborting the loop) and
    return a ConnectorRunResult summarizing what happened. A work_fn that
    raises is treated as a total connector failure — the whole run rolls back.
    A work_fn that returns normally but reports result.errors > 0 (rows it
    caught and skipped rather than raising) is also treated as a failed run —
    the already-committed good rows stay committed, but the run itself must
    report failure so run.sh's retry/alert logic and the heartbeat both see
    it, instead of silently retrying the same bad rows forever unnoticed.

    Returns 0 on success, 1 on failure — the same convention run.sh already
    expects from every other scraper module.
    """
    spec = get_spec(source_type)  # fail fast if the caller passed an unregistered source_type
    start = time.monotonic()
    result = ConnectorRunResult()
    success = True
    error_message: str | None = None

    try:
        with get_db_context() as session:
            result = work_fn(session, county_id)
            if dry_run:
                raise _DryRunRollback()
    except _DryRunRollback:
        logger.info("[%s] dry run complete — rolled back, no rows persisted.", source_type)
    except Exception as e:
        success = False
        error_message = str(e)[:500]
        logger.exception(
            "[%s] connector run failed (county=%s): %s", source_type, county_id, e
        )

    duration = time.monotonic() - start

    if success and result.errors:
        success = False
        error_message = (
            f"{result.errors} row(s) failed to process — see logs for row ids"
        )
        logger.error(
            "[%s] completed with %d per-record errors (county=%s, source=%s) — "
            "marking run as failed for retry/alert.",
            source_type, result.errors, county_id, spec.reads_table,
        )

    record_scraper_stats(
        source_type=source_type,
        total_scraped=result.total_read,
        matched=result.staged,
        unmatched=result.quarantined,
        skipped=result.skipped,
        run_success=success,
        error_type="connector_error" if not success else None,
        error_message=error_message,
        duration_seconds=round(duration, 2),
        county_id=county_id,
    )

    return 0 if success else 1
