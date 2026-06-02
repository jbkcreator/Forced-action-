"""
Stage 12 — Bankruptcy Filing Alert daily task.

Two phases:
  1. Ingest new bankruptcy filings from CourtListener (rate-limit friendly).
  2. Dispatch alerts to eligible subscribers (email + SMS), deduped + logged.

Monitoring:
  - On ingest API failure → email ops (ALERT_EMAIL) once per run.
  - On alert delivery failure rate above threshold → email ops.
  Both also record a ScraperRunStats row (source_type='bankruptcy_alerts') so
  the existing anomaly/heartbeat monitors see this job.

Usage:
    python -m src.tasks.bankruptcy_alert_dispatch
    python -m src.tasks.bankruptcy_alert_dispatch --lookback 3
    python -m src.tasks.bankruptcy_alert_dispatch --dry-run        # ingest only, no sends
    python -m src.tasks.bankruptcy_alert_dispatch --ingest-only
    python -m src.tasks.bankruptcy_alert_dispatch --dispatch-only

Cron: 0 5 * * *  (05:00 UTC daily — after the bankruptcy scraper window)
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Optional

from config.bankruptcy_alert_config import (
    ALERT_DELIVERY_FAILURE_RATE_THRESHOLD,
    DEFAULT_LOOKBACK_DAYS,
    INGEST_FAILURE_ALERT,
)
from config.settings import get_settings
from src.core.database import get_db_context
from src.services.bankruptcy_alert.alerts import dispatch_alerts
from src.services.bankruptcy_alert.ingest import ingest_filings

logger = logging.getLogger(__name__)


def _ops_alert(subject: str, body: str) -> None:
    """Email the ops recipient. Best-effort — never raises."""
    try:
        from src.services.email import send_email
        settings = get_settings()
        to = getattr(settings, "alert_email", None)
        if to:
            send_email(to, subject, body)
    except Exception:
        logger.warning("[bk-task] failed to send ops alert", exc_info=True)


def _record_stats(*, total: int, matched: int, success: bool, error: Optional[str], duration: float) -> None:
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type="bankruptcy_alerts",
            total_scraped=total,
            matched=matched,
            unmatched=0,
            skipped=0,
            run_success=success,
            error_message=(error[:500] if error else None),
            duration_seconds=round(duration, 2),
            county_id="hillsborough",
        )
    except Exception:
        logger.warning("[bk-task] could not record scraper stats", exc_info=True)


def run(
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    ingest: bool = True,
    dispatch: bool = True,
    dry_run: bool = False,
) -> dict:
    t0 = time.monotonic()
    summary: dict = {"lookback_days": lookback_days, "dry_run": dry_run}

    with get_db_context() as db:
        # ── Phase 1: ingest ──────────────────────────────────────────────
        ingest_result = None
        if ingest:
            ingest_result = ingest_filings(db, lookback_days=lookback_days)
            summary["ingest"] = {
                "fetched": ingest_result.fetched,
                "matched": ingest_result.matched,
                "inserted": ingest_result.inserted,
                "duplicates": ingest_result.duplicates,
                "pages_read": ingest_result.pages_read,
                "success": ingest_result.success,
                "error": ingest_result.error,
            }
            if not ingest_result.success and INGEST_FAILURE_ALERT:
                _ops_alert(
                    "[ALERT] Bankruptcy ingest failed",
                    f"CourtListener ingest failed:\n\n{ingest_result.error}",
                )

        # ── Phase 2: dispatch ────────────────────────────────────────────
        if dispatch and not dry_run:
            dispatch_result = dispatch_alerts(db)
            summary["dispatch"] = {
                "subscriptions": dispatch_result.subscriptions_considered,
                "filings": dispatch_result.filings_considered,
                "emails_sent": dispatch_result.emails_sent,
                "sms_sent": dispatch_result.sms_sent,
                "failed": dispatch_result.failed,
                "deduped": dispatch_result.deduped,
                "failure_rate": round(dispatch_result.failure_rate, 3),
            }
            if (dispatch_result.total_attempts > 0
                    and dispatch_result.failure_rate > ALERT_DELIVERY_FAILURE_RATE_THRESHOLD):
                _ops_alert(
                    "[ALERT] Bankruptcy alert delivery failures",
                    f"Failure rate {dispatch_result.failure_rate:.0%} "
                    f"({dispatch_result.failed}/{dispatch_result.total_attempts}).\n\n"
                    + "\n".join(dispatch_result.errors[:20]),
                )
        elif dispatch and dry_run:
            summary["dispatch"] = {"skipped": "dry_run"}

    duration = time.monotonic() - t0
    summary["duration_seconds"] = round(duration, 2)

    # Record stats from the ingest phase (the scraper-shaped half of this job).
    if ingest_result is not None:
        _record_stats(
            total=ingest_result.fetched,
            matched=ingest_result.matched,
            success=ingest_result.success,
            error=ingest_result.error,
            duration=duration,
        )

    logger.info("[bk-task] %s", json.dumps(summary, default=str))
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    import argparse
    parser = argparse.ArgumentParser(description="Bankruptcy filing alert ingest + dispatch")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--dry-run", action="store_true", help="Ingest only, no alert sends")
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--dispatch-only", action="store_true")
    args = parser.parse_args(argv or sys.argv[1:])

    ingest = not args.dispatch_only
    dispatch = not args.ingest_only

    summary = run(
        lookback_days=args.lookback,
        ingest=ingest,
        dispatch=dispatch,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
