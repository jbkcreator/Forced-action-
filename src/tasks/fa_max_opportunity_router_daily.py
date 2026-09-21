"""WP-T2-11 — GYR Opportunity Router nightly sweep.

Classifies every open fa_max_opportunity as green/yellow/red, persists the
result, logs to fa_max_gyr_routing_log, and posts to the MONEY/EXCEPTIONS Slack
queues. Must complete before 07:00 America/New_York so Josh sees the day's
list at his morning pass.

Stagger: run after the nightly CDS rescore (07:00 UTC) and profile sweep.
Suggested cron: 06:30 UTC daily (02:30 ET, well before 07:00 ET MONEY post).

Usage:
    PYTHONPATH=. python -m src.tasks.fa_max_opportunity_router_daily
    PYTHONPATH=. python -m src.tasks.fa_max_opportunity_router_daily --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

sys.path.insert(0, ".")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _run(dry_run: bool = False) -> None:
    from src.core.database import get_db_context
    from src.services.opportunity_router import run_sweep

    today = date.today()
    logger.info("GYR daily sweep starting for %s (dry_run=%s)", today, dry_run)

    if dry_run:
        logger.info("DRY RUN — no DB writes, no Slack posts")
        return

    with get_db_context() as db:
        count = run_sweep(db, as_of=today)

    logger.info("GYR daily sweep complete: %d opportunities routed", count)


def main() -> None:
    parser = argparse.ArgumentParser(description="GYR opportunity router daily sweep")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and log intent without writing to DB or posting to Slack",
    )
    args = parser.parse_args()
    _run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
