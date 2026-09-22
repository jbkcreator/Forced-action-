"""WP-T2-11 — GYR Opportunity Router staleness pass.

Resurfaces unactioned green opportunities older than one business day
(Mon–Fri America/New_York). Run after Josh's morning review window opens
so stale alerts land alongside fresh deal flow.

Suggested cron: 08:00 UTC daily.

Usage:
    PYTHONPATH=. python -m src.tasks.fa_max_opportunity_router_staleness
"""
from __future__ import annotations

import logging
import sys

sys.path.insert(0, ".")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    from src.core.database import get_db_context
    from src.services.opportunity_router import run_staleness_pass

    logger.info("GYR staleness pass starting")
    with get_db_context() as db:
        count = run_staleness_pass(db)
    logger.info("GYR staleness pass complete: %d stale alerts posted", count)


if __name__ == "__main__":
    main()
