"""Build or retry the background enrichment card for LendingFlow leads (T-12).

Creates a row for every emitted, non-suppressed lead that has none (a crashed event handler, or a
process that never subscribed, loses nothing) and re-runs failed rows with growing waits
(config.lending_enrichment.RETRY_BACKOFF_MINUTES, capped at MAX_ATTEMPTS). No-op while
LENDING_ENRICHMENT_ENABLED is false. Cron: every 5 minutes.

    python -m src.tasks.lending_enrichment_sweep
"""
from __future__ import annotations

import logging

from src.lending.db import lending_session
from src.lending.enrichment.service import enabled, run_sweep

logger = logging.getLogger(__name__)


def run() -> int:
    if not enabled():
        return 0
    try:
        with lending_session() as db:
            ran = run_sweep(db)
    except Exception as exc:
        logger.error("[enrichment] sweep crashed: %s", type(exc).__name__)
        return 0
    logger.info("[enrichment] sweep ran=%d", ran)
    return ran


if __name__ == "__main__":
    run()
