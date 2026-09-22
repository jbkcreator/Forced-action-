"""scripts/run_stage_monitor_worker.py

WP-T2-6 Stage Monitoring — per-run cron worker. Runs all three sweeps
(stall detection, proactive status touch, document chase) in sequence.
Mirrors scripts/run_abandonment_worker.py's shape.

Usage:
    PYTHONPATH=. python scripts/run_stage_monitor_worker.py           # live
    PYTHONPATH=. python scripts/run_stage_monitor_worker.py --dry-run # log only

Cron (every 15 minutes — cheaper than abandonment's per-minute cadence
since these thresholds are measured in business days, not minutes):
    */15 * * * * cd $PROJECT && PYTHONPATH=. python scripts/run_stage_monitor_worker.py >> /var/log/fa/stage_monitor_worker.log 2>&1
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import get_settings

logger = logging.getLogger(__name__)


def main(dry_run: bool) -> None:
    from src.agents.reply_concierge import stage_monitor

    engine = create_engine(get_settings().database_url)
    Session_ = sessionmaker(bind=engine)

    with Session_() as session:
        if dry_run:
            print("[DRY RUN] stage_monitor sweeps would run here — no writes performed.")
            return

        stalled = stage_monitor.sweep_stalled_files(session)
        touched = stage_monitor.sweep_status_touches(session)
        chased = stage_monitor.sweep_document_chases(session)
        logger.info(
            "stage_monitor_worker: stalled=%d status_touches=%d doc_chases=%d",
            stalled, touched, chased,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="WP-T2-6 Stage Monitor worker")
    parser.add_argument("--dry-run", action="store_true", help="Log only, no DB writes or sends")
    args = parser.parse_args()

    main(dry_run=args.dry_run)
