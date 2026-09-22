"""
WP-T2-5 Abandonment Agent — per-minute cron worker.

Polls abandonment_sequences for touches where due_at <= now() and fires
them via relay_approval_queue. Safe to run every minute — idempotency_key
on relay_approval_queue prevents duplicate sends on back-to-back runs.

Usage:
    PYTHONPATH=. python scripts/run_abandonment_worker.py           # live
    PYTHONPATH=. python scripts/run_abandonment_worker.py --dry-run # log only

Cron (every minute):
    * * * * * cd $PROJECT && PYTHONPATH=. python scripts/run_abandonment_worker.py >> /var/log/fa/abandonment_worker.log 2>&1
"""
from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import get_settings
from src.agents.reply_concierge.abandonment_agent import fire_due_touches

logger = logging.getLogger(__name__)


def main(dry_run: bool) -> None:
    engine = create_engine(get_settings().database_url)
    Session_ = sessionmaker(bind=engine)

    with Session_() as session:
        if dry_run:
            from sqlalchemy import text
            due = session.execute(
                text("""
                    SELECT id, person_id, touch_number, due_at
                    FROM abandonment_sequences
                    WHERE due_at <= NOW()
                      AND sent_at IS NULL
                      AND cancelled_at IS NULL
                    ORDER BY due_at
                    LIMIT 50
                """),
            ).fetchall()
            if due:
                print(f"[DRY RUN] {len(due)} touch(es) due:")
                for r in due:
                    print(f"  seq_id={r[0]} person={r[1]} touch={r[2]} due={r[3]}")
            else:
                print("[DRY RUN] No touches due.")
            return

        fired = fire_due_touches(session)
        if fired:
            logger.info("abandonment_worker: fired %d touch(es)", fired)
        else:
            logger.debug("abandonment_worker: no touches due")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="WP-T2-5 Abandonment Agent worker")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log due touches without sending or writing to DB",
    )
    args = parser.parse_args()

    main(dry_run=args.dry_run)
