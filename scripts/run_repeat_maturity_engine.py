"""
WP-6 Repeat & Maturity Engine — nightly CLI driver.

Runs all 4 borrower monitors, posts Slack alerts, commits idempotency log.

Usage:
    PYTHONPATH=. python scripts/run_repeat_maturity_engine.py           # live run
    PYTHONPATH=. python scripts/run_repeat_maturity_engine.py --dry-run # log only, no Slack, no DB writes

Cron (06:45 UTC — after scrapers, before CDS):
    45 6 * * * PYTHONPATH=/app python scripts/run_repeat_maturity_engine.py >> /var/log/repeat_maturity.log 2>&1
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import get_settings
from src.services.repeat_maturity_engine import run_monitors

logger = logging.getLogger(__name__)


def main(dry_run: bool, as_of: date) -> None:
    if dry_run:
        logger.info("[repeat_maturity] DRY RUN — no Slack posts, no DB writes")

    engine = create_engine(get_settings().database_url)
    Session_ = sessionmaker(bind=engine)

    with Session_() as session:
        if dry_run:
            # Run queries so we can log counts, but roll back instead of committing.
            alerts = run_monitors(session, today=as_of, deliver=False)
            session.rollback()
            print(f"\n[DRY RUN] Would have fired {len(alerts)} alert(s):")
            for a in alerts:
                print(f"  {a.monitor_type:25s} — {a.canonical_name} (entity {a.buyer_entity_id})")
        else:
            alerts = run_monitors(session, today=as_of)
            session.commit()
            print(f"\nFired {len(alerts)} alert(s) and committed idempotency log.")
            for a in alerts:
                print(f"  {a.monitor_type:25s} — {a.canonical_name} (entity {a.buyer_entity_id})")

    if not alerts:
        logger.info("[repeat_maturity] No alerts fired for %s", as_of)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="WP-6 Repeat & Maturity Engine")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run queries and log results without posting to Slack or writing to DB",
    )
    parser.add_argument(
        "--date", type=date.fromisoformat, default=date.today(),
        help="Run as-of this date (YYYY-MM-DD). Default: today.",
    )
    args = parser.parse_args()

    main(dry_run=args.dry_run, as_of=args.date)
