"""Backfill missing customer_accounts rows for subscribers holding a locked ZIP territory.

apply_fa084_s1_revenue_engine.py created customer_accounts but never backfilled
existing subscribers. lead_delivery.candidates_for() inner-joins zip_territories
to customer_accounts on subscriber_id, so any locked-territory subscriber missing
a customer_accounts row is silently excluded from delivery (sweep still logs
SUCCESS). This creates the missing rows with status='active' — they already
hold a locked (paid) territory, so free_trial would misrepresent their state.

Idempotent: only inserts for subscriber_ids that don't already have a
customer_accounts row. Safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_backfill_locked_zip_customer_accounts.py [--dry-run]
"""

import argparse
import logging

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FIND_MISSING = text(
    """
    SELECT DISTINCT zt.subscriber_id
    FROM zip_territories zt
    LEFT JOIN customer_accounts ca ON ca.subscriber_id = zt.subscriber_id
    WHERE zt.status = 'locked'
      AND zt.subscriber_id IS NOT NULL
      AND ca.account_id IS NULL
    """
)

INSERT_ACCOUNT = text(
    """
    INSERT INTO customer_accounts (subscriber_id, status, acquisition_source)
    VALUES (:subscriber_id, 'active', 'backfill_locked_zip_territories')
    """
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = Database()
    with db.session_scope() as s:
        missing = [row[0] for row in s.execute(FIND_MISSING).fetchall()]
        logger.info("Found %d subscriber(s) with a locked ZIP territory missing a customer_accounts row", len(missing))

        if args.dry_run:
            logger.info("Dry run — no rows inserted. subscriber_ids: %s", missing)
            return

        for subscriber_id in missing:
            s.execute(INSERT_ACCOUNT, {"subscriber_id": subscriber_id})

        logger.info("Inserted %d customer_accounts row(s)", len(missing))


if __name__ == "__main__":
    main()
