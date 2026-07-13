"""Allow 'pending' in guarantee_credits.status.

src/tasks/guarantee_shortfall_sweep.py now claims a cycle with a 'pending'
row (INSERT .. ON CONFLICT) before calling Stripe, so a crash or an
overlapping sweep run can never credit Stripe twice for the same cycle.
'pending' wasn't in the original ck_guarantee_credit_status check constraint
(migrations/apply_guarantee_credits.py) — this widens it.

Idempotent — drops and recreates the constraint each run.

Usage:
    PYTHONPATH=. python migrations/apply_guarantee_credits_pending_status.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE guarantee_credits DROP CONSTRAINT IF EXISTS ck_guarantee_credit_status;",
    """
    ALTER TABLE guarantee_credits ADD CONSTRAINT ck_guarantee_credit_status
        CHECK (status IN ('pending', 'met', 'issued', 'failed', 'skipped_no_charge_basis'));
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("guarantee_credits_pending_status migration complete.")


if __name__ == "__main__":
    main()
