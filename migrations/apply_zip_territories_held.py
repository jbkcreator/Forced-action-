"""Extend zip_territories.status CHECK constraint to allow 'held'.

Before: status IN ('available', 'locked', 'grace')
After:  status IN ('available', 'locked', 'grace', 'held')

A 'held' row belongs to the 3M deal-room hold-deposit flow — a prospect has
paid a refundable deposit to temporarily hold a ZIP while the deal room is
open. The existing grace_expiry sweep only targets status='grace', so it
cannot affect held rows by design.

Postgres does not support ALTER CONSTRAINT directly; the idempotent pattern is
to drop the old named constraint and recreate it with the new value set.
Both DROP and ADD run inside a single DO block (one transaction) so there
is no window during which the constraint is absent.

Idempotent — safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_zip_territories_held.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    # Single atomic block: drop the 3-value form if present, then add the
    # 4-value form if absent. Both ALTER TABLE calls share one implicit
    # transaction so the constraint is never absent between the two steps.
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'check_zip_status'
              AND conrelid = 'zip_territories'::regclass
        ) THEN
            ALTER TABLE zip_territories DROP CONSTRAINT check_zip_status;
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'check_zip_status'
              AND conrelid = 'zip_territories'::regclass
        ) THEN
            ALTER TABLE zip_territories
                ADD CONSTRAINT check_zip_status
                CHECK (status IN ('available', 'locked', 'grace', 'held'));
        END IF;
    END $$;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("apply_zip_territories_held complete.")


if __name__ == "__main__":
    main()
