"""Widen venture_ladder_events.decision CHECK to include 'auto_throttle' and 'auto_revive'.

The current constraint is:
    CHECK (decision IN ('advanced', 'blocked', 'auto_double', 'demoted'))

LEARN-v2.2 Layer 3 adds two new decisions written by
src/services/cell_allocation.py. In Postgres, CHECK constraints must be dropped
and recreated — there is no ALTER to add values. This script is idempotent: it
skips the DDL if 'auto_throttle' is already present in the constraint definition.

Usage:
    PYTHONPATH=. python migrations/apply_cell_allocation_events.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_CHECK_ALREADY_PRESENT = """
SELECT 1
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
WHERE t.relname = 'venture_ladder_events'
  AND c.contype = 'c'
  AND c.conname = 'venture_ladder_events_decision_check'
  AND pg_get_constraintdef(c.oid) LIKE '%auto_throttle%'
LIMIT 1
"""

_DROP_CONSTRAINT = """
ALTER TABLE venture_ladder_events
    DROP CONSTRAINT IF EXISTS venture_ladder_events_decision_check
"""

_ADD_CONSTRAINT = """
ALTER TABLE venture_ladder_events
    ADD CONSTRAINT venture_ladder_events_decision_check
    CHECK (decision IN (
        'advanced', 'blocked', 'auto_double', 'demoted',
        'auto_throttle', 'auto_revive'
    ))
"""


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        already = conn.execute(text(_CHECK_ALREADY_PRESENT)).first()
        if already:
            logger.info(
                "venture_ladder_events CHECK already contains 'auto_throttle' — skipping."
            )
            return

        logger.info("Dropping existing decision CHECK constraint …")
        conn.execute(text(_DROP_CONSTRAINT))

        logger.info("Adding widened decision CHECK constraint …")
        conn.execute(text(_ADD_CONSTRAINT))

    logger.info("apply_cell_allocation_events complete.")


if __name__ == "__main__":
    main()
