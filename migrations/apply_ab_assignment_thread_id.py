"""Add ab_assignments.opportunity_thread_id (REVINT-v2.2 I3 review fix).

get_price_variant() needs to assign a price-band arm to a cold Cora
prospect BEFORE they're a subscriber — but ab_assignments.subscriber_id was
NOT NULL, FK'd to subscribers, so there was no way to represent that. This
adds a second, thread-keyed identity path alongside the existing
subscriber-keyed one (message-swap/rollout tests keep using subscriber_id
unchanged), with a check constraint requiring exactly one of the two per
row — see AbAssignment in src/core/models.py.

Idempotent — IF NOT EXISTS / pg_constraint guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_ab_assignment_thread_id.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE ab_assignments ADD COLUMN IF NOT EXISTS opportunity_thread_id VARCHAR(20);",
    "ALTER TABLE ab_assignments ALTER COLUMN subscriber_id DROP NOT NULL;",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'uq_ab_assignment_thread'
        ) THEN
            ALTER TABLE ab_assignments
                ADD CONSTRAINT uq_ab_assignment_thread UNIQUE (test_id, opportunity_thread_id);
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'check_ab_assignment_key_xor'
        ) THEN
            ALTER TABLE ab_assignments
                ADD CONSTRAINT check_ab_assignment_key_xor
                CHECK ((subscriber_id IS NOT NULL) != (opportunity_thread_id IS NOT NULL));
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

    logger.info("ab_assignments opportunity_thread_id migration complete.")


if __name__ == "__main__":
    main()
