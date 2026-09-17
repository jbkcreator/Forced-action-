"""WP-T2-8 Stage F — create builder_dial_queue table.

Stores operator decisions from RELATIONSHIPS-lane Slack action buttons:
  - Add to dial list
  - Snooze 7 days
  - Not a fit (dismiss)

One row per buyer_entity_id (upserted). Read by Stage-E dial-list wiring
once WP-9 is merged to dev.

Idempotent — CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.

Usage:
    PYTHONPATH=. python migrations/apply_builder_dial_queue.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS builder_dial_queue (
        buyer_entity_id BIGINT PRIMARY KEY
            REFERENCES buyer_entities(id) ON DELETE CASCADE,
        queued_by       VARCHAR(64)  NOT NULL,
        queued_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        snoozed_until   TIMESTAMPTZ,
        dismissed       BOOLEAN      NOT NULL DEFAULT FALSE
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_builder_dial_queue_snoozed
        ON builder_dial_queue (snoozed_until)
        WHERE snoozed_until IS NOT NULL;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("apply_builder_dial_queue complete.")


if __name__ == "__main__":
    main()
