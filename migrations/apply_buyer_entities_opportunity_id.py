"""Add opportunity_thread_id to buyer_entities (HUNTER-02, W3).

Minted once, the moment an entity first qualifies as a whale — per the
dev-split plan §6b, which names Hunter (W1/W3) as the first Phase 1 task to
need an Opportunity Thread ID (`OPP-YYYY-#####`). Relay's R4 stamps it on
completion receipts once Relay is built; nothing else in this repo mints one
yet.

Idempotent — IF NOT EXISTS guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_buyer_entities_opportunity_id.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS opportunity_thread_id VARCHAR(20);",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'uq_buyer_entities_opportunity_thread_id'
        ) THEN
            ALTER TABLE buyer_entities
                ADD CONSTRAINT uq_buyer_entities_opportunity_thread_id UNIQUE (opportunity_thread_id);
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

    logger.info("buyer_entities opportunity_thread_id migration complete.")


if __name__ == "__main__":
    main()
