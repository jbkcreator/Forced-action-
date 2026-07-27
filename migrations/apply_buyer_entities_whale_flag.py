"""Add is_whale / whale_flagged_at to buyer_entities (HUNTER-02, W1).

Persisted rather than recomputed on every read — Cell #1's ranked whale
list (W3) needs to query this cheaply and often. Refreshed by the nightly
sweep (H3), not on every write to buyer_entities.

Idempotent — IF NOT EXISTS guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_buyer_entities_whale_flag.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS is_whale BOOLEAN NOT NULL DEFAULT false;",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS whale_flagged_at TIMESTAMPTZ;",
    """
    CREATE INDEX IF NOT EXISTS idx_buyer_entities_is_whale
        ON buyer_entities (is_whale) WHERE is_whale;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("buyer_entities whale-flag migration complete.")


if __name__ == "__main__":
    main()
