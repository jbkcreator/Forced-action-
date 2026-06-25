"""Apply fa096 - unified subscriber memory table.

Creates the `unified_subscriber_memory` audit-spine table.

Idempotent — uses IF NOT EXISTS guard on CREATE TABLE, and each index/stage
step checks for existence. Safe to rerun.

Usage:
    PYTHONPATH=. python scripts/apply_fa096_unified_subscriber_memory.py
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    # ── 1. Create the table ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS unified_subscriber_memory (
        id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        subscriber_id   VARCHAR(100) NOT NULL,
        property_id     INTEGER REFERENCES properties(id),
        stream_source   VARCHAR(50) NOT NULL,
        event_type      VARCHAR(100) NOT NULL,
        event_payload   JSONB NOT NULL DEFAULT '{}',
        created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """,
    # ── 2. Check constraint on stream_source ─────────────────────────────────
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'ck_usm_stream_source'
              AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
        ) THEN
            ALTER TABLE unified_subscriber_memory
            ADD CONSTRAINT ck_usm_stream_source
            CHECK (stream_source IN ('STRIPE', 'GHL', 'SMS', 'SYNTHFLOW', 'UNDERWRITING'));
        END IF;
    END $$;
    """,
    # ── 3. Index: subscriber_id + created_at (Cora's hot path) ───────────────
    """
    CREATE INDEX IF NOT EXISTS idx_usm_subscriber_created
        ON unified_subscriber_memory (subscriber_id, created_at DESC);
    """,
    # ── 4. Index: property_id (partial, non-null only) ───────────────────────
    """
    CREATE INDEX IF NOT EXISTS idx_usm_property
        ON unified_subscriber_memory (property_id)
        WHERE property_id IS NOT NULL;
    """,
    # ── 5. Index: stream_source + created_at (aggregation queries) ───────────
    """
    CREATE INDEX IF NOT EXISTS idx_usm_stream_source
        ON unified_subscriber_memory (stream_source, created_at DESC);
    """,
    # ── 6. Index: event_type ─────────────────────────────────────────────────
    """
    CREATE INDEX IF NOT EXISTS idx_usm_event_type
        ON unified_subscriber_memory (event_type);
    """,
]


def main() -> None:
    settings = get_settings()
    db_url = settings.database_url

    logger.info("Connecting to %s", db_url)
    engine = create_engine(db_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            clean = stmt.strip()
            logger.info("Step %d/%d — executing", i, len(DDL))
            conn.execute(text(clean))

    logger.info("fa096 complete — unified_subscriber_memory table applied.")


if __name__ == "__main__":
    main()