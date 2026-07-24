"""Widen buyer_entities.entity_type to allow 'Estate' (HUNTER-01, H2.6).

`entity_type_for_cluster()` (src/services/buyer_entity_resolution.py) has
always recognized "Estate" as a classification, sourced directly from real
`owners.owner_type` data (28,869 rows / ~2.9% of ~970K owners carry it) --
but the original check_buyer_entity_type constraint only allowed
Individual/LLC/Trust/Corporate. Never caught by dry-run testing since a dry
run returns before any INSERT ever runs; surfaced only on the first real
backfill attempt (2026-07-24), which crashed on cluster ~6001 hitting an
"IRENE K MCKINNEY / LIFE ESTATE" owner record.

Idempotent — drops and recreates the constraint only if its definition
doesn't already include 'Estate'.

Usage:
    PYTHONPATH=. python migrations/apply_buyer_entities_estate_type.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'check_buyer_entity_type'
              AND pg_get_constraintdef(oid) LIKE '%Estate%'
        ) THEN
            ALTER TABLE buyer_entities DROP CONSTRAINT IF EXISTS check_buyer_entity_type;
            ALTER TABLE buyer_entities
                ADD CONSTRAINT check_buyer_entity_type
                CHECK (entity_type IN ('Individual', 'LLC', 'Trust', 'Corporate', 'Estate'));
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

    logger.info("buyer_entities entity_type Estate migration complete.")


if __name__ == "__main__":
    main()
