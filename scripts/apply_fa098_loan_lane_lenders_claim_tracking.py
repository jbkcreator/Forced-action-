"""Apply fa098 - Loan Lane lender tracking / claim metadata.

Idempotent DDL for the live Postgres database.

Usage:
    PYTHONPATH=. python scripts/apply_fa098_loan_lane_lenders_claim_tracking.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS lenders (
        lender_id   UUID PRIMARY KEY DEFAULT generate_uuidv7(),
        name        VARCHAR(255) NOT NULL UNIQUE,
        is_cleared  BOOLEAN NOT NULL DEFAULT FALSE,
        is_active   BOOLEAN NOT NULL DEFAULT TRUE,
        created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_lenders_name ON lenders (name);
    """,
    """
    ALTER TABLE lanes
        ADD COLUMN IF NOT EXISTS lender_id UUID,
        ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP WITH TIME ZONE,
        ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'fk_lanes_lender_id_lenders'
        ) THEN
            ALTER TABLE lanes
                ADD CONSTRAINT fk_lanes_lender_id_lenders
                FOREIGN KEY (lender_id) REFERENCES lenders(lender_id);
        END IF;
    END $$;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("Step %d/%d - executing", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info("fa098 complete - lenders + claim metadata applied.")


if __name__ == "__main__":
    main()

