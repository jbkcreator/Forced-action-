"""Add unique constraint on handoff_quality_ratings(boundary, reference_id).

Prevents duplicate ratings for the same handoff event — an idempotency
guarantee so retries of run_outreach() cannot inflate Hunter's scorecard.
ON CONFLICT DO NOTHING in rate_handoff() relies on this constraint.

Idempotent — DO $$ guards against re-application.

Usage:
    PYTHONPATH=. python migrations/apply_handoff_quality_unique_reference.py
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
            SELECT 1 FROM pg_constraint WHERE conname = 'uq_handoff_quality_boundary_reference'
        ) THEN
            ALTER TABLE handoff_quality_ratings
                ADD CONSTRAINT uq_handoff_quality_boundary_reference
                UNIQUE (boundary, reference_id);
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

    logger.info("handoff_quality_ratings unique constraint applied.")


if __name__ == "__main__":
    main()
