"""Apply fa_s1 — financing_intent_scores table.

Creates the financing_intent_scores table with indexes if it does not already
exist. Idempotent: safe to run multiple times.

Usage:
    PYTHONPATH=. python scripts/apply_fa_s1_financing_intent_scores.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS financing_intent_scores (
        id                     BIGSERIAL PRIMARY KEY,
        property_id            INTEGER NOT NULL REFERENCES properties(id),
        county_id              VARCHAR(50),
        score_date             DATE NOT NULL,
        financing_intent_score NUMERIC(5,2) NOT NULL,
        intent_tier            VARCHAR(20) NOT NULL
                                 CONSTRAINT ck_fis_intent_tier
                                 CHECK (intent_tier IN ('high', 'medium', 'low')),
        recommended_product    VARCHAR(50),
        signal_flags           JSONB NOT NULL DEFAULT '{}',
        signal_scores          JSONB NOT NULL DEFAULT '{}',
        signal_details         JSONB NOT NULL DEFAULT '{}',
        source_ids             JSONB NOT NULL DEFAULT '{}',
        excluded_reasons       JSONB NOT NULL DEFAULT '{}',
        created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_fis_property_date UNIQUE (property_id, score_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fis_property_id ON financing_intent_scores (property_id)",
    "CREATE INDEX IF NOT EXISTS idx_fis_county_id   ON financing_intent_scores (county_id)",
    "CREATE INDEX IF NOT EXISTS idx_fis_score_date  ON financing_intent_scores (score_date)",
    "CREATE INDEX IF NOT EXISTS idx_fis_intent_tier ON financing_intent_scores (intent_tier)",
    "CREATE INDEX IF NOT EXISTS idx_fis_score_desc  ON financing_intent_scores (financing_intent_score DESC)",
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt.strip()))
            logger.info("OK: %s", stmt.strip()[:80])
    logger.info("fa_s1_financing_intent_scores DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
