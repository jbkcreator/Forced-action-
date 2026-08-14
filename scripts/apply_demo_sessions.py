"""Apply demo_sessions table + supporting indexes for Live Demo Mode (3g+3h+3i).

Idempotent: CREATE TABLE/INDEX IF NOT EXISTS. Safe to run multiple times.

Usage:
    PYTHONPATH=. python scripts/apply_demo_sessions.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS demo_sessions (
        id            SERIAL PRIMARY KEY,
        subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
        zip_code      VARCHAR(10) NOT NULL,
        vertical      VARCHAR(50) NOT NULL,
        county_id     VARCHAR(50) NOT NULL DEFAULT 'hillsborough',
        property_id   INTEGER REFERENCES properties(id),
        masked_address VARCHAR(255),
        lead_tier     VARCHAR(50),
        distress_types JSONB,
        revealed_at   TIMESTAMPTZ,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_demo_sessions_sub
        ON demo_sessions (subscriber_id, created_at DESC)
    """,
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt))
            logger.info("OK: %s", " ".join(stmt.split())[:90])
    logger.info("demo_sessions DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
