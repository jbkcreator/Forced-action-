"""Apply fa_s5 — revenue_leak_log and win_story_assets tables.

Idempotent: safe to run multiple times.

Usage:
    PYTHONPATH=. python scripts/apply_fa_s5_enhancement_loops.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS revenue_leak_log (
        id                    SERIAL PRIMARY KEY,
        log_date              DATE NOT NULL,
        county_id             VARCHAR(50) NOT NULL,
        total_leads_leaked    INTEGER NOT NULL DEFAULT 0,
        estimated_dollar_value NUMERIC(14,2) NOT NULL DEFAULT 0,
        vertical_breakdown    JSONB,
        created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_revenue_leak_day_county UNIQUE (log_date, county_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_revenue_leak_date   ON revenue_leak_log (log_date)",
    "CREATE INDEX IF NOT EXISTS idx_revenue_leak_county ON revenue_leak_log (county_id)",
    """
    CREATE TABLE IF NOT EXISTS win_story_assets (
        id           SERIAL PRIMARY KEY,
        event_type   VARCHAR(40) NOT NULL
                       CONSTRAINT ck_win_story_event_type
                       CHECK (event_type IN ('lead_pack', 'loan_funded')),
        county_id    VARCHAR(50) NOT NULL,
        proof_text   TEXT NOT NULL,
        amount_range VARCHAR(60),
        is_public    BOOLEAN NOT NULL DEFAULT TRUE,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_win_story_public_created ON win_story_assets (is_public, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_win_story_county         ON win_story_assets (county_id)",
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt.strip()))
            logger.info("OK: %s", stmt.strip()[:80])
    logger.info("fa_s5_enhancement_loops DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
