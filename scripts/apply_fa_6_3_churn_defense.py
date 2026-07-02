"""Apply fa_6_3_churn_defense - Predictive Engagement & Churn Defense (Task 6.3).

Creates subscriber_session_metrics + churn_defense_leads. Idempotent DDL for the
live Postgres database (CREATE TABLE / INDEX IF NOT EXISTS). Safe to re-run.

subscriber_id is an INTEGER FK -> subscribers(id) (the live schema is integer-keyed;
43 tables already reference it), deviating from the spec's literal VARCHAR(100).

Usage:
    PYTHONPATH=. python scripts/apply_fa_6_3_churn_defense.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS subscriber_session_metrics (
        id                      SERIAL PRIMARY KEY,
        subscriber_id           INTEGER NOT NULL UNIQUE REFERENCES subscribers(id) ON DELETE CASCADE,
        last_login_at           TIMESTAMP WITH TIME ZONE,
        dashboard_views_7_day   INTEGER NOT NULL DEFAULT 0,
        lead_downloads_7_day    INTEGER NOT NULL DEFAULT 0,
        auth_intervals_seconds  INTEGER NOT NULL DEFAULT 0,
        engagement_decay_scalar NUMERIC(3, 2) NOT NULL DEFAULT 1.00,
        updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_subscriber_session_metrics_subscriber
    ON subscriber_session_metrics (subscriber_id);
    """,
    """
    CREATE TABLE IF NOT EXISTS churn_defense_leads (
        id              SERIAL PRIMARY KEY,
        subscriber_id   INTEGER NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
        risk_score      NUMERIC(4, 3) NOT NULL,
        outreach_status VARCHAR(50) NOT NULL DEFAULT 'STAGED',
        triggered_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_churn_defense_leads_subscriber
    ON churn_defense_leads (subscriber_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_churn_defense_leads_triggered_at
    ON churn_defense_leads (triggered_at);
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("Step %d/%d - executing", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info("fa_6_3_churn_defense complete - churn-defense tables + indexes applied.")


if __name__ == "__main__":
    main()
