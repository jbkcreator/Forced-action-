"""Create revenue_heartbeat_alert_log — cooldown tracking for the daily
revenue heartbeat's alert email (src/tasks/revenue_fulfillment_heartbeat.py).

Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_revenue_heartbeat_alert_log.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS revenue_heartbeat_alert_log (
        id BIGSERIAL PRIMARY KEY,
        alert_key VARCHAR(255) NOT NULL,
        alerted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_revenue_heartbeat_alert_log_key_time
        ON revenue_heartbeat_alert_log (alert_key, alerted_at);
    """,
]


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
    logger.info("revenue_heartbeat_alert_log created.")


if __name__ == "__main__":
    main()
