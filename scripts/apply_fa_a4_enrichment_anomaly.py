"""Apply fa_a4 - enrichment_anomaly_log (A4 degraded-provider detection).

Idempotent — IF NOT EXISTS guards on table + indexes.

Usage:
    PYTHONPATH=. python scripts/apply_fa_a4_enrichment_anomaly.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS enrichment_anomaly_log (
        id                BIGSERIAL PRIMARY KEY,
        provider          VARCHAR(32)  NOT NULL,
        detected_at       TIMESTAMP    NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        observed_hit_rate NUMERIC(5,4) NOT NULL,
        floor_hit_rate    NUMERIC(5,4) NOT NULL,
        sample_size       INTEGER      NOT NULL DEFAULT 0,
        records_affected  INTEGER      NOT NULL DEFAULT 0,
        alert_sent        BOOLEAN      NOT NULL DEFAULT FALSE
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_enrichment_anomaly_log_provider ON enrichment_anomaly_log (provider)",
    "CREATE INDEX IF NOT EXISTS ix_enrichment_anomaly_log_detected_at ON enrichment_anomaly_log (detected_at)",
    # Idempotency marker for the degraded-batch discount (each hit discounted once).
    "ALTER TABLE enrichment_usage_logs ADD COLUMN IF NOT EXISTS quality_discounted BOOLEAN NOT NULL DEFAULT FALSE",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(str(settings.database_url))
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
    logger.info("fa_a4 applied: enrichment_anomaly_log ready")


if __name__ == "__main__":
    main()
