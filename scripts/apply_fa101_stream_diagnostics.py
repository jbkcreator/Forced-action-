"""Apply fa101 - stream_diagnostics table.

Idempotent — IF NOT EXISTS guards on all DDL.

Usage:
    PYTHONPATH=. python scripts/apply_fa101_stream_diagnostics.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS stream_diagnostics (
        id              BIGSERIAL PRIMARY KEY,
        county_id       VARCHAR(50)   NOT NULL,
        stream          VARCHAR(50)   NOT NULL,
        metric_name     VARCHAR(64)   NOT NULL,
        severity        VARCHAR(16)   NOT NULL CHECK (severity IN ('yellow','red')),
        observed_value  NUMERIC(10,4) NOT NULL,
        target_value    NUMERIC(10,4) NOT NULL,
        baseline_value  NUMERIC(10,4),
        days_below      INTEGER       NOT NULL,
        category        VARCHAR(64)   NOT NULL,
        trend_summary   TEXT,
        recommendations JSONB         NOT NULL DEFAULT '[]'::jsonb,
        detected_on     DATE          NOT NULL,
        resolved_on     DATE,
        created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_stream_diag_episode UNIQUE (county_id, metric_name, detected_on)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_stream_diag_open
        ON stream_diagnostics (county_id, metric_name)
        WHERE resolved_on IS NULL;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("Step %d/%d", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info("fa101 complete — stream_diagnostics applied.")


if __name__ == "__main__":
    main()
