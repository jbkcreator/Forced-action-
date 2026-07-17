"""Apply Stage F — scoring_cutover_log table (CDS retune closed loop).

Audit trail + active-weights pointer for the automatic scoring re-tune loop
(client Q9). Stage F (src/tasks/scoring_cutover.py) inserts one row per cutover
attempt; the latest row with applied=true is the fit artifact the live scoring
engine overlays onto config/scoring.py at startup.

Idempotent — CREATE ... IF NOT EXISTS guards.

Usage:
    PYTHONPATH=. python migrations/apply_scoring_cutover_log.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS scoring_cutover_log (
        id                SERIAL PRIMARY KEY,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        fit_artifact_path TEXT        NOT NULL,
        validation_status VARCHAR(16) NOT NULL,
        applied           BOOLEAN     NOT NULL DEFAULT false,
        weights_snapshot  JSONB,
        detail            TEXT,
        run_id            VARCHAR(20)
    );
    """,
    # Additive/idempotent for environments where the table already exists
    # from before validation_status/run_id were widened/added.
    "ALTER TABLE scoring_cutover_log ALTER COLUMN validation_status TYPE VARCHAR(16);",
    "ALTER TABLE scoring_cutover_log ADD COLUMN IF NOT EXISTS run_id VARCHAR(20);",
    "CREATE INDEX IF NOT EXISTS ix_scoring_cutover_log_active "
    "ON scoring_cutover_log (applied, created_at DESC);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("scoring_cutover_log complete.")


if __name__ == "__main__":
    main()
