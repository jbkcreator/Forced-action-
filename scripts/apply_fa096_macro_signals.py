"""Apply fa096 — A7 macro_signals table.

Companion to alembic/versions/fa096_macro_signals.py.
The Alembic CLI is unusable in this repo's multi-head tree, so this script
applies the same DDL directly.

Idempotent: uses CREATE TABLE IF NOT EXISTS and CREATE INDEX IF NOT EXISTS.

Usage:
    PYTHONPATH=. python scripts/apply_fa096_macro_signals.py
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS macro_signals (
        id          UUID NOT NULL DEFAULT generate_uuidv7() PRIMARY KEY,
        source      VARCHAR(30)  NOT NULL,
        signal_key  VARCHAR(80)  NOT NULL,
        source_series_id VARCHAR(120) NOT NULL DEFAULT '',
        value       NUMERIC(18, 6) NOT NULL,
        unit        VARCHAR(30)  NOT NULL,
        observed_at DATE         NOT NULL,
        frequency   VARCHAR(20)  NOT NULL,
        geography_scope VARCHAR(50) NOT NULL,
        geography_id    VARCHAR(30) NOT NULL,
        raw_payload JSONB,
        created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_macro_signal_observation
            UNIQUE (source, signal_key, source_series_id, observed_at, geography_scope, geography_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_macro_signals_source ON macro_signals (source)",
    "CREATE INDEX IF NOT EXISTS idx_macro_signals_signal_key ON macro_signals (signal_key)",
    "CREATE INDEX IF NOT EXISTS idx_macro_signals_observed_at ON macro_signals (observed_at)",
    """
    CREATE INDEX IF NOT EXISTS idx_macro_signals_source_key_date
        ON macro_signals (source, signal_key, observed_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_macro_signals_geo
        ON macro_signals (geography_scope, geography_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_macro_signals_source_geo
        ON macro_signals (source, geography_scope, geography_id)
    """,
]

STAMP = """
INSERT INTO alembic_version (version_num)
VALUES ('fa096_macro_signals')
ON CONFLICT DO NOTHING;
"""


def main() -> None:
    settings = get_settings()
    engine = create_engine(str(settings.database_url))
    with engine.begin() as conn:
        logger.info("Applying fa096: creating macro_signals table...")
        for stmt in DDL:
            conn.execute(text(stmt))
            logger.info("OK: %s", " ".join(stmt.split())[:100])
        conn.execute(text(STAMP))
        logger.info("Done - fa096 macro_signals applied and alembic_version stamped.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        sys.exit(1)
