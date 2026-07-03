"""Backfilled from alembic migration `fa029_add_normalized_address`.

Adds properties.normalized_address + btree index + (if pg_trgm present) a GIN
trigram index. Idempotent. Live DB already has this; kept so every schema change
lives in scripts/ (ADR 0024).

Usage:
    PYTHONPATH=. python scripts/apply_fa029_add_normalized_address.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE properties ADD COLUMN IF NOT EXISTS normalized_address VARCHAR(255);
CREATE INDEX IF NOT EXISTS idx_property_normalized_address ON properties (normalized_address);

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE INDEX IF NOT EXISTS idx_property_normalized_address_trgm
            ON properties USING gin (normalized_address gin_trgm_ops);
    ELSE
        RAISE WARNING 'pg_trgm not installed - normalized_address trgm index skipped';
    END IF;
END $$;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa029_add_normalized_address")


if __name__ == "__main__":
    main()
