"""Backfilled from alembic migration `b1c2d3e4f5a6_add_trgm_indexes_for_matching`.

GIN trigram indexes for fuzzy matching. Requires the pg_trgm extension (an app
user cannot create it — a superuser must run
`CREATE EXTENSION IF NOT EXISTS pg_trgm` first). If pg_trgm is absent the index
creation is skipped with a warning, mirroring the original migration. Idempotent.
Live DB already has these; kept so every schema change lives in scripts/ (ADR 0024).

Usage:
    PYTHONPATH=. python scripts/apply_b1c2d3e4f5a6_add_trgm_indexes_for_matching.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE INDEX IF NOT EXISTS idx_owner_name_trgm
            ON owners USING gin (owner_name gin_trgm_ops);
        CREATE INDEX IF NOT EXISTS idx_property_legal_desc_trgm
            ON properties USING gin (legal_description gin_trgm_ops);
        CREATE INDEX IF NOT EXISTS idx_property_address_trgm
            ON properties USING gin (address gin_trgm_ops);
    ELSE
        RAISE WARNING 'pg_trgm not installed - trgm indexes skipped (superuser must CREATE EXTENSION pg_trgm)';
    END IF;
END $$;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied b1c2d3e4f5a6_add_trgm_indexes_for_matching")


if __name__ == "__main__":
    main()
