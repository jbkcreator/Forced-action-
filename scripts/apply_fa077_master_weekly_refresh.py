"""Auto-converted from alembic migration `fa077_master_weekly_refresh` (revision fa077_master_weekly_refresh).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa077_master_weekly_refresh.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE properties ADD COLUMN source_row_hash VARCHAR(32);

ALTER TABLE properties ADD COLUMN last_seen_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE properties ADD COLUMN needs_rescore BOOLEAN DEFAULT false NOT NULL;

CREATE INDEX idx_properties_needs_rescore ON properties (id) WHERE needs_rescore;

ALTER TABLE owners ADD COLUMN skip_trace_stale BOOLEAN DEFAULT false NOT NULL;

CREATE INDEX idx_owner_skip_trace_stale ON owners (id) WHERE skip_trace_stale;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa077_master_weekly_refresh")


if __name__ == "__main__":
    main()
