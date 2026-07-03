"""Auto-converted from alembic migration `fa042_parcel_id_normalized_index` (revision fa042_parcel_id_normalized).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa042_parcel_id_normalized_index.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
COMMIT;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_property_parcel_id_normalized
            ON properties (regexp_replace(parcel_id, '[^A-Za-z0-9]', '', 'g'), county_id);

BEGIN;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa042_parcel_id_normalized_index")


if __name__ == "__main__":
    main()
