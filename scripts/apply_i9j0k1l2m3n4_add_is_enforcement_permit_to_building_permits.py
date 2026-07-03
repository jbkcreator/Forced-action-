"""Auto-converted from alembic migration `i9j0k1l2m3n4_add_is_enforcement_permit_to_building_permits` (revision i9j0k1l2m3n4).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_i9j0k1l2m3n4_add_is_enforcement_permit_to_building_permits.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE building_permits ADD COLUMN is_enforcement_permit BOOLEAN DEFAULT false NOT NULL;

CREATE INDEX idx_building_permits_is_enforcement ON building_permits (is_enforcement_permit);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied i9j0k1l2m3n4_add_is_enforcement_permit_to_building_permits")


if __name__ == "__main__":
    main()
