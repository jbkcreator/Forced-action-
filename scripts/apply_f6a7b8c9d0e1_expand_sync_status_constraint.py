"""Auto-converted from alembic migration `f6a7b8c9d0e1_expand_sync_status_constraint` (revision f6a7b8c9d0e1).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_f6a7b8c9d0e1_expand_sync_status_constraint.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE properties DROP CONSTRAINT check_sync_status;

ALTER TABLE properties ADD CONSTRAINT check_sync_status CHECK (sync_status IN ('pending', 'pending_sync', 'synced', 'sync_failed', 'error'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied f6a7b8c9d0e1_expand_sync_status_constraint")


if __name__ == "__main__":
    main()
