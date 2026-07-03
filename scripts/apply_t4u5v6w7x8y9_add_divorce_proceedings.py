"""Auto-converted from alembic migration `t4u5v6w7x8y9_add_divorce_proceedings` (revision t4u5v6w7x8y9).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_t4u5v6w7x8y9_add_divorce_proceedings.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE legal_proceedings DROP CONSTRAINT IF EXISTS check_proceeding_record_type;

ALTER TABLE legal_proceedings ADD CONSTRAINT check_proceeding_record_type CHECK (record_type IN ('Probate', 'Eviction', 'Bankruptcy', 'Divorce'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied t4u5v6w7x8y9_add_divorce_proceedings")


if __name__ == "__main__":
    main()
