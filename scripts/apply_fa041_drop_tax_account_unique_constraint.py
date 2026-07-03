"""Auto-converted from alembic migration `fa041_drop_tax_account_unique_constraint` (revision fa041_drop_tax_account_unique).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa041_drop_tax_account_unique_constraint.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE tax_delinquencies DROP CONSTRAINT uq_tax_delinquency_account_county;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa041_drop_tax_account_unique_constraint")


if __name__ == "__main__":
    main()
