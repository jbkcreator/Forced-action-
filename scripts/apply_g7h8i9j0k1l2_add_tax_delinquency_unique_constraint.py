"""Auto-converted from alembic migration `g7h8i9j0k1l2_add_tax_delinquency_unique_constraint` (revision g7h8i9j0k1l2).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_g7h8i9j0k1l2_add_tax_delinquency_unique_constraint.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
DELETE FROM tax_delinquencies
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM tax_delinquencies
            GROUP BY property_id, tax_year
        );

ALTER TABLE tax_delinquencies ADD CONSTRAINT uq_tax_delinquency_property_year UNIQUE (property_id, tax_year);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied g7h8i9j0k1l2_add_tax_delinquency_unique_constraint")


if __name__ == "__main__":
    main()
