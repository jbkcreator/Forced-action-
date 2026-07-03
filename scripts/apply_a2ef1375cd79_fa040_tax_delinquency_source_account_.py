"""Auto-converted from alembic migration `a2ef1375cd79_fa040_tax_delinquency_source_account_` (revision a2ef1375cd79).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_a2ef1375cd79_fa040_tax_delinquency_source_account_.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE tax_delinquencies ADD COLUMN source_account_number VARCHAR(50);

CREATE INDEX ix_tax_delinquencies_source_account_number ON tax_delinquencies (source_account_number);

ALTER TABLE tax_delinquencies ADD CONSTRAINT uq_tax_delinquency_account_county UNIQUE (source_account_number, county_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied a2ef1375cd79_fa040_tax_delinquency_source_account_")


if __name__ == "__main__":
    main()
