"""Auto-converted from alembic migration `1dfb55b61ac6_unique_constraint_on_unmatched_records_` (revision 1dfb55b61ac6).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_1dfb55b61ac6_unique_constraint_on_unmatched_records_.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE UNIQUE INDEX uq_unmatched_instrument_source_county ON unmatched_records (instrument_number, source_type, county_id) WHERE instrument_number IS NOT NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 1dfb55b61ac6_unique_constraint_on_unmatched_records_")


if __name__ == "__main__":
    main()
