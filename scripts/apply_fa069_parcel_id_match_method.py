"""Auto-converted from alembic migration `fa069_parcel_id_match_method` (revision fa069_parcel_id_match_method).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa069_parcel_id_match_method.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE legal_and_liens DROP CONSTRAINT check_legal_match_method;

ALTER TABLE legal_and_liens ADD CONSTRAINT check_legal_match_method CHECK (match_method IN ('parcel_id', 'legal_desc', 'owner_name', 'llm_verified', 'address', 'manual'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa069_parcel_id_match_method")


if __name__ == "__main__":
    main()
