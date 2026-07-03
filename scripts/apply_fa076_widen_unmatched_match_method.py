"""Auto-converted from alembic migration `fa076_widen_unmatched_match_method` (revision fa076_widen_unmatched_match_method).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa076_widen_unmatched_match_method.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE unmatched_records DROP CONSTRAINT IF EXISTS check_unmatched_match_method;

ALTER TABLE unmatched_records ADD CONSTRAINT check_unmatched_match_method CHECK (match_method IN ('address', 'normalized_address', 'owner_name', 'owner_name_zip', 'owner_name_city', 'legal_desc', 'parcel_id') OR match_method IS NULL);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa076_widen_unmatched_match_method")


if __name__ == "__main__":
    main()
