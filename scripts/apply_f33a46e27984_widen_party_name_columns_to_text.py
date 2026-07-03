"""Auto-converted from alembic migration `f33a46e27984_widen_party_name_columns_to_text` (revision f33a46e27984).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_f33a46e27984_widen_party_name_columns_to_text.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE deeds ALTER COLUMN grantor TYPE TEXT;

ALTER TABLE deeds ALTER COLUMN grantee TYPE TEXT;

ALTER TABLE foreclosures ALTER COLUMN plaintiff TYPE TEXT;

ALTER TABLE legal_and_liens ALTER COLUMN creditor TYPE TEXT;

ALTER TABLE legal_and_liens ALTER COLUMN debtor TYPE TEXT;

ALTER TABLE legal_proceedings ALTER COLUMN associated_party TYPE TEXT;

ALTER TABLE legal_proceedings ALTER COLUMN secondary_party TYPE TEXT;

ALTER TABLE owners ALTER COLUMN owner_name TYPE TEXT;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied f33a46e27984_widen_party_name_columns_to_text")


if __name__ == "__main__":
    main()
