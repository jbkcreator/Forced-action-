"""Auto-converted from alembic migration `fa105_drop_prospect_id_denorm` (revision fa105_drop_prospect_id_denorm).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa105_drop_prospect_id_denorm.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
DROP INDEX IF EXISTS idx_bt_prospect_id;

ALTER TABLE broker_transitions DROP COLUMN prospect_id;

ALTER TABLE commission_ledger DROP COLUMN prospect_id;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa105_drop_prospect_id_denorm")


if __name__ == "__main__":
    main()
