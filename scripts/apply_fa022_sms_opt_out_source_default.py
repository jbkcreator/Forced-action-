"""Auto-converted from alembic migration `fa022_sms_opt_out_source_default` (revision fa022_sms_opt_out_source_default).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa022_sms_opt_out_source_default.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE sms_opt_outs ALTER COLUMN source SET DEFAULT 'inbound_sms';
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa022_sms_opt_out_source_default")


if __name__ == "__main__":
    main()
