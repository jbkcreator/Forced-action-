"""Auto-converted from alembic migration `fa063_dbpr_work_email_linkedin` (revision fa063).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa063_dbpr_work_email_linkedin.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE dbpr_contacts ADD COLUMN work_email VARCHAR(200);

ALTER TABLE dbpr_contacts ADD COLUMN linkedin_url VARCHAR(500);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa063_dbpr_work_email_linkedin")


if __name__ == "__main__":
    main()
