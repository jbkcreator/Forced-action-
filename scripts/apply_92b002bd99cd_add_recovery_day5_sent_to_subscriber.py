"""Auto-converted from alembic migration `92b002bd99cd_add_recovery_day5_sent_to_subscriber` (revision 92b002bd99cd).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_92b002bd99cd_add_recovery_day5_sent_to_subscriber.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE counties ADD COLUMN address_city_tokens JSONB;

ALTER TABLE subscribers ADD COLUMN recovery_day5_sent BOOLEAN DEFAULT 'false' NOT NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 92b002bd99cd_add_recovery_day5_sent_to_subscriber")


if __name__ == "__main__":
    main()
