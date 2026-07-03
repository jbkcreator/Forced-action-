"""Auto-converted from alembic migration `fa012_stage6_subscriber_payment_recovery_fields` (revision fa012_stage6).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa012_stage6_subscriber_payment_recovery_fields.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers ADD COLUMN payment_failed_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE subscribers ADD COLUMN recovery_day1_sent BOOLEAN DEFAULT 'false' NOT NULL;

ALTER TABLE subscribers ADD COLUMN recovery_day3_sent BOOLEAN DEFAULT 'false' NOT NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa012_stage6_subscriber_payment_recovery_fields")


if __name__ == "__main__":
    main()
