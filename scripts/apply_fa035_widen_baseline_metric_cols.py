"""Auto-converted from alembic migration `fa035_widen_baseline_metric_cols` (revision fa035_widen_baseline_metric_cols).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa035_widen_baseline_metric_cols.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE platform_daily_stats ALTER COLUMN sms_reply_rate TYPE NUMERIC(7, 4);

ALTER TABLE platform_daily_stats ALTER COLUMN offer_acceptance_rate TYPE NUMERIC(7, 4);

ALTER TABLE platform_daily_stats ALTER COLUMN first_payment_rate TYPE NUMERIC(7, 4);

ALTER TABLE platform_daily_stats ALTER COLUMN saved_card_rate TYPE NUMERIC(7, 4);

ALTER TABLE platform_daily_stats ALTER COLUMN wallet_adoption TYPE NUMERIC(7, 4);

ALTER TABLE platform_daily_stats ALTER COLUMN lock_conversion TYPE NUMERIC(7, 4);

ALTER TABLE platform_daily_stats ALTER COLUMN retention_30d TYPE NUMERIC(7, 4);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa035_widen_baseline_metric_cols")


if __name__ == "__main__":
    main()
