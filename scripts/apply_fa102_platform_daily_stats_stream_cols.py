"""Apply fa102 - stream metric columns on platform_daily_stats.

Adds enrichment_rate, dialable_rate, sms_delivery_rate, closer_conv_rate
as NUMERIC(7,4) NULL (fraction 0–1).

Idempotent — ADD COLUMN IF NOT EXISTS.

Usage:
    PYTHONPATH=. python scripts/apply_fa102_platform_daily_stats_stream_cols.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE platform_daily_stats ADD COLUMN IF NOT EXISTS enrichment_rate   NUMERIC(7,4);",
    "ALTER TABLE platform_daily_stats ADD COLUMN IF NOT EXISTS dialable_rate      NUMERIC(7,4);",
    "ALTER TABLE platform_daily_stats ADD COLUMN IF NOT EXISTS sms_delivery_rate  NUMERIC(7,4);",
    "ALTER TABLE platform_daily_stats ADD COLUMN IF NOT EXISTS closer_conv_rate   NUMERIC(7,4);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("Step %d/%d — %s", i, len(DDL), stmt.strip())
            conn.execute(text(stmt.strip()))

    logger.info("fa102 complete — stream metric columns added to platform_daily_stats.")


if __name__ == "__main__":
    main()
