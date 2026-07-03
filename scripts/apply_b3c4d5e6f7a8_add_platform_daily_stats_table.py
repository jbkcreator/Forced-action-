"""Auto-converted from alembic migration `b3c4d5e6f7a8_add_platform_daily_stats_table` (revision b3c4d5e6f7a8).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_b3c4d5e6f7a8_add_platform_daily_stats_table.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE platform_daily_stats (
    id SERIAL NOT NULL, 
    run_date DATE NOT NULL, 
    county_id VARCHAR(50) DEFAULT 'hillsborough' NOT NULL, 
    signals_scraped INTEGER DEFAULT '0' NOT NULL, 
    signals_matched INTEGER DEFAULT '0' NOT NULL, 
    signals_skipped INTEGER DEFAULT '0' NOT NULL, 
    properties_scored INTEGER DEFAULT '0' NOT NULL, 
    properties_with_signals INTEGER DEFAULT '0' NOT NULL, 
    score_runs_total INTEGER DEFAULT '0' NOT NULL, 
    leads_new INTEGER DEFAULT '0' NOT NULL, 
    leads_updated INTEGER DEFAULT '0' NOT NULL, 
    leads_unchanged INTEGER DEFAULT '0' NOT NULL, 
    leads_qualified INTEGER DEFAULT '0' NOT NULL, 
    leads_upgraded INTEGER DEFAULT '0' NOT NULL, 
    tier_ultra_platinum INTEGER DEFAULT '0' NOT NULL, 
    tier_platinum INTEGER DEFAULT '0' NOT NULL, 
    tier_gold INTEGER DEFAULT '0' NOT NULL, 
    tier_silver INTEGER DEFAULT '0' NOT NULL, 
    tier_bronze INTEGER DEFAULT '0' NOT NULL, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_platform_daily_stats UNIQUE (run_date, county_id)
);

CREATE INDEX idx_platform_stats_date ON platform_daily_stats (run_date);

CREATE INDEX idx_platform_stats_county_id ON platform_daily_stats (county_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied b3c4d5e6f7a8_add_platform_daily_stats_table")


if __name__ == "__main__":
    main()
