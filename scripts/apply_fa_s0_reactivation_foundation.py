"""Backfilled from alembic migration `fa_s0_reactivation_foundation`.

subscribers.last_reactivation_attempt_at (cooldown gate) + gold_plus_zip_snapshots
(nightly Gold+ supply per ZIP). Idempotent. Live DB already has this; kept so
every schema change lives in scripts/ (ADR 0024).

Usage:
    PYTHONPATH=. python scripts/apply_fa_s0_reactivation_foundation.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers
    ADD COLUMN IF NOT EXISTS last_reactivation_attempt_at TIMESTAMP WITH TIME ZONE;
CREATE INDEX IF NOT EXISTS idx_subscriber_last_reactivation_at
    ON subscribers (last_reactivation_attempt_at);

CREATE TABLE IF NOT EXISTS gold_plus_zip_snapshots (
    id                    SERIAL PRIMARY KEY,
    zip_code              VARCHAR(10) NOT NULL,
    county_id             VARCHAR(50) NOT NULL,
    snapshot_date         DATE NOT NULL,
    gold_plus_lead_count  INTEGER NOT NULL DEFAULT 0,
    computed_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uq_gpzs_zip_county_date UNIQUE (zip_code, county_id, snapshot_date)
);
CREATE INDEX IF NOT EXISTS idx_gpzs_zip_county_date
    ON gold_plus_zip_snapshots (zip_code, county_id, snapshot_date);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa_s0_reactivation_foundation")


if __name__ == "__main__":
    main()
