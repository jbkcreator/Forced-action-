"""Auto-converted from alembic migration `fa013_phone_deliverability_snapshots` (revision fa013_phone_deliv).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa013_phone_deliverability_snapshots.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE phone_deliverability_snapshots (
    id SERIAL NOT NULL, 
    snapshot_date DATE NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    tier_filter VARCHAR(40) DEFAULT 'gold_plus' NOT NULL, 
    sample_size INTEGER NOT NULL, 
    lookups_cached INTEGER DEFAULT '0' NOT NULL, 
    lookups_attempted INTEGER DEFAULT '0' NOT NULL, 
    lookups_succeeded INTEGER DEFAULT '0' NOT NULL, 
    mobile_count INTEGER DEFAULT '0' NOT NULL, 
    voip_count INTEGER DEFAULT '0' NOT NULL, 
    landline_count INTEGER DEFAULT '0' NOT NULL, 
    unknown_count INTEGER DEFAULT '0' NOT NULL, 
    no_phone_count INTEGER DEFAULT '0' NOT NULL, 
    mobile_pct NUMERIC(5, 2), 
    vendor VARCHAR(20) DEFAULT 'telnyx' NOT NULL, 
    cost_cents INTEGER DEFAULT '0', 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_phone_deliv_snapshot_day UNIQUE (snapshot_date, county_id, tier_filter)
);

CREATE INDEX ix_phone_deliverability_snapshots_snapshot_date ON phone_deliverability_snapshots (snapshot_date);

CREATE INDEX ix_phone_deliverability_snapshots_county_id ON phone_deliverability_snapshots (county_id);

CREATE INDEX idx_phone_deliv_date_county ON phone_deliverability_snapshots (snapshot_date, county_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa013_phone_deliverability_snapshots")


if __name__ == "__main__":
    main()
