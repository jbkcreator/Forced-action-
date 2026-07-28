"""Auto-converted from alembic migration `fa018_nws_alerts_table` (revision fa018_nws_alerts_table).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa018_nws_alerts_table.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE nws_alerts (
    id SERIAL NOT NULL, 
    alert_id VARCHAR(200) NOT NULL, 
    event VARCHAR(100) NOT NULL, 
    severity VARCHAR(30), 
    urgency VARCHAR(30), 
    certainty VARCHAR(30), 
    headline TEXT, 
    description TEXT, 
    instruction TEXT, 
    area_desc TEXT, 
    same_codes JSONB, 
    ugc_codes JSONB, 
    affected_zips JSONB, 
    effective TIMESTAMP WITH TIME ZONE, 
    onset TIMESTAMP WITH TIME ZONE, 
    expires TIMESTAMP WITH TIME ZONE, 
    ends TIMESTAMP WITH TIME ZONE, 
    county_id VARCHAR(50) DEFAULT 'hillsborough' NOT NULL, 
    storm_pack_triggered BOOLEAN DEFAULT 'false' NOT NULL, 
    lifecycle_urgency_sent BOOLEAN DEFAULT 'false' NOT NULL, 
    subscriber_count INTEGER DEFAULT '0' NOT NULL, 
    raw_payload JSONB, 
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    UNIQUE (alert_id)
);

CREATE UNIQUE INDEX ix_nws_alerts_alert_id ON nws_alerts (alert_id);

CREATE INDEX ix_nws_alerts_county_id ON nws_alerts (county_id);

CREATE INDEX ix_nws_alerts_processed_at ON nws_alerts (processed_at);

CREATE INDEX ix_nws_alerts_event_processed ON nws_alerts (event, processed_at);

CREATE INDEX ix_nws_alerts_affected_zips_gin ON nws_alerts USING gin (affected_zips);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa018_nws_alerts_table")


if __name__ == "__main__":
    main()
