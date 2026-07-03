"""Auto-converted from alembic migration `fa008_enrichment_usage_logs` (revision fa008_enrichment_usage_logs).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa008_enrichment_usage_logs.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE enrichment_usage_logs (
    id SERIAL NOT NULL, 
    vendor VARCHAR(30) NOT NULL, 
    purpose VARCHAR(40) NOT NULL, 
    subscriber_id INTEGER, 
    property_id INTEGER, 
    target_address VARCHAR(255), 
    cost_cents INTEGER DEFAULT '0' NOT NULL, 
    success BOOLEAN DEFAULT false NOT NULL, 
    error VARCHAR(255), 
    request_ref VARCHAR(100), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id)
);

CREATE INDEX ix_enrichment_subscriber_id ON enrichment_usage_logs (subscriber_id);

CREATE INDEX ix_enrichment_property_id ON enrichment_usage_logs (property_id);

CREATE INDEX ix_enrichment_created_at ON enrichment_usage_logs (created_at);

CREATE INDEX idx_enrichment_purpose_created ON enrichment_usage_logs (purpose, created_at);

CREATE INDEX idx_enrichment_vendor_created ON enrichment_usage_logs (vendor, created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa008_enrichment_usage_logs")


if __name__ == "__main__":
    main()
