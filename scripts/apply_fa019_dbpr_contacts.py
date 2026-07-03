"""Auto-converted from alembic migration `fa019_dbpr_contacts` (revision fa019_dbpr_contacts).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa019_dbpr_contacts.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE dbpr_contacts (
    id SERIAL NOT NULL, 
    license_number VARCHAR(30) NOT NULL, 
    license_type_code VARCHAR(10) NOT NULL, 
    license_type_desc VARCHAR(60), 
    full_name VARCHAR(200) NOT NULL, 
    address VARCHAR(255), 
    city VARCHAR(100), 
    state VARCHAR(5) DEFAULT 'FL', 
    zip_code VARCHAR(10), 
    county_id VARCHAR(50), 
    license_expiry DATE, 
    data_source VARCHAR(20) DEFAULT 'certified' NOT NULL, 
    vertical VARCHAR(50), 
    email VARCHAR(200), 
    phone VARCHAR(20), 
    enrichment_status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    enrichment_attempted_at TIMESTAMP WITH TIME ZONE, 
    email_status VARCHAR(20) DEFAULT 'not_sent' NOT NULL, 
    email_sent_at TIMESTAMP WITH TIME ZONE, 
    subscriber_id INTEGER, 
    signed_up_at TIMESTAMP WITH TIME ZONE, 
    last_synced_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_dbpr_enrichment_status CHECK (enrichment_status IN ('pending', 'enriched', 'failed', 'skipped')), 
    CONSTRAINT check_dbpr_email_status CHECK (email_status IN ('not_sent', 'sent', 'bounced', 'signed_up', 'opted_out')), 
    CONSTRAINT check_dbpr_data_source CHECK (data_source IN ('certified', 'registered')), 
    UNIQUE (license_number), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE UNIQUE INDEX ix_dbpr_license_number ON dbpr_contacts (license_number);

CREATE INDEX ix_dbpr_zip_code ON dbpr_contacts (zip_code);

CREATE INDEX ix_dbpr_county_id ON dbpr_contacts (county_id);

CREATE INDEX ix_dbpr_vertical ON dbpr_contacts (vertical);

CREATE INDEX ix_dbpr_county_vertical ON dbpr_contacts (county_id, vertical);

CREATE INDEX ix_dbpr_enrichment_status ON dbpr_contacts (enrichment_status);

CREATE INDEX ix_dbpr_email_status ON dbpr_contacts (email_status);

CREATE INDEX ix_dbpr_subscriber_id ON dbpr_contacts (subscriber_id);

CREATE INDEX ix_dbpr_last_synced ON dbpr_contacts (last_synced_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa019_dbpr_contacts")


if __name__ == "__main__":
    main()
