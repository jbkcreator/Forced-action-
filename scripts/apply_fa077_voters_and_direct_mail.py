"""Auto-converted from alembic migration `fa077_voters_and_direct_mail` (revision fa077_voters_and_direct_mail).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa077_voters_and_direct_mail.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE voters (
    id SERIAL NOT NULL, 
    property_id INTEGER NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    source_voter_id VARCHAR(20) NOT NULL, 
    voter_name VARCHAR(255), 
    first_name VARCHAR(100), 
    middle_name VARCHAR(100), 
    last_name VARCHAR(100), 
    residential_address VARCHAR(500), 
    residential_city VARCHAR(100), 
    residential_zip VARCHAR(10), 
    mailing_address VARCHAR(500), 
    registration_status VARCHAR(10), 
    registration_date DATE, 
    phones JSONB, 
    phone_1 VARCHAR(20), 
    email VARCHAR(255), 
    meta_data JSONB, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_voter_county_source_id UNIQUE (county_id, source_voter_id), 
    CONSTRAINT check_voter_registration_status CHECK (registration_status IN ('ACT', 'INA') OR registration_status IS NULL), 
    FOREIGN KEY(property_id) REFERENCES properties (id)
);

CREATE INDEX ix_voters_property_id ON voters (property_id);

CREATE INDEX ix_voters_county_id ON voters (county_id);

CREATE INDEX ix_voters_voter_name ON voters (voter_name);

CREATE INDEX idx_voter_registration_status ON voters (registration_status);

ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source;

ALTER TABLE enriched_contacts ADD CONSTRAINT check_enriched_source CHECK (source IN ('batch_skip_tracing', 'idi', 'pdl', 'tracerfy', 'tax_collector'));

ALTER TABLE owners ADD COLUMN direct_mail_eligible BOOLEAN DEFAULT 'false' NOT NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa077_voters_and_direct_mail")


if __name__ == "__main__":
    main()
