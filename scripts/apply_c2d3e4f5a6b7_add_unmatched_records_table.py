"""Auto-converted from alembic migration `c2d3e4f5a6b7_add_unmatched_records_table` (revision c2d3e4f5a6b7).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_c2d3e4f5a6b7_add_unmatched_records_table.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE unmatched_records (
    id SERIAL NOT NULL, 
    source_type VARCHAR(50) NOT NULL, 
    county_id VARCHAR(50) DEFAULT 'hillsborough' NOT NULL, 
    raw_data JSON NOT NULL, 
    instrument_number VARCHAR(100), 
    grantor TEXT, 
    address_string TEXT, 
    match_status VARCHAR(20) DEFAULT 'unmatched' NOT NULL, 
    match_attempted_at TIMESTAMP WITH TIME ZONE, 
    matched_property_id INTEGER, 
    date_added TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT fk_unmatched_property FOREIGN KEY(matched_property_id) REFERENCES properties (id)
);

CREATE INDEX ix_unmatched_records_source_type ON unmatched_records (source_type);

CREATE INDEX ix_unmatched_records_county_id ON unmatched_records (county_id);

CREATE INDEX ix_unmatched_records_match_status ON unmatched_records (match_status);

CREATE INDEX ix_unmatched_records_instrument_number ON unmatched_records (instrument_number);

CREATE INDEX ix_unmatched_source_status ON unmatched_records (source_type, match_status);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied c2d3e4f5a6b7_add_unmatched_records_table")


if __name__ == "__main__":
    main()
