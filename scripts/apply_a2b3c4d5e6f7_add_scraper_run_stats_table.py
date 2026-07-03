"""Auto-converted from alembic migration `a2b3c4d5e6f7_add_scraper_run_stats_table` (revision a2b3c4d5e6f7).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_a2b3c4d5e6f7_add_scraper_run_stats_table.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE scraper_run_stats (
    id SERIAL NOT NULL, 
    run_date DATE NOT NULL, 
    source_type VARCHAR(50) NOT NULL, 
    county_id VARCHAR(50) DEFAULT 'hillsborough' NOT NULL, 
    total_scraped INTEGER DEFAULT '0' NOT NULL, 
    matched INTEGER DEFAULT '0' NOT NULL, 
    unmatched INTEGER DEFAULT '0' NOT NULL, 
    skipped INTEGER DEFAULT '0' NOT NULL, 
    scored INTEGER DEFAULT '0' NOT NULL, 
    run_success BOOLEAN DEFAULT 'true' NOT NULL, 
    error_message TEXT, 
    duration_seconds NUMERIC(10, 2), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_scraper_run_stats UNIQUE (run_date, source_type, county_id), 
    CONSTRAINT check_run_stats_source_type CHECK (source_type IN ('lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl','judgments', 'deeds', 'evictions', 'probate', 'bankruptcy','violations', 'foreclosures', 'permits', 'tax_delinquencies','roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents'))
);

CREATE INDEX idx_run_stats_run_date ON scraper_run_stats (run_date);

CREATE INDEX idx_run_stats_source_type ON scraper_run_stats (source_type);

CREATE INDEX idx_run_stats_county_id ON scraper_run_stats (county_id);

CREATE INDEX idx_run_stats_date_source ON scraper_run_stats (run_date, source_type);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied a2b3c4d5e6f7_add_scraper_run_stats_table")


if __name__ == "__main__":
    main()
