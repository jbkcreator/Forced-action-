"""Auto-converted from alembic migration `fa103_tax_deed_vacant_land` (revision fa103).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa103_tax_deed_vacant_land.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE tax_deed_auctions (
    id SERIAL NOT NULL, 
    property_id INTEGER, 
    county_id VARCHAR(50) NOT NULL, 
    parcel_id VARCHAR(100), 
    auction_date DATE NOT NULL, 
    case_number VARCHAR(100) NOT NULL, 
    certificate_number VARCHAR(50), 
    certificate_year SMALLINT, 
    status VARCHAR(50), 
    auction_type VARCHAR(50), 
    opening_bid NUMERIC(14, 2), 
    sold_amount NUMERIC(14, 2), 
    sold_to VARCHAR(255), 
    raw_fields JSONB, 
    match_method VARCHAR(30), 
    match_confidence NUMERIC(4, 3), 
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_tax_deed_auction UNIQUE (county_id, auction_date, case_number), 
    FOREIGN KEY(property_id) REFERENCES properties (id) ON DELETE SET NULL
);

CREATE INDEX ix_tax_deed_auctions_property_id ON tax_deed_auctions (property_id);

CREATE INDEX ix_tax_deed_auctions_county_date ON tax_deed_auctions (county_id, auction_date);

CREATE INDEX ix_tax_deed_auctions_parcel_id ON tax_deed_auctions (parcel_id);

CREATE TABLE vacant_parcels (
    id SERIAL NOT NULL, 
    property_id INTEGER, 
    county_id VARCHAR(50) NOT NULL, 
    parcel_id VARCHAR(100) NOT NULL, 
    use_code VARCHAR(20), 
    property_use VARCHAR(200), 
    dor_code VARCHAR(20), 
    source_name VARCHAR(20) NOT NULL, 
    last_verified DATE NOT NULL, 
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_vacant_parcel UNIQUE (county_id, parcel_id), 
    FOREIGN KEY(property_id) REFERENCES properties (id) ON DELETE SET NULL
);

CREATE INDEX ix_vacant_parcels_property_id ON vacant_parcels (property_id);

CREATE INDEX ix_vacant_parcels_county_id ON vacant_parcels (county_id);

ALTER TABLE scraper_run_stats DROP CONSTRAINT check_run_stats_source_type;

ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type CHECK (source_type IN ('lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens','judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy','violations', 'foreclosures', 'permits', 'tax_delinquencies','roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents','sunbiz', 'property_appraiser', 'dbpr_company','tax_deed_auction', 'vacant_land'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa103_tax_deed_vacant_land")


if __name__ == "__main__":
    main()
