"""Auto-converted from alembic migration `7baab3a62175_add_hcpa_enrichment_fields_and_tax_` (revision 7baab3a62175).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_7baab3a62175_add_hcpa_enrichment_fields_and_tax_.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE tax_payment_history (
    id SERIAL NOT NULL, 
    property_id INTEGER NOT NULL, 
    tax_year INTEGER NOT NULL, 
    bill_type VARCHAR(50), 
    amount_paid NUMERIC(10, 2), 
    payment_date DATE, 
    receipt_number VARCHAR(50), 
    days_late INTEGER, 
    county_id VARCHAR(50) NOT NULL, 
    date_added DATE, 
    PRIMARY KEY (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id), 
    CONSTRAINT uq_tax_payment_property_year_type UNIQUE (property_id, tax_year, bill_type)
);

CREATE INDEX idx_tax_payment_bill_type ON tax_payment_history (bill_type);

CREATE INDEX idx_tax_payment_date ON tax_payment_history (payment_date);

CREATE INDEX idx_tax_payment_property_year ON tax_payment_history (property_id, tax_year);

CREATE INDEX ix_tax_payment_history_property_id ON tax_payment_history (property_id);

ALTER TABLE deeds ADD COLUMN sale_qualified BOOLEAN;

ALTER TABLE deeds ADD COLUMN vacant_improved VARCHAR(20);

ALTER TABLE financials ADD COLUMN exemption_code VARCHAR(10);

ALTER TABLE financials ADD COLUMN soh_assessment_reduction NUMERIC(12, 2);

ALTER TABLE financials ADD COLUMN taxable_value_county NUMERIC(12, 2);

ALTER TABLE financials ADD COLUMN taxable_value_schools NUMERIC(12, 2);

ALTER TABLE financials ADD COLUMN prior_year_market_value NUMERIC(12, 2);

ALTER TABLE financials ADD COLUMN proposed_next_assessed NUMERIC(12, 2);

ALTER TABLE financials ADD COLUMN tax_current_status VARCHAR(20);

ALTER TABLE financials ADD COLUMN tax_last_paid_amount NUMERIC(10, 2);

ALTER TABLE financials ADD COLUMN tax_last_paid_date DATE;

ALTER TABLE financials ADD COLUMN hcpa_refreshed_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE properties ADD COLUMN property_use_code VARCHAR(20);

ALTER TABLE properties ADD COLUMN building_condition VARCHAR(20);

ALTER TABLE properties ADD COLUMN building_class VARCHAR(5);

ALTER TABLE properties ADD COLUMN heated_sq_ft NUMERIC(10, 2);

ALTER TABLE properties ADD COLUMN subdivision VARCHAR(255);

ALTER TABLE properties ADD COLUMN hcpa_neighborhood_code VARCHAR(50);

ALTER TABLE properties ADD COLUMN building_details JSONB;

ALTER TABLE properties ADD COLUMN hcpa_last_refreshed TIMESTAMP WITHOUT TIME ZONE;

CREATE INDEX idx_property_building_condition ON properties (building_condition);

CREATE INDEX idx_property_building_details ON properties USING gin (building_details);

CREATE INDEX idx_property_hcpa_refreshed ON properties (hcpa_last_refreshed);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 7baab3a62175_add_hcpa_enrichment_fields_and_tax_")


if __name__ == "__main__":
    main()
