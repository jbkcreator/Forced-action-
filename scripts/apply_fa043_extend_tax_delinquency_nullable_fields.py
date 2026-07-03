"""Auto-converted from alembic migration `fa043_extend_tax_delinquency_nullable_fields` (revision fa043_extend_tax_delinquency_fields).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa043_extend_tax_delinquency_nullable_fields.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE tax_delinquencies ADD COLUMN source_report VARCHAR(100);

ALTER TABLE tax_delinquencies ADD COLUMN account_number VARCHAR(50);

ALTER TABLE tax_delinquencies ADD COLUMN alternate_key VARCHAR(50);

ALTER TABLE tax_delinquencies ADD COLUMN parcel_number VARCHAR(100);

ALTER TABLE tax_delinquencies ADD COLUMN owner_name VARCHAR(255);

ALTER TABLE tax_delinquencies ADD COLUMN owner_address VARCHAR(500);

ALTER TABLE tax_delinquencies ADD COLUMN property_address VARCHAR(500);

ALTER TABLE tax_delinquencies ADD COLUMN certificate_number VARCHAR(50);

ALTER TABLE tax_delinquencies ADD COLUMN certificate_status VARCHAR(50);

ALTER TABLE tax_delinquencies ADD COLUMN issued_date DATE;

ALTER TABLE tax_delinquencies ADD COLUMN bidder_number VARCHAR(50);

ALTER TABLE tax_delinquencies ADD COLUMN certificate_buyer VARCHAR(255);

ALTER TABLE tax_delinquencies ADD COLUMN certificate_buyer_address VARCHAR(500);

ALTER TABLE tax_delinquencies ADD COLUMN face_amount NUMERIC(12, 2);

ALTER TABLE tax_delinquencies ADD COLUMN account_balance_amount NUMERIC(12, 2);

ALTER TABLE tax_delinquencies ADD COLUMN interest_rate NUMERIC(8, 4);

ALTER TABLE tax_delinquencies ADD COLUMN assessed_value NUMERIC(14, 2);

ALTER TABLE tax_delinquencies ADD COLUMN account_status VARCHAR(50);

ALTER TABLE tax_delinquencies ADD COLUMN deed_status VARCHAR(50);

ALTER TABLE tax_delinquencies ADD COLUMN date_redeemed DATE;

ALTER TABLE tax_delinquencies ADD COLUMN purchased_date DATE;

ALTER TABLE tax_delinquencies ADD COLUMN county_held BOOLEAN;

ALTER TABLE tax_delinquencies ADD COLUMN standard_flags VARCHAR(255);

ALTER TABLE tax_delinquencies ADD COLUMN custom_flags VARCHAR(255);

ALTER TABLE tax_delinquencies ADD COLUMN use_code VARCHAR(50);

ALTER TABLE tax_delinquencies ADD COLUMN raw_source_data JSONB;

ALTER TABLE tax_delinquencies ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE tax_delinquencies ADD COLUMN updated_at TIMESTAMP WITHOUT TIME ZONE;

CREATE INDEX ix_tax_delinquencies_account_number ON tax_delinquencies (account_number);

CREATE INDEX ix_tax_delinquencies_alternate_key ON tax_delinquencies (alternate_key);

CREATE INDEX ix_tax_delinquencies_parcel_number ON tax_delinquencies (parcel_number);

CREATE INDEX ix_tax_delinquencies_owner_name ON tax_delinquencies (owner_name);

CREATE INDEX ix_tax_delinquencies_certificate_number ON tax_delinquencies (certificate_number);

CREATE INDEX ix_tax_delinquencies_certificate_status ON tax_delinquencies (certificate_status);

CREATE INDEX ix_tax_delinquencies_account_status ON tax_delinquencies (account_status);

CREATE INDEX ix_tax_delinquencies_deed_status ON tax_delinquencies (deed_status);

CREATE INDEX idx_tax_delinquency_county_status ON tax_delinquencies (county_id, account_status);

CREATE INDEX idx_tax_delinquency_cert_status ON tax_delinquencies (certificate_status);

CREATE INDEX idx_tax_delinquency_county_account ON tax_delinquencies (county_id, source_account_number);

CREATE INDEX idx_tax_delinquency_parcel ON tax_delinquencies (parcel_number);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa043_extend_tax_delinquency_nullable_fields")


if __name__ == "__main__":
    main()
