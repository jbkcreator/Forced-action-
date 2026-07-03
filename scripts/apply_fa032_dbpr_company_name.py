"""Auto-converted from alembic migration `fa032_dbpr_company_name` (revision fa032_dbpr_company_name).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa032_dbpr_company_name.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS company_name VARCHAR(255);

ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS company_name_status VARCHAR(20) NOT NULL DEFAULT 'pending';

ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS company_name_scraped_at TIMESTAMPTZ;

ALTER TABLE dbpr_contacts DROP CONSTRAINT IF EXISTS check_dbpr_company_name_status;

ALTER TABLE dbpr_contacts ADD CONSTRAINT check_dbpr_company_name_status CHECK (company_name_status IN ('pending', 'found', 'none', 'failed'));

CREATE INDEX IF NOT EXISTS ix_dbpr_company_name_status ON dbpr_contacts (company_name_status);

ALTER TABLE scraper_run_stats DROP CONSTRAINT check_run_stats_source_type;

ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type CHECK (source_type IN ('lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens','judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy','violations', 'foreclosures', 'permits', 'tax_delinquencies','roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents','sunbiz', 'property_appraiser', 'dbpr_company'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa032_dbpr_company_name")


if __name__ == "__main__":
    main()
