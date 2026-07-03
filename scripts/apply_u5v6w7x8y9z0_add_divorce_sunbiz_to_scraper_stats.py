"""Auto-converted from alembic migration `u5v6w7x8y9z0_add_divorce_sunbiz_to_scraper_stats` (revision u5v6w7x8y9z0).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_u5v6w7x8y9z0_add_divorce_sunbiz_to_scraper_stats.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE scraper_run_stats DROP CONSTRAINT check_run_stats_source_type;

ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type CHECK (source_type IN ('lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl','judgments', 'deeds', 'evictions', 'probate', 'bankruptcy','violations', 'foreclosures', 'permits', 'tax_delinquencies','roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents','lis_pendens', 'divorce_filings', 'sunbiz'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied u5v6w7x8y9z0_add_divorce_sunbiz_to_scraper_stats")


if __name__ == "__main__":
    main()
