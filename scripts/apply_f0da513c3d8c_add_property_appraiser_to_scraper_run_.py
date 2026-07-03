"""Auto-converted from alembic migration `f0da513c3d8c_add_property_appraiser_to_scraper_run_` (revision f0da513c3d8c).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_f0da513c3d8c_add_property_appraiser_to_scraper_run_.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE scraper_run_stats DROP CONSTRAINT check_run_stats_source_type;

ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type CHECK (source_type IN ('lien_tcl','lien_ccl','lien_hoa','lien_ml','lien_tl','lis_pendens','judgments','deeds','evictions','divorce_filings','probate','bankruptcy','violations','foreclosures','permits','tax_delinquencies','roofing_permits','storm_damage','flood_damage','insurance_claims','fire_incidents','sunbiz','property_appraiser'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied f0da513c3d8c_add_property_appraiser_to_scraper_run_")


if __name__ == "__main__":
    main()
