"""Apply CDE-02 go-live — add foreclosure_outcomes to
ScraperRunStats.check_run_stats_source_type.

Deferred from registry scaffolding by design (same pattern as
tax_deed_outcomes/appraiser_sale_outcomes) -- each connector adds its own
source_type at go-live, not before.

Idempotent -- DROP CONSTRAINT IF EXISTS before re-adding.

Usage:
    PYTHONPATH=. python migrations/apply_cde02_scraper_run_stats_source_type.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE scraper_run_stats DROP CONSTRAINT IF EXISTS check_run_stats_source_type;",
    """
    ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type CHECK (
        source_type IN (
            'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens',
            'judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy',
            'violations', 'foreclosures', 'permits', 'tax_delinquencies',
            'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',
            'sunbiz', 'property_appraiser', 'dbpr_company',
            'tax_deed_auction', 'vacant_land',
            'tax_deed_outcomes', 'appraiser_sale_outcomes', 'foreclosure_outcomes'
        )
    );
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("cde02_scraper_run_stats_source_type complete.")


if __name__ == "__main__":
    main()
