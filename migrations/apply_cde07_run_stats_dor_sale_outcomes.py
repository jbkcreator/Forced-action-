"""Apply CDE-07 — add 'dor_sale_outcomes' to scraper_run_stats.source_type.

The dor_sales table (CDE-07 raw ingestion) and its 160k+ matched rows
already exist in the shared DB — loaded out-of-band ahead of this migration.
The check_run_stats_source_type constraint already includes 'dor_sales' (the
ingestion source_type) but not 'dor_sale_outcomes' (the outcome connector's
own source_type, src/connectors/dor_sale_outcomes.py) — run_connector()'s
record_scraper_stats() call would fail its CHECK without this.

Re-asserts the full current value list (verified against the live
constraint) plus 'dor_sale_outcomes', so unrelated in-flight source_types
(e.g. Dev 2's deed_flip_outcomes/probate_lien_outcomes, already live) are
preserved rather than dropped.

Idempotent — DROP CONSTRAINT IF EXISTS + re-ADD is safe to rerun.

Usage:
    PYTHONPATH=. python migrations/apply_cde07_run_stats_dor_sale_outcomes.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    ALTER TABLE scraper_run_stats DROP CONSTRAINT IF EXISTS check_run_stats_source_type;
    """,
    """
    ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type
        CHECK (source_type IN (
            'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens',
            'judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy',
            'violations', 'foreclosures', 'permits', 'tax_delinquencies',
            'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',
            'sunbiz', 'property_appraiser', 'dbpr_company',
            'tax_deed_auction', 'vacant_land',
            'tax_deed_outcomes', 'appraiser_sale_outcomes', 'foreclosure_outcomes',
            'outcome_label_layer', 'dor_sales', 'deed_flip_outcomes', 'probate_lien_outcomes',
            'dor_sale_outcomes'
        ));
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info("cde07_run_stats_dor_sale_outcomes complete — 'dor_sale_outcomes' added to check_run_stats_source_type.")


if __name__ == "__main__":
    main()
