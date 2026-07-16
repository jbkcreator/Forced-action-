"""Apply CDE-03/CDE-08 — widen outcome_candidates.event_type CHECK to include
deed_flip, probate_sale, lien_sale.

Idempotent — DROP CONSTRAINT IF EXISTS before re-adding the widened list.

Usage:
    PYTHONPATH=. python migrations/apply_cde03_08_event_types.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    # raw_payload holds connector-specific extras the flat columns can't carry
    # (deed_flip stores margin/hold_days/instruments/prices here).
    "ALTER TABLE outcome_candidates ADD COLUMN IF NOT EXISTS raw_payload JSONB;",
    # Drop the stale auto-named duplicate left over from a pre-CDE-09 create_all
    # run (before the constraint was given an explicit name) -- it still carries
    # the narrower 8-value list and blocks inserts even though the named
    # constraint below is already widened.
    "ALTER TABLE outcome_candidates DROP CONSTRAINT IF EXISTS outcome_candidates_event_type_check;",
    "ALTER TABLE outcome_candidates DROP CONSTRAINT IF EXISTS check_outcome_candidate_event_type;",
    "ALTER TABLE outcome_candidates ADD CONSTRAINT check_outcome_candidate_event_type "
    "CHECK (event_type IN ('auction_sold_third_party','auction_reverted_to_lender',"
    "'auction_cancelled','tax_deed_sold','tax_deed_cancelled','tax_deed_redeemed',"
    "'qualified_sale','unqualified_sale','deed_flip','probate_sale','lien_sale'));",
    # record_scraper_stats() needs deed_flip_outcomes/probate_lien_outcomes
    # registered too, or every run silently fails to record its stats row.
    "ALTER TABLE scraper_run_stats DROP CONSTRAINT IF EXISTS check_run_stats_source_type;",
    "ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type "
    "CHECK (source_type IN ('lien_tcl','lien_ccl','lien_hoa','lien_ml','lien_tl','lien_unknown',"
    "'lis_pendens','judgments','deeds','evictions','divorce_filings','probate','bankruptcy',"
    "'violations','foreclosures','permits','tax_delinquencies','roofing_permits','storm_damage',"
    "'flood_damage','insurance_claims','fire_incidents','sunbiz','property_appraiser','dbpr_company',"
    "'tax_deed_auction','vacant_land','tax_deed_outcomes','appraiser_sale_outcomes',"
    "'foreclosure_outcomes','outcome_label_layer','dor_sales','deed_flip_outcomes',"
    "'probate_lien_outcomes'));",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url)
    with engine.begin() as conn:
        for stmt in DDL:
            logger.info("Executing: %s", stmt)
            conn.execute(text(stmt))
    logger.info("CDE-03/08 event_type CHECK constraint applied.")


if __name__ == "__main__":
    main()
