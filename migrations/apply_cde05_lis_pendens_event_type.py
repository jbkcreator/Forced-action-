"""Apply CDE-05 — add outcome_candidates.raw_payload and widen event_type CHECK
to include deed_flip, probate_sale, lien_sale, lp_sold_pre_auction.

Superset list: includes CDE-03/08's types too, since this branch was cut
before those merged — whichever of this script or
migrations/apply_cde03_08_event_types.py runs second is a no-op (both DROP
IF EXISTS before re-adding the same widened list).

Idempotent — DROP CONSTRAINT IF EXISTS before re-adding.

Usage:
    PYTHONPATH=. python migrations/apply_cde05_lis_pendens_event_type.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE outcome_candidates ADD COLUMN IF NOT EXISTS raw_payload JSONB;",
    "ALTER TABLE outcome_candidates DROP CONSTRAINT IF EXISTS outcome_candidates_event_type_check;",
    "ALTER TABLE outcome_candidates DROP CONSTRAINT IF EXISTS check_outcome_candidate_event_type;",
    "ALTER TABLE outcome_candidates ADD CONSTRAINT check_outcome_candidate_event_type "
    "CHECK (event_type IN ('auction_sold_third_party','auction_reverted_to_lender',"
    "'auction_cancelled','tax_deed_sold','tax_deed_cancelled','tax_deed_redeemed',"
    "'qualified_sale','unqualified_sale','deed_flip','probate_sale','lien_sale',"
    "'lp_sold_pre_auction'));",
    "ALTER TABLE scraper_run_stats DROP CONSTRAINT IF EXISTS check_run_stats_source_type;",
    "ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type "
    "CHECK (source_type IN ('lien_tcl','lien_ccl','lien_hoa','lien_ml','lien_tl','lien_unknown',"
    "'lis_pendens','judgments','deeds','evictions','divorce_filings','probate','bankruptcy',"
    "'violations','foreclosures','permits','tax_delinquencies','roofing_permits','storm_damage',"
    "'flood_damage','insurance_claims','fire_incidents','sunbiz','property_appraiser','dbpr_company',"
    "'tax_deed_auction','vacant_land','tax_deed_outcomes','appraiser_sale_outcomes',"
    "'foreclosure_outcomes','outcome_label_layer','dor_sales','dor_sale_outcomes',"
    "'deed_flip_outcomes','probate_lien_outcomes','lis_pendens_outcomes'));",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url)
    with engine.begin() as conn:
        for stmt in DDL:
            logger.info("Executing: %s", stmt)
            conn.execute(text(stmt))
    logger.info("CDE-05 event_type CHECK constraint applied.")


if __name__ == "__main__":
    main()
