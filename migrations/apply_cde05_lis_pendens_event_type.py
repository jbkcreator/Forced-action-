"""Apply CDE-05 — add outcome_candidates.raw_payload and widen the
event_type / source_type CHECK constraints to include lp_sold_pre_auction
(and the other CDE-03/08 types, in case that migration hasn't run yet).

Genuinely order-independent with migrations/apply_cde03_08_event_types.py:
rather than hardcoding a snapshot list and overwriting it (which silently
DROPS whatever a differently-ordered run added), this script reads the
constraint's CURRENT allowed values from Postgres and takes the UNION with
its own required set before re-applying. Running this before, after, or
repeatedly interleaved with the CDE-03/08 script always converges on the
full set — neither script can ever remove a value the other added.

Usage:
    PYTHONPATH=. python migrations/apply_cde05_lis_pendens_event_type.py
"""
from __future__ import annotations

import logging
import re

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REQUIRED_EVENT_TYPES = {
    "auction_sold_third_party", "auction_reverted_to_lender", "auction_cancelled",
    "tax_deed_sold", "tax_deed_cancelled", "tax_deed_redeemed",
    "qualified_sale", "unqualified_sale",
    "deed_flip", "probate_sale", "lien_sale", "lp_sold_pre_auction",
}

REQUIRED_SOURCE_TYPES = {
    "lien_tcl", "lien_ccl", "lien_hoa", "lien_ml", "lien_tl", "lien_unknown", "lis_pendens",
    "judgments", "deeds", "evictions", "divorce_filings", "probate", "bankruptcy",
    "violations", "foreclosures", "permits", "tax_delinquencies", "roofing_permits",
    "storm_damage", "flood_damage", "insurance_claims", "fire_incidents", "sunbiz",
    "property_appraiser", "dbpr_company", "tax_deed_auction", "vacant_land",
    "tax_deed_outcomes", "appraiser_sale_outcomes", "foreclosure_outcomes",
    "outcome_label_layer", "dor_sales", "dor_sale_outcomes",
    "deed_flip_outcomes", "probate_lien_outcomes", "lis_pendens_outcomes",
}


def _existing_check_values(conn: Connection, constraint_name: str, table: str) -> set[str]:
    """Read a table's current CHECK-constraint allowed values, or empty set if it doesn't exist yet."""
    row = conn.execute(
        text(
            "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint "
            "WHERE conname = :name AND conrelid = CAST(:table AS regclass)"
        ),
        {"name": constraint_name, "table": table},
    ).first()
    if row is None:
        return set()
    return set(re.findall(r"'([^']+)'", row._mapping["def"]))


def _widen_check(conn: Connection, table: str, column: str, constraint_name: str, required: set[str]) -> None:
    current = _existing_check_values(conn, constraint_name, table)
    union = sorted(current | required)
    values_sql = ",".join(f"'{v}'" for v in union)
    conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint_name};"))
    conn.execute(
        text(f"ALTER TABLE {table} ADD CONSTRAINT {constraint_name} CHECK ({column} IN ({values_sql}));")
    )
    added = required - current
    logger.info("%s.%s: union applied (%d existing, %d added: %s)", table, column, len(current), len(added), sorted(added))


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE outcome_candidates ADD COLUMN IF NOT EXISTS raw_payload JSONB;"))
        _widen_check(conn, "outcome_candidates", "event_type", "check_outcome_candidate_event_type", REQUIRED_EVENT_TYPES)
        _widen_check(conn, "scraper_run_stats", "source_type", "check_run_stats_source_type", REQUIRED_SOURCE_TYPES)
    logger.info("CDE-05 constraints applied (order-independent union).")


if __name__ == "__main__":
    main()
