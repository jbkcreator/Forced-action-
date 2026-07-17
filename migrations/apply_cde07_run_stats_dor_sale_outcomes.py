"""Apply CDE-07 — add 'dor_sale_outcomes' to scraper_run_stats.source_type.

The dor_sales table (CDE-07 raw ingestion) and its 160k+ matched rows
already exist in the shared DB — loaded out-of-band ahead of this migration.
The check_run_stats_source_type constraint already includes 'dor_sales' (the
ingestion source_type) but not 'dor_sale_outcomes' (the outcome connector's
own source_type, src/connectors/dor_sale_outcomes.py) — run_connector()'s
record_scraper_stats() call would fail its CHECK without this.

Reads the constraint's CURRENT allowed values from Postgres and takes the
UNION with its own required set before re-applying (same pattern as
apply_cde10_run_stats_source_type.py and Dev 2's
apply_cde05_lis_pendens_event_type.py) rather than re-asserting a hardcoded
snapshot — a hardcoded list silently drops whatever a sibling migration
added if this one runs after it. Order-independent regardless of which of
these three scripts runs last.

Idempotent — DROP CONSTRAINT IF EXISTS + re-ADD is safe to rerun.

Usage:
    PYTHONPATH=. python migrations/apply_cde07_run_stats_dor_sale_outcomes.py
"""
from __future__ import annotations

import logging
import re

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REQUIRED_SOURCE_TYPES = {
    "lien_tcl", "lien_ccl", "lien_hoa", "lien_ml", "lien_tl", "lien_unknown", "lis_pendens",
    "judgments", "deeds", "evictions", "divorce_filings", "probate", "bankruptcy",
    "violations", "foreclosures", "permits", "tax_delinquencies", "roofing_permits",
    "storm_damage", "flood_damage", "insurance_claims", "fire_incidents", "sunbiz",
    "property_appraiser", "dbpr_company", "tax_deed_auction", "vacant_land",
    "tax_deed_outcomes", "appraiser_sale_outcomes", "foreclosure_outcomes",
    "outcome_label_layer", "dor_sales", "deed_flip_outcomes", "probate_lien_outcomes",
    "dor_sale_outcomes",
}


def _existing_check_values(conn: Connection, constraint_name: str, table: str) -> set[str]:
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


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        current = _existing_check_values(conn, "check_run_stats_source_type", "scraper_run_stats")
        union = sorted(current | REQUIRED_SOURCE_TYPES)
        values_sql = ",".join(f"'{v}'" for v in union)
        conn.execute(text("ALTER TABLE scraper_run_stats DROP CONSTRAINT IF EXISTS check_run_stats_source_type;"))
        conn.execute(
            text(f"ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type "
                 f"CHECK (source_type IN ({values_sql}));")
        )
        added = REQUIRED_SOURCE_TYPES - current
        logger.info("union applied (%d existing, %d added: %s)", len(current), len(added), sorted(added))

    logger.info("cde07_run_stats_dor_sale_outcomes complete.")


if __name__ == "__main__":
    main()
