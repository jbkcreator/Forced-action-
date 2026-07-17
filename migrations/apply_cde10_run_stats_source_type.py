"""Apply CDE-10 go-live — add outcome_label_layer to
ScraperRunStats.check_run_stats_source_type.

Reads the constraint's CURRENT allowed values from Postgres and takes the
UNION with its own required set before re-applying, instead of hardcoding a
snapshot list and overwriting it. A hardcoded snapshot silently drops
whatever any other migration (CDE-07's dor_sale_outcomes/dor_sales, or Dev
2's deed_flip_outcomes/probate_lien_outcomes) added if this script runs
after them. Running this before, after, or interleaved with any sibling
migration always converges on the full set — none of them can remove a
value another added.

Usage:
    PYTHONPATH=. python migrations/apply_cde10_run_stats_source_type.py
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
    "outcome_label_layer",
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

    logger.info("cde10_run_stats_source_type complete.")


if __name__ == "__main__":
    main()
