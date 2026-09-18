"""Stage B — WP-T2-8: extend buyer_entity_links.source_table to include permit tables.

Drops and recreates the CHECK constraint to allow 'building_permits' and
'permit_staging' as valid source_table values in buyer_entity_links. The DROP
is safe: Postgres enforces the constraint on write, not on existing data, and
we are only expanding the allowed set, never narrowing it.

Idempotent — uses a transaction-per-step with a guard query so re-running is
safe on the shared DB.

Usage:
    PYTHONPATH=. python migrations/apply_builder_permit_entity_links.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# The full expanded set — includes all values the previous migration defined plus the new ones.
_NEW_CONSTRAINT_VALUES = (
    "'owners', 'deeds', 'sunbiz_snapshots', 'tax_deed_auctions', "
    "'building_permits', 'permit_staging'"
)

DDL = [
    # Drop old constraint (name from apply_buyer_entities_profiling_columns.py).
    # IF EXISTS = safe re-run after the constraint was already replaced.
    "ALTER TABLE buyer_entity_links DROP CONSTRAINT IF EXISTS check_buyer_entity_link_source_table;",

    # Recreate with expanded set.
    f"""
    ALTER TABLE buyer_entity_links
        ADD CONSTRAINT check_buyer_entity_link_source_table
        CHECK (source_table IN ({_NEW_CONSTRAINT_VALUES}));
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("apply_builder_permit_entity_links complete.")


if __name__ == "__main__":
    main()
