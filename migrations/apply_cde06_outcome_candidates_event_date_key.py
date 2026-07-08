"""Apply CDE-06 — widen outcome_candidates uniqueness to include event_date.

Financial is 1:1 with Property, overwritten on every appraiser refresh — it
can only ever represent "the property's most recent sale." Keying
outcome_candidates on (source_type, source_table, source_id) alone means a
second qualified sale on the same property (same Financial.id forever) would
silently overwrite the first sale's staged outcome instead of creating a
second one, destroying the flip/resale signal this pipeline exists to
capture. Adding event_date to the key fixes this: it's stable per tax-deed
auction (so tax_deed_outcomes keeps correctly updating the same row as a case
resolves) but changes on every new sale for financials (so
appraiser_sale_outcomes correctly creates a new row per sale event).

Table is empty as of this migration — zero data risk.

Idempotent — DROP CONSTRAINT IF EXISTS before re-adding.

Usage:
    PYTHONPATH=. python migrations/apply_cde06_outcome_candidates_event_date_key.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE outcome_candidates DROP CONSTRAINT IF EXISTS uq_outcome_candidate;",
    "ALTER TABLE outcome_candidates ADD CONSTRAINT uq_outcome_candidate "
    "UNIQUE (source_type, source_table, source_id, event_date);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("cde06_outcome_candidates_event_date_key complete.")


if __name__ == "__main__":
    main()
