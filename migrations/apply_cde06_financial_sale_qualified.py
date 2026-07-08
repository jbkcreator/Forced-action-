"""Apply CDE-06 — Financial.last_sale_qualified + last_sale_vacant_improved.

pa_parser.py already extracts both fields from the HCPA sales-history table
(the "Qualified/Unqualified" and "Vacant/Improved" columns) into the scraper's
canonical DataFrame — they were just discarded before reaching the DB.
src/loaders/property_appraiser.py now persists them. This migration adds the
columns; existing rows populate gradually as the normal weekly appraiser
refresh cadence re-touches each property (no immediate mass backfill —
per-row upsert, not a blanket rewrite).

Idempotent — IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_cde06_financial_sale_qualified.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE financials ADD COLUMN IF NOT EXISTS last_sale_qualified BOOLEAN;",
    "ALTER TABLE financials ADD COLUMN IF NOT EXISTS last_sale_vacant_improved VARCHAR(20);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("cde06_financial_sale_qualified complete.")


if __name__ == "__main__":
    main()
