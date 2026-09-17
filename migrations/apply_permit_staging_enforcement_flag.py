"""WP-T2-8 review fix #3 — add is_enforcement_permit to permit_staging.

Enforcement classification was computed only in the matched-property branch, so
unmatched permits reached permit_staging without the flag and the builder
detectors could not exclude enforcement records from the staging side (~74–90%
of permits are staged). This adds the column so the flag is carried through.

Idempotent — ADD COLUMN IF NOT EXISTS.

Usage:
    PYTHONPATH=. python migrations/apply_permit_staging_enforcement_flag.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE permit_staging ADD COLUMN IF NOT EXISTS is_enforcement_permit BOOLEAN NOT NULL DEFAULT FALSE;",
    "CREATE INDEX IF NOT EXISTS idx_permit_staging_enforcement ON permit_staging (is_enforcement_permit);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))
    logger.info("apply_permit_staging_enforcement_flag complete.")


if __name__ == "__main__":
    main()
