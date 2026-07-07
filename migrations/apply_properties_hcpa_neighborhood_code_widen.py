"""Widen properties.hcpa_neighborhood_code VARCHAR(50) -> VARCHAR(255).

Found while doing end-to-end real-scraper testing for CDE-06: real HCPA
neighborhood values are "CODE | Description" strings (e.g.
"207004.00 | NW Hillsborough & Dale Mabry, S of Sligh", 53 chars) that
routinely exceed VARCHAR(50). Because the appraiser loader writes property
updates via a batched `executemany`, a single oversized value fails the
ENTIRE batch — confirmed live: a 3-property refresh batch containing one
long neighborhood-code value rolled back all 3 properties' updates, not just
the offending one. This has likely been silently dropping appraiser refresh
batches in production. Widened to 255 to match `subdivision`, which uses the
same "CODE | Description" format and column width.

Idempotent — ALTER COLUMN TYPE is a no-op if already VARCHAR(255) or wider.

Usage:
    PYTHONPATH=. python migrations/apply_properties_hcpa_neighborhood_code_widen.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE properties ALTER COLUMN hcpa_neighborhood_code TYPE VARCHAR(255);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("properties_hcpa_neighborhood_code_widen complete.")


if __name__ == "__main__":
    main()
