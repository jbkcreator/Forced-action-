"""Apply fa104 - add term_months to competitor_rate_sheets (Task 4.8).

Idempotent — ADD COLUMN IF NOT EXISTS.

Usage:
    PYTHONPATH=. python scripts/apply_fa104_competitor_term_months.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE competitor_rate_sheets ADD COLUMN IF NOT EXISTS term_months INTEGER;",
]


def main() -> None:
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))
    logger.info("fa104 complete — term_months added.")


if __name__ == "__main__":
    main()
