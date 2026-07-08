"""Apply CDE-02 — Foreclosure.winning_bid + sold_to.

Confirmed via live-site recon (RealForeclose, same .ASTAT_* class family as
the already-shipped tax-deed connector): a resolved auction item shows a
winning-bid dollar amount and a "Sold To" value that is exactly one of
"Plaintiff" (reverted to lender) or "3rd Party Bidder" (genuine sale) --
verified across 5 real auction dates, no other values seen. The scraper
previously discarded both entirely.

Idempotent -- IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_cde02_foreclosure_outcome_fields.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE foreclosures ADD COLUMN IF NOT EXISTS winning_bid NUMERIC(12, 2);",
    "ALTER TABLE foreclosures ADD COLUMN IF NOT EXISTS sold_to VARCHAR(50);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("cde02_foreclosure_outcome_fields complete.")


if __name__ == "__main__":
    main()
