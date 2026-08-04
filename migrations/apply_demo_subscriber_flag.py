"""Add is_demo flag to subscribers table.

Demo subscribers bypass the locked-ZIP territory filter on the feed endpoint —
they see all scored, qualified leads across their county. This lets a sales
demo account answer "show me leads in ZIP 33578" for any ZIP a prospect names
without needing every ZIP locked in zip_territories.

Real subscriber checkouts are unaffected — claim_zip_territory only touches
zip_territories rows, which demo accounts never create.

Idempotent — IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_demo_subscriber_flag.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS "
    "is_demo BOOLEAN NOT NULL DEFAULT false;",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
    logger.info("apply_demo_subscriber_flag: done.")


if __name__ == "__main__":
    main()
