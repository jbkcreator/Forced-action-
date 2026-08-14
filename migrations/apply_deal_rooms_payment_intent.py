"""Add stripe_payment_intent_id to deal_rooms.

Idempotent — ADD COLUMN IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_deal_rooms_payment_intent.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    ALTER TABLE deal_rooms
        ADD COLUMN IF NOT EXISTS stripe_payment_intent_id VARCHAR(100);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_deal_rooms_stripe_pi
        ON deal_rooms (stripe_payment_intent_id)
        WHERE stripe_payment_intent_id IS NOT NULL;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("apply_deal_rooms_payment_intent complete.")


if __name__ == "__main__":
    main()
