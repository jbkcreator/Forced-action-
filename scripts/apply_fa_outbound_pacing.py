"""Apply fa_outbound_pacing — outbound staging columns on enriched_contacts (fa5.3).

Idempotent — ADD COLUMN IF NOT EXISTS and CREATE INDEX IF NOT EXISTS guards.

Usage:
    PYTHONPATH=. python scripts/apply_fa_outbound_pacing.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE enriched_contacts ADD COLUMN IF NOT EXISTS outbound_queued_at  TIMESTAMPTZ",
    "ALTER TABLE enriched_contacts ADD COLUMN IF NOT EXISTS first_touch_sent_at TIMESTAMPTZ",
    """
    CREATE INDEX IF NOT EXISTS idx_ec_outbound_queued
        ON enriched_contacts (outbound_queued_at)
        WHERE outbound_queued_at IS NOT NULL
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(str(settings.database_url))
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
    logger.info("fa_outbound_pacing applied: outbound_queued_at + first_touch_sent_at ready")


if __name__ == "__main__":
    main()
