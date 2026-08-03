"""Add the Section 4.10 activation-event checkpoints.

Adds:
  - activation_events.welcome_email_sent_time
  - activation_events.magic_link_redeemed_time

Idempotent: uses IF NOT EXISTS guards.

Usage:
    PYTHONPATH=. python migrations/apply_activation_event_checkpoints.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    ALTER TABLE activation_events
    ADD COLUMN IF NOT EXISTS welcome_email_sent_time TIMESTAMPTZ
    """,
    """
    ALTER TABLE activation_events
    ADD COLUMN IF NOT EXISTS magic_link_redeemed_time TIMESTAMPTZ
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("activation_events checkpoint migration complete.")


if __name__ == "__main__":
    main()
