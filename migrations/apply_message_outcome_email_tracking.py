"""Add transactional-email tracking fields to message_outcomes.

Fields:
  - recipient_email
  - provider_message_id
  - failure_reason

Idempotent: IF NOT EXISTS guards.
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    ALTER TABLE message_outcomes
    ADD COLUMN IF NOT EXISTS recipient_email TEXT
    """,
    """
    ALTER TABLE message_outcomes
    ADD COLUMN IF NOT EXISTS provider_message_id TEXT
    """,
    """
    ALTER TABLE message_outcomes
    ADD COLUMN IF NOT EXISTS failure_reason TEXT
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_outcomes_recipient_email
    ON message_outcomes (recipient_email)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_outcomes_provider_message_id
    ON message_outcomes (provider_message_id)
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("message_outcomes email tracking migration complete.")


if __name__ == "__main__":
    main()
