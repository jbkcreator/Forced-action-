"""Apply the activation_events table (T-B12-05: 5-min activation onboarding).

One row per subscriber tracking the 5-minute activation funnel:
  signup_time            -- stamped at row creation (mirrors Subscriber.created_at)
  first_leads_shown_time -- first time the free-tier dashboard rendered the
                            3-5 real scored leads (contact locked)
  first_unlock_time      -- first time the subscriber unlocked any lead's
                            contact info (the activation event)

Idempotent — IF NOT EXISTS guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_activation_events.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS activation_events (
        subscriber_id INTEGER PRIMARY KEY REFERENCES subscribers(id),
        signup_time TIMESTAMPTZ NOT NULL DEFAULT now(),
        first_leads_shown_time TIMESTAMPTZ,
        first_unlock_time TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_activation_events_first_unlock "
    "ON activation_events (first_unlock_time);",
    # Backfill signup_time from Subscriber.created_at for any subscriber that
    # doesn't have a row yet, so historical subscribers get a baseline too.
    """
    INSERT INTO activation_events (subscriber_id, signup_time)
    SELECT s.id, s.created_at
    FROM subscribers s
    LEFT JOIN activation_events ae ON ae.subscriber_id = s.id
    WHERE ae.subscriber_id IS NULL;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("activation_events migration complete.")


if __name__ == "__main__":
    main()
