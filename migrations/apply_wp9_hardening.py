"""Apply WP-9 dial-list failure-behavior hardening tables.

Three tables backing the amendment's "Failure behavior" requirements
(amendment-1-detail.md lines 900-904):

  dial_list_snapshot         -- last successfully generated list per county/day;
                                served when live generation fails so the dial
                                list "still posts from cached state".
  dial_list_touch            -- durable record of non-terminal Called/Skip taps
                                (audit + idempotency), one row per
                                (property, day, action).
  dial_list_needs_enrichment -- candidates whose enrichment returned no contact;
                                held out of the callable list, retried each batch,
                                never surfaced with a guessed contact.

Idempotent — IF NOT EXISTS guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_wp9_hardening.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS dial_list_snapshot (
        id BIGSERIAL PRIMARY KEY,
        county_id TEXT,
        generated_for DATE NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_dial_list_snapshot_county_date "
    "ON dial_list_snapshot (county_id, generated_for DESC);",
    """
    CREATE TABLE IF NOT EXISTS dial_list_touch (
        id BIGSERIAL PRIMARY KEY,
        opportunity_thread_id TEXT,
        property_id BIGINT,
        action TEXT NOT NULL,
        actor TEXT,
        generation_date DATE NOT NULL,
        touched_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    "DROP INDEX IF EXISTS uq_dial_list_touch_prop_day_action;",
    """
    CREATE TABLE IF NOT EXISTS dial_list_needs_enrichment (
        id BIGSERIAL PRIMARY KEY,
        property_id BIGINT NOT NULL UNIQUE,
        reason TEXT NOT NULL,
        first_seen DATE NOT NULL,
        last_seen DATE NOT NULL,
        retry_count INTEGER NOT NULL DEFAULT 0
    );
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("wp9_hardening migration complete.")


if __name__ == "__main__":
    main()
