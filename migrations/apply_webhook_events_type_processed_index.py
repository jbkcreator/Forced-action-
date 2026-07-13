"""Add composite index on webhook_events(event_type, processed_at).

Funnel analytics (Part 3 traffic-capture) queries webhook_events filtered by
event_type IN (...) and processed_at BETWEEN a date range, then groups by
event_type. The table already has event_type indexed alone and a composite
(source, processed_at) index, but no (event_type, processed_at) composite —
the exact shape this query needs. Without it, the query falls back to a
sequential scan as webhook_events grows.

Idempotent — CREATE INDEX IF NOT EXISTS.

Usage:
    PYTHONPATH=. python migrations/apply_webhook_events_type_processed_index.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "CREATE INDEX IF NOT EXISTS idx_webhook_events_type_processed "
    "ON webhook_events (event_type, processed_at);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("webhook_events_type_processed_index complete.")


if __name__ == "__main__":
    main()
