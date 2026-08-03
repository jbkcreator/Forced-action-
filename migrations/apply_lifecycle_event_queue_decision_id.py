"""Add decision_id to lifecycle_event_queue (Redis-down fallback correctness).

The Postgres fallback path (publish_lifecycle_event -> lifecycle_event_queue ->
_sweep_postgres_queue) previously dropped decision_id entirely: only
idempotency_key was stored and forwarded. Every event that pins a specific
decision_id (abandonment Wave 2 via requires_decision_id, Block 11 inbound
callback tracking) got a fresh random decision_id when dispatched from the
fallback, breaking downstream joins on agent_decisions.decision_id.

Nullable + additive — existing rows/callers are unaffected (decision_id NULL,
same as before). Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_lifecycle_event_queue_decision_id.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE lifecycle_event_queue ADD COLUMN IF NOT EXISTS decision_id VARCHAR(36);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("lifecycle_event_queue.decision_id migration complete.")


if __name__ == "__main__":
    main()
