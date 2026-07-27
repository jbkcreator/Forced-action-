"""Apply the inbound_response table (Block 11 — Inbound Velocity, B11-04).

One row per hot inbound call. t0 = webhook_log.created_at at score time; t1
is backfilled from agent_decisions.completed_at (decision_id join) once the
shared new_lead_voice_call graph run finishes. Report-only — no closed loop.

Idempotent — IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_inbound_response.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS inbound_response (
        id                BIGSERIAL PRIMARY KEY,
        subscriber_id     INTEGER REFERENCES subscribers(id),
        decision_id       VARCHAR(36),
        t0                TIMESTAMPTZ NOT NULL,
        t1                TIMESTAMPTZ,
        score             INTEGER NOT NULL,
        matched_signals   JSONB,
        outcome           VARCHAR(20) NOT NULL DEFAULT 'pending',
        created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT check_inbound_response_outcome CHECK (
            outcome IN ('pending', 'called', 'consent_blocked', 'dnc_blocked', 'failed')
        )
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_inbound_response_subscriber_id ON inbound_response (subscriber_id);",
    "CREATE INDEX IF NOT EXISTS idx_inbound_response_decision_id ON inbound_response (decision_id);",
    "CREATE INDEX IF NOT EXISTS idx_inbound_response_created_at ON inbound_response (created_at);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("inbound_response migration complete.")


if __name__ == "__main__":
    main()
