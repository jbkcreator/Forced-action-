"""Backfilled from alembic migration `z0a1b2c3d4e5_add_chat_sessions_and_messages`
(revision fa005_concierge_chat).

Concierge Chat tables: chat_sessions + chat_messages. Idempotent. Live DB already
has this; kept so every schema change lives in scripts/ (ADR 0024).

Usage:
    PYTHONPATH=. python scripts/apply_z0a1b2c3d4e5_add_chat_sessions_and_messages.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE IF NOT EXISTS chat_sessions (
    id             VARCHAR(36) PRIMARY KEY,
    subscriber_id  INTEGER REFERENCES subscribers(id),
    anonymous_id   VARCHAR(36),
    source         VARCHAR(20) NOT NULL DEFAULT 'landing',
    created_at     TIMESTAMP NOT NULL,
    last_seen_at   TIMESTAMP NOT NULL,
    linked_at      TIMESTAMP,
    CONSTRAINT check_chat_session_source CHECK (source IN ('landing', 'dashboard', 'lead_feed'))
);
CREATE INDEX IF NOT EXISTS idx_chat_session_subscriber ON chat_sessions (subscriber_id, created_at);
CREATE INDEX IF NOT EXISTS idx_chat_session_anon        ON chat_sessions (anonymous_id, created_at);

CREATE TABLE IF NOT EXISTS chat_messages (
    id                    SERIAL PRIMARY KEY,
    session_id            VARCHAR(36) NOT NULL REFERENCES chat_sessions(id),
    role                  VARCHAR(10) NOT NULL,
    content               TEXT,
    intent_label          VARCHAR(40),
    intent_confidence     NUMERIC(4,3),
    tool_calls_json       JSONB,
    payment_trigger_json  JSONB,
    claude_model          VARCHAR(20),
    tokens_in             INTEGER,
    tokens_out            INTEGER,
    latency_ms            INTEGER,
    error                 VARCHAR(200),
    created_at            TIMESTAMP NOT NULL,
    CONSTRAINT check_chat_message_role CHECK (role IN ('user', 'assistant', 'system', 'tool'))
);
CREATE INDEX IF NOT EXISTS idx_chat_message_session_created ON chat_messages (session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_chat_message_intent          ON chat_messages (intent_label, created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied z0a1b2c3d4e5_add_chat_sessions_and_messages")


if __name__ == "__main__":
    main()
