"""``lending.after_hours_conversations``: one row per after-hours inquiry the night responder is answering (the
three qualifying questions, then a call slot). Rows are added by ``src.lending.after_hours_conversation``.

Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema). Run it BEFORE setting
LENDING_AFTER_HOURS_CONVERSATION_ENABLED=true: the voicemail intake and the reply webhook write this table.

Usage:
    PYTHONPATH=. python migrations/apply_lending_after_hours_conversations.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingAfterHoursConversation

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingAfterHoursConversation.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_after_hours_conversations complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
