"""R1 1.5: ``lending.inbox_events`` (GHL texts/emails and missed inbound calls waiting for Cora's triage) and the
``question`` intent on ``lending.instantly_replies``.

Idempotent; run after apply_lending_instantly_replies.py. Run it BEFORE deploying the code: the GHL reply webhook and
the missed-call webhook write the new table once LENDING_UNIFIED_INBOX_ENABLED is on, and the sweep writes the new
intent value.

Usage:
    PYTHONPATH=. python migrations/apply_lending_unified_inbox.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingInboxEvent

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_INTENT_CHECK = ("intent IS NULL OR intent IN ('deal', 'question', 'objection', 'dead', 'unsubscribe', "
                 "'unclassified')")


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingInboxEvent.__table__.create(translated, checkfirst=True)
    conn.execute(text(f'ALTER TABLE "{schema}".instantly_replies DROP CONSTRAINT IF EXISTS ck_instantly_replies_intent'))
    conn.execute(text(f'ALTER TABLE "{schema}".instantly_replies ADD CONSTRAINT ck_instantly_replies_intent '
                      f"CHECK ({_INTENT_CHECK})"))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_unified_inbox complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
