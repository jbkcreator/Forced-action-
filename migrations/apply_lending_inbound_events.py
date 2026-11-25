"""``lending.inbound_events``: the R1 0.5 inbound journal. Each journaled webhook body (GHL stage / reply /
appointment, LendingFlow lead, Instantly event) is saved here raw before it is processed, so a crash or a
handler failure never loses it (``src.lending.inbound.journal``).

Idempotent; run after apply_lending_webhook_failures.py (``failure_id`` references ``lending.webhook_failures``).
Run it BEFORE setting LENDING_INBOUND_JOURNAL_ENABLED=true.

Usage:
    PYTHONPATH=. python migrations/apply_lending_inbound_events.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingInboundEvent, LendingWebhookFailure

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingWebhookFailure.__table__.create(translated, checkfirst=True)
    LendingInboundEvent.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_inbound_events complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
