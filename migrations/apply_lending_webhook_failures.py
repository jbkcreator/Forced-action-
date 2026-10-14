"""``lending.webhook_failures``: GHL opt-out and consent webhook calls that failed after they arrived, kept so
``src.lending.webhook_failures`` can replay them and an opt-out is never lost to a database outage.

Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema).

Usage:
    PYTHONPATH=. python migrations/apply_lending_webhook_failures.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingWebhookFailure

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingWebhookFailure.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_webhook_failures complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
