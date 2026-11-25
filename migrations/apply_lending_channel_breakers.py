"""R1 0.4 safe send: per-channel circuit breaker, per-minute rate counters, and the ``held`` card status.

Creates ``lending.channel_breakers`` (seeded closed for ``ghl_sms``, ``ghl_email``, ``instantly``),
``lending.channel_breaker_events`` and ``lending.channel_rate_counters``, and runs the agent_core safeguards so
``lending.pending_actions`` gets the ``held`` status and the ``held_reason`` column. Idempotent; run after
apply_lending_pending_action_safeguards.py and BEFORE deploying the code: every send asks the breaker table first and
an unreadable breaker refuses the send.

Usage:
    PYTHONPATH=. python migrations/apply_lending_channel_breakers.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from packages.agent_core.schema import apply_safeguards
from src.lending.dispatch.circuit_breaker import CHANNELS
from src.lending.models import (LENDING_SCHEMA, LendingChannelBreaker, LendingChannelBreakerEvent,
                                LendingChannelRateCounter)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    apply_safeguards(conn, schema)
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    for model in (LendingChannelBreaker, LendingChannelBreakerEvent, LendingChannelRateCounter):
        model.__table__.create(translated, checkfirst=True)
    for channel in CHANNELS:
        conn.execute(text(f'INSERT INTO "{schema}".channel_breakers (channel) VALUES (:c) ON CONFLICT (channel) DO NOTHING'),
                     {"c": channel})


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_channel_breakers complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
