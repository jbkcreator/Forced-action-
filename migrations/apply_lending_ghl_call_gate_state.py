"""``lending.ghl_call_gate_state``: one row (id 1) with the GHL call-list gate's last success and failure, so a gate
that stops running is alerted once. Written by ``src.lending.ghl_call_gate``.

Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema), before enabling
LENDING_GHL_CALL_GATE_ENABLED.

Usage:
    PYTHONPATH=. python migrations/apply_lending_ghl_call_gate_state.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingGhlCallGateState

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingGhlCallGateState.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_ghl_call_gate_state complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
