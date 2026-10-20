"""``lending.global_halt`` (LENDING_GLOBAL_HALT, one row) and ``lending.global_halt_events`` (every set/clear).
Written by ``src.lending.global_halt`` (``@Cora emergency halt``).

Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema).

Usage:
    PYTHONPATH=. python migrations/apply_lending_global_halt.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingGlobalHalt, LendingGlobalHaltEvent

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingGlobalHalt.__table__.create(translated, checkfirst=True)
    LendingGlobalHaltEvent.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_global_halt complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
