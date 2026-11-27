"""R1 0.9 Shadow Mode: ``lending.shadow_classes`` (class mode + 14-day clock) and
``lending.shadow_decisions`` (one prediction per class per card).

Idempotent; run before deploying the shadow runner.  No other migration required first
(it creates its own tables in the ``lending`` schema).

Usage:
    PYTHONPATH=. python migrations/apply_lending_shadow.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingShadowClass, LendingShadowDecision

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingShadowClass.__table__.create(translated, checkfirst=True)
    LendingShadowDecision.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_shadow complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
