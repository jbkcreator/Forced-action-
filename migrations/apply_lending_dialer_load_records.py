"""Create ``lending.dialer_load_records``: the records loaded into the dialer.

One row per pool record loaded, with the details the caller sees and the
Aircall contact it was loaded as. A partial unique index keeps at most one
active row per phone. Idempotent (``checkfirst``); run after
apply_lending_compliance.py, which creates the ``lending`` schema.

Usage:
    PYTHONPATH=. python migrations/apply_lending_dialer_load_records.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingDialerLoadRecord

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
        LendingDialerLoadRecord.__table__.create(translated, checkfirst=True)
    logger.info("apply_lending_dialer_load_records complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
