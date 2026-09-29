"""Create lending.call_dispositions (one row per Aircall call).

Creates only this table (src/lending/models.py:LendingCallDisposition); the
compliance tables are applied by migrations/apply_lending_compliance.py.
Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_lending_call_dispositions.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingBase, LendingCallDisposition

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
        LendingBase.metadata.create_all(translated, tables=[LendingCallDisposition.__table__], checkfirst=True)
    logger.info("apply_lending_call_dispositions complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
