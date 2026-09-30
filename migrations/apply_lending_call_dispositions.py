"""Go Live call log: ``lending.call_dispositions`` with queue / source_tag / seat_group.

The table already exists in the shared DB (created for the call webhook), so this
creates it only when absent and adds the three Go Live columns. Idempotent; run
after apply_lending_compliance.py, which creates the ``lending`` schema.

Usage:
    PYTHONPATH=. python migrations/apply_lending_call_dispositions.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingCallDisposition

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GO_LIVE_COLUMNS = (("queue", "varchar(30)"), ("source_tag", "varchar(40)"), ("seat_group", "varchar(10)"))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
        LendingCallDisposition.__table__.create(translated, checkfirst=True)
        for name, ddl in GO_LIVE_COLUMNS:
            conn.execute(text(f'ALTER TABLE "{schema}".call_dispositions ADD COLUMN IF NOT EXISTS {name} {ddl}'))
    logger.info("apply_lending_call_dispositions complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
