"""Create ``lending.dialer_unconfirmed_creates``: dialer creates that failed ambiguously.

A create that times out or returns 5xx may still have made the contact, leaving one with no
``dialer_load_records`` row. While a phone has an open row here, an opt-out for it stays
pending instead of being reported complete. After checking the dialer for that number (delete
the contact if present), close the row:

    UPDATE lending.dialer_unconfirmed_creates SET resolved_at = now(), resolution_note = '<finding>'
    WHERE phone = '<E.164>' AND resolved_at IS NULL;

Idempotent (``checkfirst``); run after apply_lending_compliance.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_dialer_unconfirmed_creates.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingDialerUnconfirmedCreate

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
        LendingDialerUnconfirmedCreate.__table__.create(translated, checkfirst=True)
    logger.info("apply_lending_dialer_unconfirmed_creates complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
