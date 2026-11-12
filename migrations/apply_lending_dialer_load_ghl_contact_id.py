"""``ghl_contact_id`` on ``lending.dialer_load_records``: the GHL contact the load sync upserted (src/lending/ghl_load_sync.py).

Agent jobs (Cora P1.1) name a contact by its GHL id only, and the nightly ledger export (P1.2) keys rows by it, so a
dialer contact needs its GHL id stored. New pushes record it; rows pushed before this column existed stay NULL until
a backfill (separate, run on request: it reads GHL).

Idempotent: ``ADD COLUMN IF NOT EXISTS`` / ``CREATE INDEX IF NOT EXISTS``. Run BEFORE deploying the code: the load sync
and the ledger export read and write the column.

Usage:
    PYTHONPATH=. python migrations/apply_lending_dialer_load_ghl_contact_id.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    table = f'"{schema}".dialer_load_records'
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS ghl_contact_id VARCHAR(64)"))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_lending_dialer_load_records_ghl_contact "
            f"ON {table} (phone) WHERE ghl_contact_id IS NOT NULL"
        ))
    logger.info("apply_lending_dialer_load_ghl_contact_id complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
