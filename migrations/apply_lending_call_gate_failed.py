"""G2 (2026-11-03): ``lending.call_dispositions.gate_failed``.

The 13 call results were cut to 10: GATE_FAILED_NURTURE and DATA_NURTURE_ONLY merged into NURTURE. Whether the
caller's gate check failed is now this flag on the call (GHL tag / BatchDialer contact field ``gate_failed``),
read by the scoreboard's Gated and Nurture columns. NULL = not reported. Old rows keep their old codes.

Idempotent; run after apply_lending_call_dispositions.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_call_gate_failed.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Checked first: even a no-op ADD COLUMN takes an exclusive lock on call_dispositions (the CDR poller writes it
# every few seconds).
_HAS_COLUMN = text("SELECT 1 FROM information_schema.columns WHERE table_schema = :schema "
                   "AND table_name = 'call_dispositions' AND column_name = 'gate_failed'")


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    if not conn.execute(_HAS_COLUMN, {"schema": schema}).scalar():
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text(f'ALTER TABLE "{schema}".call_dispositions ADD COLUMN gate_failed boolean'))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_call_gate_failed complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
