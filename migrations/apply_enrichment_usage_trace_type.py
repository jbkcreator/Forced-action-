"""Add ``enrichment_usage_logs.trace_type``: the Tracerfy mode (normal / advanced) each trace was sent as.

The PropertyRadar trace code already writes this column (src/services/property_radar/live_trace.py,
src/tasks/property_radar_lead_handoff.py) and the skip-trace ledger reads it, but no migration added it, so a
database built from the scripts alone rejects every trace ledger write. Idempotent: a no-op where it exists.

Usage:
    PYTHONPATH=. python migrations/apply_enrichment_usage_trace_type.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply(engine: Engine | None = None, table: str = "enrichment_usage_logs") -> None:
    """``table`` is overridable so tests never touch the shared table."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS trace_type varchar(20)"))
    logger.info("apply_enrichment_usage_trace_type complete (table=%s).", table)


if __name__ == "__main__":
    apply()
