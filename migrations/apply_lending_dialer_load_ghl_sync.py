"""GHL sync columns on ``lending.dialer_load_records`` (src/lending/ghl_load_sync.py).

Adds ``ghl_sync_status`` / ``ghl_sync_attempts`` / ``ghl_synced_at`` / ``ghl_sync_error`` and a partial index on
the rows still to sync. Backfill: active ``verified_maturity`` and ``wholesalers`` rows loaded before 2026-11-04 are already in GHL
(one-off push 2026-11-04 and the wholesaler load's own push), so they are marked ``pushed``; the cutoff keeps a
rerun from marking any later load. Every other active row is left NULL and the sync pushes it once enabled.

Idempotent: ``ADD COLUMN IF NOT EXISTS``, ``CREATE INDEX IF NOT EXISTS``, backfill only touches NULL rows.
Run BEFORE deploying the code (the dialer load ORM model reads the new columns).

Usage:
    PYTHONPATH=. python migrations/apply_lending_dialer_load_ghl_sync.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ALREADY_IN_GHL_POOLS = ("verified_maturity", "wholesalers")
# Only loads from before the sync existed were pushed by hand; a rerun must never mark a later load as pushed.
ALREADY_IN_GHL_LOADED_BEFORE = "2026-11-04T00:00:00+00:00"


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    table = f'"{schema}".dialer_load_records'
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text(
            f"ALTER TABLE {table} "
            "ADD COLUMN IF NOT EXISTS ghl_sync_status VARCHAR(40), "
            "ADD COLUMN IF NOT EXISTS ghl_sync_attempts INTEGER NOT NULL DEFAULT 0, "
            "ADD COLUMN IF NOT EXISTS ghl_synced_at TIMESTAMPTZ, "
            "ADD COLUMN IF NOT EXISTS ghl_sync_error VARCHAR(200)"
        ))
        # A first run created VARCHAR(20), too short for skipped_already_in_ghl; widening a varchar is metadata-only.
        conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN ghl_sync_status TYPE VARCHAR(40)"))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_lending_dialer_load_records_ghl_pending "
            f"ON {table} (id) WHERE active AND ghl_sync_status IS NULL"
        ))
        backfilled = conn.execute(text(
            f"UPDATE {table} SET ghl_sync_status = 'pushed', ghl_synced_at = now() "
            "WHERE active AND ghl_sync_status IS NULL AND pool = ANY(:pools) AND loaded_at < CAST(:before AS timestamptz)"
        ), {"pools": list(ALREADY_IN_GHL_POOLS), "before": ALREADY_IN_GHL_LOADED_BEFORE}).rowcount
    logger.info("apply_lending_dialer_load_ghl_sync complete (schema=%s, backfilled=%d).", schema, backfilled)


if __name__ == "__main__":
    apply()
