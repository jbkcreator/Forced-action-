"""Q2 review fix: ``lending.soft_approvals.lead_ref``, the LendingFlow lead (``lead_uuid``) a Call-One submission assessed.

The post-call pre-qual letter is sent for that lead only, by the hand-off and by the pre-qual sweep's recovery, so a
newer lead on the same phone never inherits an earlier deal's approval. Rows saved before this column are left NULL
and are never recovered (they name no assessed lead). Idempotent; run after apply_lending_soft_approvals.py and BEFORE
deploying the code (the soft approval insert writes the column).

Usage:
    PYTHONPATH=. python migrations/apply_lending_soft_approvals_lead_ref.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ALTER = 'ALTER TABLE "{t}".soft_approvals ADD COLUMN IF NOT EXISTS lead_ref VARCHAR(36)'


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    if conn.execute(text("SELECT to_regclass(:t)"), {"t": f"{schema}.soft_approvals"}).scalar() is None:
        logger.info("apply_lending_soft_approvals_lead_ref: no soft_approvals table yet; run "
                    "apply_lending_soft_approvals.py first (it creates the table with this column).")
        return
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text(_ALTER.format(t=schema)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_soft_approvals_lead_ref complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
