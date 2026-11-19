"""``lending.instantly_block_queue`` (P1.3): opted-out addresses waiting to be added to NDL's Instantly block list.

Rows are added by ``compliance._propagate`` (``src/lending/instantly_blocklist.enqueue``) and worked off by Cora's
minute sweep (``BlockListSweep``). Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema).
Run it BEFORE deploying the code: without the table an opt-out is still recorded, but its address is not queued.

Usage:
    PYTHONPATH=. python migrations/apply_lending_instantly_block_queue.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    table = f'"{schema}".instantly_block_queue'
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id              bigserial    PRIMARY KEY,
            email           varchar(320) NOT NULL,
            status          varchar(10)  NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'done', 'failed')),
            attempts        integer      NOT NULL DEFAULT 0,
            next_attempt_at timestamptz  NOT NULL DEFAULT now(),
            last_error      varchar(200),
            created_at      timestamptz  NOT NULL DEFAULT now(),
            done_at         timestamptz,
            CONSTRAINT uq_lending_instantly_block_queue_email UNIQUE (email)
        )"""))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_lending_instantly_block_queue_pending "
                      f"ON {table} (next_attempt_at) WHERE status = 'pending'"))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_instantly_block_queue complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
