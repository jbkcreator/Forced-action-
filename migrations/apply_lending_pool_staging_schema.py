"""Move the calling-pool staging table into the lending schema (ADR 0001).

``public.lending_calling_pool_staging`` becomes ``lending.calling_pool_staging`` with
its data, sequence, indexes and constraints. A plain view keeps the old name in
``public``, so any writer or reader still using it keeps working until it moves.
Idempotent; run after apply_lending_pool_source_tags.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_pool_staging_schema.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OLD_NAME = "lending_calling_pool_staging"
NEW_NAME = "calling_pool_staging"


def _kind(conn, schema: str, name: str):
    return conn.execute(text("SELECT table_type FROM information_schema.tables "
                             "WHERE table_schema = :s AND table_name = :n"), {"s": schema, "n": name}).scalar()


def apply(engine: Engine | None = None, source_schema: str = "public", target_schema: str = "lending") -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    src, dst = f'"{source_schema}"', f'"{target_schema}"'
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {dst}"))
        if _kind(conn, source_schema, OLD_NAME) == "BASE TABLE" and _kind(conn, target_schema, NEW_NAME) is None:
            conn.execute(text(f"ALTER TABLE {src}.{OLD_NAME} SET SCHEMA {dst}"))
            conn.execute(text(f"ALTER TABLE {dst}.{OLD_NAME} RENAME TO {NEW_NAME}"))
            logger.info("moved %s.%s -> %s.%s", source_schema, OLD_NAME, target_schema, NEW_NAME)
        if _kind(conn, target_schema, NEW_NAME) == "BASE TABLE" and _kind(conn, source_schema, OLD_NAME) is None:
            conn.execute(text(f"CREATE VIEW {src}.{OLD_NAME} AS SELECT * FROM {dst}.{NEW_NAME}"))
    logger.info("apply_lending_pool_staging_schema complete (%s.%s).", target_schema, NEW_NAME)


if __name__ == "__main__":
    apply()
