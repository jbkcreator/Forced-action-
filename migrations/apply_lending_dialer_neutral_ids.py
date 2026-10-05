"""Vendor-neutral dialer id columns on the lending call log and load records.

Same names and types as PR #319 (apply_lending_call_dispositions_dialer.py), which
skips any rename already done, so the two scripts can run in either order:

- call_dispositions: aircall_call_id -> dialer_call_id, aircall_contact_id ->
  dialer_contact_id (varchar(64)), caller_line -> caller_id_number; unique
  constraint renamed to call_dispositions_dialer_call_id_key.
- dialer_load_records: aircall_contact_id (bigint) -> dialer_contact_id varchar(64).

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_lending_dialer_neutral_ids.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RENAMES = {
    "call_dispositions": (
        ("aircall_call_id", "dialer_call_id"),
        ("aircall_contact_id", "dialer_contact_id"),
        ("caller_line", "caller_id_number"),
    ),
    "dialer_load_records": (("aircall_contact_id", "dialer_contact_id"),),
}


def _columns(conn: Connection, schema: str, table: str) -> set[str]:
    return set(conn.execute(
        text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t"),
        {"s": schema, "t": table}).scalars())


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    s = f'"{schema}"'
    for table, pairs in RENAMES.items():
        cols = _columns(conn, schema, table)
        for old, new in pairs:
            if old in cols and new not in cols:
                conn.execute(text(f"ALTER TABLE {s}.{table} RENAME COLUMN {old} TO {new}"))
    conn.execute(text(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'call_dispositions_aircall_call_id_key' "
        f"AND conrelid = '{s}.call_dispositions'::regclass) THEN "
        f"ALTER TABLE {s}.call_dispositions RENAME CONSTRAINT call_dispositions_aircall_call_id_key "
        "TO call_dispositions_dialer_call_id_key; END IF; END $$"))
    if "dialer_contact_id" in _columns(conn, schema, "call_dispositions"):
        conn.execute(text(f"ALTER TABLE {s}.call_dispositions ALTER COLUMN dialer_contact_id TYPE varchar(64)"))
    if "dialer_contact_id" in _columns(conn, schema, "dialer_load_records"):
        conn.execute(text(f"ALTER TABLE {s}.dialer_load_records ALTER COLUMN dialer_contact_id "
                          "TYPE varchar(64) USING dialer_contact_id::text"))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_dialer_neutral_ids complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
