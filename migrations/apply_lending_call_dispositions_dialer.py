"""Make ``lending.call_dispositions`` vendor-neutral and add the dialer-logging columns.

Follows apply_lending_call_dispositions.py (which created the Aircall-era table). Additive
and re-runnable in any order with the renames landing in code:

- renames aircall_* / caller_line / disposition_tag_raw to vendor-neutral names (only if the old name exists)
- drops the old 5-code CHECK (codes are validated in config/lending_dispositions.py so an
  unknown code can be stored raw) and multiple_dispositions
- widens code/id columns; dialer_load_records.dialer_contact_id becomes varchar
- adds unfunded-cause, list-version, recording, missing-disposition and booking-blocked columns
- creates lending.missed_call_events (one sendable row per phone per Eastern day)

Usage:
    PYTHONPATH=. python migrations/apply_lending_call_dispositions_dialer.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingMissedCallEvent

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RENAMES = {
    "call_dispositions": (
        ("aircall_call_id", "dialer_call_id"),
        ("aircall_contact_id", "dialer_contact_id"),
        ("caller_line", "caller_id_number"),
        ("disposition_tag_raw", "disposition_raw"),
    ),
    "dialer_load_records": (("aircall_contact_id", "dialer_contact_id"),),
}
WIDEN = (
    ("call_dispositions", "dialer_call_id", "varchar(100)"),
    ("call_dispositions", "direction", "varchar(20)"),
    ("call_dispositions", "caller_name", "varchar(200)"),
    ("call_dispositions", "caller_id_number", "varchar(50)"),
    ("call_dispositions", "campaign_tag", "varchar(50)"),
    ("call_dispositions", "dialer_contact_id", "varchar(64)"),
    ("call_dispositions", "disposition", "varchar(50)"),
    ("call_dispositions", "disposition_raw", "varchar(100)"),
    ("call_dispositions", "sheet_synced_disposition", "varchar(50)"),
    ("call_dispositions", "slack_posted_disposition", "varchar(50)"),
    ("call_dispositions", "slack_ts", "varchar(50)"),
    ("dialer_load_records", "dialer_contact_id", "varchar(64)"),
)
NEW_COLUMNS = (
    ("dialer_call_id", "varchar(100)"),
    ("dialer_contact_id", "varchar(64)"),
    ("caller_id_number", "varchar(50)"),
    ("disposition_raw", "varchar(100)"),
    ("disposition_list_version", "varchar(20)"),
    ("unfunded_cause", "varchar(30)"),
    ("unfunded_cause_provisional", "boolean NOT NULL DEFAULT false"),
    ("disposition_missing_alerted_at", "timestamptz"),
    ("recording_ref", "varchar(500)"),
    ("booking_blocked", "boolean NOT NULL DEFAULT false"),
    ("queue", "varchar(30)"),
    ("source_tag", "varchar(40)"),
    ("seat_group", "varchar(10)"),
)


def _columns(conn: Connection, schema: str, table: str) -> set[str]:
    return {r[0] for r in conn.execute(
        text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t"),
        {"s": schema, "t": table})}


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    """Run every step on ``conn`` (the caller owns the transaction, so tests can roll it back)."""
    s = f'"{schema}"'
    call_cols = _columns(conn, schema, "call_dispositions")
    if not call_cols:
        raise RuntimeError("run apply_lending_call_dispositions.py first: lending.call_dispositions is missing")

    for table, pairs in RENAMES.items():
        cols = _columns(conn, schema, table)
        for old, new in pairs:
            if old in cols and new not in cols:
                conn.execute(text(f"ALTER TABLE {s}.{table} RENAME COLUMN {old} TO {new}"))

    conn.execute(text(f"ALTER TABLE {s}.call_dispositions DROP COLUMN IF EXISTS multiple_dispositions"))
    conn.execute(text(f"ALTER TABLE {s}.call_dispositions DROP CONSTRAINT IF EXISTS ck_lending_call_dispositions_disposition"))
    conn.execute(text(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'call_dispositions_aircall_call_id_key' "
        f"AND conrelid = '{s}.call_dispositions'::regclass) THEN "
        f"ALTER TABLE {s}.call_dispositions RENAME CONSTRAINT call_dispositions_aircall_call_id_key "
        "TO call_dispositions_dialer_call_id_key; END IF; END $$"))

    for name, ddl in NEW_COLUMNS:
        conn.execute(text(f"ALTER TABLE {s}.call_dispositions ADD COLUMN IF NOT EXISTS {name} {ddl}"))

    load_cols = _columns(conn, schema, "dialer_load_records")
    for table, column, ddl in WIDEN:
        if table == "dialer_load_records" and column not in load_cols:
            continue
        using = f" USING {column}::text" if table == "dialer_load_records" else ""
        conn.execute(text(f"ALTER TABLE {s}.{table} ALTER COLUMN {column} TYPE {ddl}{using}"))

    conn.execute(text(
        f"CREATE INDEX IF NOT EXISTS idx_lending_call_dispositions_undelivered ON {s}.call_dispositions (disposition_at) "
        "WHERE sheet_synced_disposition IS DISTINCT FROM disposition "
        "OR slack_posted_disposition IS DISTINCT FROM disposition"))

    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingMissedCallEvent.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_call_dispositions_dialer complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
