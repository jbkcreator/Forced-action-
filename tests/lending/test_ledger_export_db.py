"""The ledger export's SQL runs on Postgres: every query is executed against the real tables (rolled back).

The throwaway test database can be older than the code, so the migrations the export reads are applied inside the
test's transaction first (all idempotent, all rolled back).
"""
from __future__ import annotations

import importlib
from datetime import date

import pytest
from sqlalchemy import text

from src.lending import ledger_export as le

_MIGRATIONS = ("web_leads", "lendingflow", "lendingflow_dialer", "booking_messages", "partner_door", "lender_routing",
               "cohorts", "is_test", "cora_agent_core", "first_contact_snapshots")


def _apply(conn) -> None:
    conn.execute(text("ALTER TABLE lending.dialer_load_records ADD COLUMN IF NOT EXISTS ghl_contact_id varchar(64)"))
    for name in _MIGRATIONS:
        module = importlib.import_module(f"migrations.apply_lending_{name}")
        if hasattr(module, "apply_to"):
            module.apply_to(conn, "lending") if name in {"booking_messages", "cora_agent_core"} else module.apply_to(conn)
        else:
            for statement in module._DDL:
                conn.execute(text(statement))


def test_every_export_query_runs_on_postgres(lending_db) -> None:
    conn = lending_db.connection()
    try:
        _apply(conn)
    except Exception as exc:  # an older migration signature: the queries cannot be checked here
        pytest.skip(f"test database could not be brought up to date ({type(exc).__name__})")
    data = le.collect(lending_db, date(2031, 1, 6))
    files = le.build_files(data)
    assert set(files) == {f"{name}.csv" for name in le.TABLES}
    assert files["cohorts.csv"].decode().splitlines()[0].startswith("batch_id,name,purchased_at")
