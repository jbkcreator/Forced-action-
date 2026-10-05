"""
PropertyRadar live-trace contact persistence (finding #4 fix).

Creates property_radar_trace_contacts: one row per traced address key
(trace_key = normalized property_address + zip), holding the emails/phones
Tracerfy returned. The skip-trace ledger (enrichment_usage_logs) already
prevents re-billing a key once it's been traced, but it never stored the
contact *value* itself — so a lead skipped by the handoff for an unrelated
reason this run (a stale Backflip feed, outside the thin path, not yet
active) had its paid-for contacts thrown away and could never be fetched
again without paying Tracerfy a second time. This table is what a later
run reads back for an already-traced key instead of treating it as
no_contact_data forever.

Idempotent: safe to re-run (CREATE TABLE IF NOT EXISTS).

Run:
  PYTHONPATH=. python migrations/apply_property_radar_trace_contacts.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS property_radar_trace_contacts (
        trace_key   VARCHAR(255) PRIMARY KEY,
        emails      JSONB        NOT NULL DEFAULT '[]'::jsonb,
        phones      JSONB        NOT NULL DEFAULT '[]'::jsonb,
        traced_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
    )
    """,
]


def run() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
        session.commit()
    print("apply_property_radar_trace_contacts: done")


if __name__ == "__main__":
    run()
