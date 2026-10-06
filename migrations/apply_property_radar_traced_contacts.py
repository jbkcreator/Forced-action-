"""
PropertyRadar live-trace contact store (PR #320 review fix).

Creates:
  property_radar_traced_contacts — phones/emails bought by the live Tracerfy trace,
                                   one row per staged radar_id. Written in the same
                                   transaction as the enrichment_usage_logs ledger
                                   rows, so a failed run cannot strand paid contacts.

Idempotent: safe to re-run. Run before the first `--live-trace`.

Run:
  PYTHONPATH=. python migrations/apply_property_radar_traced_contacts.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS property_radar_traced_contacts (
        radar_id   VARCHAR(50) PRIMARY KEY,
        phones     VARCHAR[]   NOT NULL DEFAULT '{}',
        emails     VARCHAR[]   NOT NULL DEFAULT '{}',
        traced_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
        session.commit()
    print("apply_property_radar_traced_contacts: done")


if __name__ == "__main__":
    main()
