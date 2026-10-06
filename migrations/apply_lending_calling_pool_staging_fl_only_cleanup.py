"""
F17 (pending-tasks DB audit, 2026-10-05): "Cleanup bad staging data: 18 NH + 1 WI
rows" in lending.calling_pool_staging.

These are mortgage_broker (List 4) rows matched by county name only, not state
(finding #9 / B4 — Hillsborough County, NH matched our "Hillsborough" filter).
That extraction bug is already fixed (pool_extraction.py now filters
``prim_state = 'FL'``), so this is a one-time cleanup of rows already staged
before the fix landed — it does not touch any other pool, and running it again
after the fix is a no-op.

Deletes every currently-staged mortgage_broker (and loan-originator) row whose
state is not FL — not just the two states already seen, so a different
out-of-state leak staged before the fix is also caught.

Idempotent: safe to re-run (the WHERE clause matches nothing once clean).

Run:
  PYTHONPATH=. python migrations/apply_lending_calling_pool_staging_fl_only_cleanup.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context


def run() -> None:
    with get_db_context() as session:
        result = session.execute(text(
            "DELETE FROM lending.calling_pool_staging "
            "WHERE pool_name = 'mortgage_broker' "
            "AND state IS NOT NULL AND upper(state) <> 'FL'"
        ))
        session.commit()
        print(f"apply_lending_calling_pool_staging_fl_only_cleanup: removed {result.rowcount} row(s)")


if __name__ == "__main__":
    run()
