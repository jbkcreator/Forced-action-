"""Add deliveries.notified_at watermark for batched lead-delivery notifications.

NULL = delivery not yet emailed to the contractor. Timestamp = summary email sent.
The batch notifier selects `notified_at IS NULL`, groups by recipient, sends one
email per recipient, then stamps the timestamp — crash-safe and idempotent.

One-time backfill: every delivery that predates this column was already emailed by
the old per-lead flow. Without a backfill the batch notifier would treat all of
them as un-notified and re-blast historical (and possibly stale) leads on the
first sweep after deploy. So the FIRST time the column is added we stamp every
existing row as already-notified (COALESCE(delivered_at, now())). The backfill is
guarded on the column not already existing, so re-runs never stamp genuinely
pending rows created by the new flow — keeping the whole script idempotent.

Idempotent. Run once against the shared DB:

    PYTHONPATH=. python migrations/apply_deliveries_notified_at.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

COLUMN_EXISTS_SQL = text("""
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'deliveries' AND column_name = 'notified_at'
""")

DDL = [
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS idx_deliveries_notified_at ON deliveries (notified_at)",
]

# Only runs when the column did not exist before this migration — see module docstring.
BACKFILL_SQL = text("""
    UPDATE deliveries
    SET notified_at = COALESCE(delivered_at, now())
    WHERE notified_at IS NULL
""")


def main() -> None:
    with get_db_context() as db:
        column_existed = db.execute(COLUMN_EXISTS_SQL).first() is not None
        for stmt in DDL:
            db.execute(text(stmt))
        if not column_existed:
            result = db.execute(BACKFILL_SQL)
            print(f"backfilled {result.rowcount} existing deliveries as already-notified")
        else:
            print("column already existed — skipping backfill")
        db.commit()
        print("applied: deliveries.notified_at + index")


if __name__ == "__main__":
    main()
