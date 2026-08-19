"""Add deliveries.notified_at watermark for batched lead-delivery notifications.

NULL = delivery not yet emailed to the contractor. Timestamp = summary email sent.
The batch notifier selects `notified_at IS NULL`, groups by recipient, sends one
email per recipient, then stamps the timestamp — crash-safe and idempotent.

Idempotent. Run once against the shared DB:

    PYTHONPATH=. python migrations/apply_deliveries_notified_at.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

DDL = [
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS idx_deliveries_notified_at ON deliveries (notified_at)",
]


def main() -> None:
    with get_db_context() as db:
        for stmt in DDL:
            db.execute(text(stmt))
        db.commit()
        print("applied: deliveries.notified_at + index")


if __name__ == "__main__":
    main()
