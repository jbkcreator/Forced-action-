"""
B1-04 fix — add checkout_recovery.founder_alerted_at.

The founder-alert dedup guard now lives on the row (stamped once, at the
first confirmed-abandonment sweep touch) instead of firing unconditionally
at row creation. Needed so repeated sweep runs, and reopened episodes for a
previously-closed email, don't re-alert.

Idempotent: ADD COLUMN IF NOT EXISTS.

    PYTHONPATH=. python migrations/apply_checkout_recovery_founder_alert.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    "ALTER TABLE checkout_recovery ADD COLUMN IF NOT EXISTS founder_alerted_at TIMESTAMPTZ",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
    print("checkout_recovery.founder_alerted_at added")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
