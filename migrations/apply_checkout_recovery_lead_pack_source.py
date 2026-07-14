"""
Task 7 — widen checkout_recovery source CHECK to allow 'lead_pack'.

Lead-pack abandonment (existing subscriber closes the $99 PaymentIntent sheet)
is a third recovery source. The original apply_checkout_recovery.py created the
table with a 2-value source CHECK; this widens it.

Idempotent: the CHECK is dropped then re-added (drop-first makes re-runs safe).

    PYTHONPATH=. python migrations/apply_checkout_recovery_lead_pack_source.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    "ALTER TABLE checkout_recovery DROP CONSTRAINT IF EXISTS ck_checkout_recovery_source",
    """
    ALTER TABLE checkout_recovery ADD CONSTRAINT ck_checkout_recovery_source
    CHECK (source IN ('session_expired','pre_payment','lead_pack'))
    """,
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
    print("checkout_recovery source CHECK widened to include 'lead_pack'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
