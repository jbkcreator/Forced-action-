"""
PR #131 review follow-up — functional index for case-insensitive email lookups
on bankruptcy_alert_subscriptions.

_already_subscribed() (invite.py) and _invite_conversion_stats() (alerts.py)
both match on LOWER(email). The existing plain btree idx_bkalert_sub_email
indexes the raw value and can't serve a lower() predicate, so those lookups
seq-scan. This adds a functional index on lower(email).

Idempotent: CREATE INDEX IF NOT EXISTS.

    PYTHONPATH=. python migrations/apply_fa_s3_bkalert_email_lower_idx.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_bkalert_sub_email_lower "
    "ON bankruptcy_alert_subscriptions (lower(email))",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        exists = db.execute(text(
            "SELECT to_regclass('public.idx_bkalert_sub_email_lower')"
        )).scalar()
    print("idx_bkalert_sub_email_lower:", exists)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
