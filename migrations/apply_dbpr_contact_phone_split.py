"""
Split dbpr_contacts.phone into mobile_phone / landline_phone.

BatchData and IDI both return a line-type per phone number, but the
enrichment write path was collapsing mobile and landline into a single
`phone` column, losing the line-type distinction. This adds the two new
columns; `phone` is left in place unchanged (still set to whichever number
the enrichment write path treats as primary) for backward compatibility.

    PYTHONPATH=. python migrations/apply_dbpr_contact_phone_split.py

Idempotent (ADD COLUMN IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    "ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS mobile_phone VARCHAR(20)",
    "ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS landline_phone VARCHAR(20)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'dbpr_contacts' AND column_name IN ('phone', 'mobile_phone', 'landline_phone')
            ORDER BY column_name
        """)).fetchall()
    print("dbpr_contacts phone columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
