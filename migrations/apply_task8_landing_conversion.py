"""
Task 8 landing conversion features (ADR 0029): adds the founding-price-
deadline column to counties.

The featured-testimonial column is NOT created here — it's owned entirely by
apply_task8_testimonials_array.py, which creates the plural
landing_featured_testimonials column from scratch if needed (it no longer
depends on this script having run first). Keeping that column out of this
script means the two migrations are independent: either can run alone, in
any order, on a fresh DB, and the ORM (which only ever knew about the plural
column) never sees a partially-applied state.

Idempotent (ADD COLUMN IF NOT EXISTS).

    PYTHONPATH=. python migrations/apply_task8_landing_conversion.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    "ALTER TABLE counties ADD COLUMN IF NOT EXISTS founding_price_deadline_at TIMESTAMPTZ",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='counties'
              AND column_name = 'founding_price_deadline_at'
        """)).fetchall()
    print("counties columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
