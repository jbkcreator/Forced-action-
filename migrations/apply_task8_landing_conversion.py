"""
Task 8 landing conversion features (ADR 0029): adds the featured-testimonial
and founding-price-deadline columns to counties.

Idempotent (ADD COLUMN IF NOT EXISTS).

    PYTHONPATH=. python migrations/apply_task8_landing_conversion.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    "ALTER TABLE counties ADD COLUMN IF NOT EXISTS landing_featured_testimonial JSONB",
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
              AND column_name IN ('landing_featured_testimonial', 'founding_price_deadline_at')
            ORDER BY column_name
        """)).fetchall()
    print("counties columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
