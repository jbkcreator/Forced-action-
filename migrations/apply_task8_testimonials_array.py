"""
Task 8 revision: featured testimonial becomes an ordered array (carousel),
not a single slot (CONTEXT.md revision 2026-07-09).

Sole owner of counties.landing_featured_testimonials — creates it from
scratch if apply_task8_landing_conversion.py never ran (that script no
longer touches this column), or renames the legacy singular
landing_featured_testimonial and wraps any existing object value in a list
if it did. Either way, this script alone is sufficient; the two migrations
have no run-order dependency on each other.

Idempotent: rename only fires when the old column exists; the wrap-in-list
UPDATE only touches rows still holding a bare object (re-running after the
wrap is a no-op).

    PYTHONPATH=. python migrations/apply_task8_testimonials_array.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

def main() -> int:
    with get_db_context() as db:
        old_col_exists = db.execute(text("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name='counties' AND column_name='landing_featured_testimonial'
        """)).scalar_one_or_none()
        if old_col_exists:
            db.execute(text(
                "ALTER TABLE counties RENAME COLUMN landing_featured_testimonial TO landing_featured_testimonials"
            ))
        else:
            db.execute(text(
                "ALTER TABLE counties ADD COLUMN IF NOT EXISTS landing_featured_testimonials JSONB"
            ))
        db.execute(text("""
            UPDATE counties
            SET landing_featured_testimonials = jsonb_build_array(landing_featured_testimonials)
            WHERE jsonb_typeof(landing_featured_testimonials) = 'object'
        """))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='counties' AND column_name = 'landing_featured_testimonials'
        """)).fetchall()
    print("counties columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
