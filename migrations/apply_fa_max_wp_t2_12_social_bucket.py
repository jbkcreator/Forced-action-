"""WP-T2-12 — widen fa_max_thread_fallback_log bucket CHECK for 'social'.

The channel responder gained a 'social' bucket (greetings/thanks/encouragement
get a brief friendly reply). The original CHECK constraint only allowed
simple_lookup / cc_query / other, so auditing a social reply raised a
CheckViolation. This drops and re-adds the constraint with 'social' included.

Idempotent: DROP CONSTRAINT IF EXISTS then ADD. Safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_fa_max_wp_t2_12_social_bucket.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "DROP old bucket CHECK (idempotent)",
        """
        ALTER TABLE fa_max_thread_fallback_log
            DROP CONSTRAINT IF EXISTS ck_fa_max_thread_fallback_bucket;
        """,
    ),
    (
        "ADD bucket CHECK including 'social'",
        """
        ALTER TABLE fa_max_thread_fallback_log
            ADD CONSTRAINT ck_fa_max_thread_fallback_bucket
            CHECK (bucket IN ('simple_lookup', 'cc_query', 'social', 'other'));
        """,
    ),
]


def main() -> None:
    print("Applying WP-T2-12 migration: widen fallback bucket CHECK for 'social'")
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  -> {label}")
            session.execute(text(sql))
        session.commit()

    with get_db_context() as session:
        row = session.execute(
            text("""
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conname = 'ck_fa_max_thread_fallback_bucket'
            """)
        ).scalar()
        print(f"  OK: constraint = {row}")


if __name__ == "__main__":
    main()
