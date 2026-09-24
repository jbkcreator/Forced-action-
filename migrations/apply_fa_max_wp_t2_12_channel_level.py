"""WP-T2-12 (channel-level extension) — relax relay_item_id to nullable.

The FA Max Slack responder now also answers top-level channel messages, which
have no originating relay card. Those audit rows carry a NULL relay_item_id.

Idempotent — DROP NOT NULL is a no-op if already nullable.

Usage:
    PYTHONPATH=. python migrations/apply_fa_max_wp_t2_12_channel_level.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "relay_item_id DROP NOT NULL (idempotent)",
        """
        ALTER TABLE fa_max_thread_fallback_log
            ALTER COLUMN relay_item_id DROP NOT NULL;
        """,
    ),
]


def main() -> None:
    print("Applying WP-T2-12 channel-level migration")
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  -> {label}")
            session.execute(text(sql))
        session.commit()

    with get_db_context() as session:
        is_nullable = session.execute(
            text("""
                SELECT is_nullable
                FROM information_schema.columns
                WHERE table_name = 'fa_max_thread_fallback_log'
                  AND column_name = 'relay_item_id'
            """)
        ).scalar()
        print(f"  OK: relay_item_id is_nullable={is_nullable}")


if __name__ == "__main__":
    main()
