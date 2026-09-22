"""WP-T2-12 — fa_max_thread_fallback_log table.

Audit log for the card-thread fallback responder. One row per invocation of
`_handle_relay_thread_action`'s else-branch (non-approve/reject replies from
the authorized approver). Safe to re-run — all DDL is CREATE IF NOT EXISTS.

Run after all WP-T2-2 migrations (fa_max_tool_call_log must exist, but this
table is independent of it).

Usage:
    PYTHONPATH=. python migrations/apply_fa_max_wp_t2_12_thread_fallback_log.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "CREATE fa_max_thread_fallback_log",
        """
        CREATE TABLE IF NOT EXISTS fa_max_thread_fallback_log (
            id              BIGSERIAL PRIMARY KEY,
            relay_item_id   BIGINT NOT NULL,
            slack_user_id   VARCHAR(60) NOT NULL,
            thread_ts       VARCHAR(40) NOT NULL,
            lane            VARCHAR(20),
            raw_text        TEXT,
            bucket          VARCHAR(20) NOT NULL,
            lookup_id       VARCHAR(60),
            reply_sent      TEXT,
            tokens_in       INTEGER,
            tokens_out      INTEGER,
            cost_usd        NUMERIC(12, 6),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ck_fa_max_thread_fallback_bucket
                CHECK (bucket IN ('simple_lookup', 'cc_query', 'social', 'other'))
        );
        """,
    ),
    (
        "ADD COLUMN lane (idempotent — safe if table already has it)",
        """
        ALTER TABLE fa_max_thread_fallback_log
            ADD COLUMN IF NOT EXISTS lane VARCHAR(20);
        """,
    ),
    (
        "INDEX ix_fa_max_thread_fallback_relay_item",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_thread_fallback_relay_item
            ON fa_max_thread_fallback_log (relay_item_id);
        """,
    ),
    (
        "INDEX ix_fa_max_thread_fallback_created",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_thread_fallback_created
            ON fa_max_thread_fallback_log (created_at DESC);
        """,
    ),
]


def main() -> None:
    print("Applying WP-T2-12 migration: fa_max_thread_fallback_log")
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  -> {label}")
            session.execute(text(sql))
        session.commit()

    # Verify
    with get_db_context() as session:
        result = session.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'fa_max_thread_fallback_log'
                ORDER BY ordinal_position
            """)
        ).scalars().all()
        if result:
            print(f"  OK: fa_max_thread_fallback_log has columns: {', '.join(result)}")
        else:
            print("  ERROR: table not found after migration")


if __name__ == "__main__":
    main()
