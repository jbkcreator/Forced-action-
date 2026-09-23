"""WP-T3-1 — Slack threading & voice-note intake tables.

fa_max_pending_slots: one short-lived Revise / Log-call slot per approver.

Safe to re-run — all DDL is IF NOT EXISTS. Independent of other FA Max tables.

Usage:
    PYTHONPATH=. python migrations/apply_fa_max_wp_t3_1_threading_voice.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

TABLES = ("fa_max_pending_slots",)

STATEMENTS: list[tuple[str, str]] = [
    (
        "CREATE fa_max_pending_slots",
        """
        CREATE TABLE IF NOT EXISTS fa_max_pending_slots (
            slack_user_id   VARCHAR(60) PRIMARY KEY,
            kind            VARCHAR(10) NOT NULL,
            target_ref      VARCHAR(64) NOT NULL,
            channel_id      VARCHAR(40) NOT NULL,
            thread_ts       VARCHAR(40),
            set_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ck_fa_max_pending_slot_kind CHECK (kind IN ('revise', 'voice'))
        );
        """,
    ),
]


def main() -> None:
    print("Applying WP-T3-1 migration: threading + voice intake")
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  -> {label}")
            session.execute(text(sql))
        session.commit()

    with get_db_context() as session:
        for table in TABLES:
            cols = session.execute(
                text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = :t ORDER BY ordinal_position
                """),
                {"t": table},
            ).scalars().all()
            print(f"  {'OK' if cols else 'ERROR'}: {table} columns: {', '.join(cols) or 'MISSING'}")


if __name__ == "__main__":
    main()
