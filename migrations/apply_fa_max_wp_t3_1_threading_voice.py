"""WP-T3-1 — Slack threading & voice-note intake tables.

fa_max_pending_slots: one short-lived Revise / Log-call slot per approver.
fa_max_draft_revisions: append-only revision history per relay draft (working memory).

Safe to re-run — all DDL is IF NOT EXISTS. Requires relay_approval_queue (FK).

Usage:
    PYTHONPATH=. python migrations/apply_fa_max_wp_t3_1_threading_voice.py
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

TABLES = ("fa_max_pending_slots", "fa_max_draft_revisions", "fa_max_call_dispositions")

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
    (
        "CREATE fa_max_draft_revisions",
        """
        CREATE TABLE IF NOT EXISTS fa_max_draft_revisions (
            id              BIGSERIAL PRIMARY KEY,
            relay_item_id   BIGINT NOT NULL REFERENCES relay_approval_queue (id),
            revision_no     INTEGER NOT NULL,
            source          VARCHAR(10) NOT NULL,
            instruction     TEXT,
            before_text     TEXT NOT NULL,
            after_text      TEXT NOT NULL,
            material_edit   BOOLEAN NOT NULL,
            revised_by      VARCHAR(80) NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ck_fa_max_draft_revision_source CHECK (source IN ('modal', 'nl')),
            CONSTRAINT uq_fa_max_draft_revision_item_no UNIQUE (relay_item_id, revision_no)
        );
        """,
    ),
    (
        "INDEX ix_fa_max_draft_revisions_item",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_draft_revisions_item
            ON fa_max_draft_revisions (relay_item_id);
        """,
    ),
    (
        "CREATE fa_max_call_dispositions",
        """
        CREATE TABLE IF NOT EXISTS fa_max_call_dispositions (
            id              BIGSERIAL PRIMARY KEY,
            interaction_id  UUID NOT NULL REFERENCES fa_max_interactions (interaction_id),
            person_id       UUID NOT NULL REFERENCES fa_max_persons (person_id),
            opportunity_id  UUID NOT NULL REFERENCES fa_max_opportunities (opportunity_id),
            outcome         VARCHAR(40) NOT NULL,
            summary         VARCHAR(280) NOT NULL,
            next_action     VARCHAR(140),
            next_action_due DATE,
            slack_user_id   VARCHAR(60) NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ck_fa_max_call_disposition_outcome CHECK (outcome IN (
                'connected_interested','connected_not_interested',
                'callback_requested','voicemail','no_answer','wrong_number','unclear'
            ))
        );
        """,
    ),
    (
        "INDEX ix_fa_max_call_dispositions_person_created",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_call_dispositions_person_created
            ON fa_max_call_dispositions (person_id, created_at);
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
