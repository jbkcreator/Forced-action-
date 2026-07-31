"""
THROUGH-v2.2 T4 — create standing_orders table.

Idempotent: safe to re-run.
"""
import os
import sys

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def main() -> None:
    from config.settings import get_settings
    settings = get_settings()
    conn = psycopg2.connect(str(settings.database_url))
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS standing_orders (
            id               SERIAL PRIMARY KEY,
            action_type      VARCHAR(80)  NOT NULL,
            vertical         VARCHAR(80)  NOT NULL,
            template_id      VARCHAR(120),
            conditions       JSONB        NOT NULL DEFAULT '[]',
            status           VARCHAR(20)  NOT NULL DEFAULT 'proposed',
            proposed_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
            ratified_at      TIMESTAMPTZ,
            declined_at      TIMESTAMPTZ,
            archived_at      TIMESTAMPTZ,
            ratified_by      VARCHAR(120),
            slack_message_ts VARCHAR(30),
            approval_count_at_proposal INT NOT NULL DEFAULT 0,
            updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
            CONSTRAINT uq_standing_orders_action_vertical UNIQUE (action_type, vertical),
            CONSTRAINT ck_standing_orders_status CHECK (
                status IN ('proposed', 'ratified', 'declined', 'archived')
            )
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS ix_standing_orders_status
        ON standing_orders (status)
    """)

    conn.close()
    print("apply_standing_orders: done.")


if __name__ == "__main__":
    main()
