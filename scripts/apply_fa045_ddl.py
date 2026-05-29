"""Apply fa045 operator-CRM DDL directly via psycopg.

Alembic CLI is unusable (multi-head tree). This script creates the two
new tables idempotently so dev/prod can move forward without resolving
heads. The matching migration file `fa045_operator_crm_tables.py` is
kept so the schema is recorded; on the next merge migration it will be
applied as a no-op (CREATE IF NOT EXISTS).

Usage: python scripts/apply_fa045_ddl.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    """
    CREATE TABLE IF NOT EXISTS subscriber_tags (
        id            SERIAL PRIMARY KEY,
        subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
        tag           VARCHAR(100) NOT NULL,
        created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (subscriber_id, tag)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_subscriber_tags_subscriber_id
        ON subscriber_tags (subscriber_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_subscriber_tags_tag
        ON subscriber_tags (tag)
    """,
    """
    CREATE TABLE IF NOT EXISTS subscriber_notes (
        id            SERIAL PRIMARY KEY,
        subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
        author_email  VARCHAR(255) NOT NULL,
        body          TEXT NOT NULL,
        pinned        BOOLEAN NOT NULL DEFAULT FALSE,
        created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_subscriber_notes_sub_pinned
        ON subscriber_notes (subscriber_id, pinned, created_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS deal_pipeline_events (
        id          SERIAL PRIMARY KEY,
        deal_id     INTEGER NOT NULL REFERENCES deal_outcomes(id),
        from_stage  VARCHAR(30),
        to_stage    VARCHAR(30) NOT NULL,
        changed_by  VARCHAR(255) NOT NULL,
        note        TEXT,
        created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_deal_pipeline_events_deal_created
        ON deal_pipeline_events (deal_id, created_at)
    """,
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("fa045 DDL applied (subscriber_tags, subscriber_notes, deal_pipeline_events).")


if __name__ == "__main__":
    main()
