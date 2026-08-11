"""Idempotent: add is_test column to subscribers.

Marks rows created during QA/testing so revenue dashboards can exclude them.
Admin-only — the normal checkout path never sets this; it defaults false.
"""
from sqlalchemy import text

from src.core.database import Database

DDL = [
    "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS is_test BOOLEAN NOT NULL DEFAULT FALSE",
    "CREATE INDEX IF NOT EXISTS ix_subscribers_is_test ON subscribers (is_test) WHERE is_test = TRUE",
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("apply_add_subscriber_is_test: done")


if __name__ == "__main__":
    main()
