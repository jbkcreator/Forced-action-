"""
lending.first_contact_snapshots: one write-once row per phone at first contact.

Holds the scoring inputs with their provenance, the rank, source tag, caller,
script version and date as they stood when a caller first reached the phone
(src/lending/first_contact.py). Rows are never updated, so point-in-time
cohorts can be rebuilt when a trained model replaces the rules.

Idempotent: safe to re-run.

Run:
  PYTHONPATH=. python migrations/apply_lending_first_contact_snapshots.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    "CREATE SCHEMA IF NOT EXISTS lending;",
    """
    CREATE TABLE IF NOT EXISTS lending.first_contact_snapshots (
        phone             VARCHAR(20)  PRIMARY KEY,
        first_contact_at  TIMESTAMPTZ  NOT NULL,
        caller_seat       TEXT,
        script_version    TEXT,
        source_tag        TEXT,
        queue             TEXT,
        warm              BOOLEAN      NOT NULL DEFAULT FALSE,
        rank              SMALLINT     NOT NULL CHECK (rank BETWEEN 1 AND 10),
        signals           JSONB        NOT NULL,
        labels            JSONB        NOT NULL,
        created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_lending_first_contact_snapshots_contact_at
        ON lending.first_contact_snapshots (first_contact_at);
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_lending_first_contact_snapshots: done")


if __name__ == "__main__":
    main()
