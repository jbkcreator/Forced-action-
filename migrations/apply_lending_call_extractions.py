"""
lending.call_extractions: the twelve fields extracted from each call transcript.

One row per dialer call (src/lending/call_extraction_store.py). Re-extracting a
call replaces its row. The fields are stored as JSON so the frozen field list
can grow without a schema change.

Idempotent: safe to re-run.

Run:
  PYTHONPATH=. python migrations/apply_lending_call_extractions.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    "CREATE SCHEMA IF NOT EXISTS lending;",
    """
    CREATE TABLE IF NOT EXISTS lending.call_extractions (
        dialer_call_id  VARCHAR(64)  PRIMARY KEY,
        phone           VARCHAR(20),
        fields          JSONB        NOT NULL,
        extracted_at    TIMESTAMPTZ  NOT NULL,
        created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_lending_call_extractions_phone
        ON lending.call_extractions (phone);
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_lending_call_extractions: done")


if __name__ == "__main__":
    main()
