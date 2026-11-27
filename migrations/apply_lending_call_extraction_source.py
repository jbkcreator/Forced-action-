"""
lending.call_extractions: who the fields came from.

R1 1.6: a connected call with no recording has no transcript, so David types the fields into a Slack form
(src/lending/dialer/call_notes.py). ``source`` is 'transcript' (extracted by the model; every existing row) or
'caller_notes'; ``entered_by`` is the Slack user id behind a caller_notes row. A later transcript extraction
replaces a caller_notes row; notes never replace a transcript row.

Idempotent: safe to re-run. Run before deploying the code (the transcript upsert writes ``source``).

Run:
  PYTHONPATH=. python migrations/apply_lending_call_extraction_source.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    "ALTER TABLE lending.call_extractions ADD COLUMN IF NOT EXISTS source VARCHAR(20) NOT NULL DEFAULT 'transcript';",
    "ALTER TABLE lending.call_extractions ADD COLUMN IF NOT EXISTS entered_by VARCHAR(64);",
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_lending_call_extraction_source: done")


if __name__ == "__main__":
    main()
