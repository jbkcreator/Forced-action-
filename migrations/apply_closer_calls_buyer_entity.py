"""
closer_calls: allow a call to be with a whale prospect, not just a subscriber
(client item 49 — wiring Hunter's ranked whale queue into the closers' call
list). Adds buyer_entity_id, relaxes subscriber_id to nullable, and enforces
"exactly one of the two" via ck_closer_calls_one_identity so a row can never
be ambiguous or orphaned between the two identity types.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_closer_calls_buyer_entity.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

DDL = [
    "ALTER TABLE closer_calls ADD COLUMN IF NOT EXISTS buyer_entity_id INTEGER "
    "REFERENCES buyer_entities(id)",
    "ALTER TABLE closer_calls ALTER COLUMN subscriber_id DROP NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_closer_calls_buyer_entity ON closer_calls (buyer_entity_id)",
]

# ADD CONSTRAINT has no IF NOT EXISTS — drop-then-add keeps it idempotent.
CONSTRAINTS = [
    ("ck_closer_calls_one_identity", "(subscriber_id IS NOT NULL) != (buyer_entity_id IS NOT NULL)"),
]


def main() -> None:
    with get_db_context() as db:
        for stmt in DDL:
            db.execute(text(stmt))
        for name, expr in CONSTRAINTS:
            db.execute(text(f"ALTER TABLE closer_calls DROP CONSTRAINT IF EXISTS {name}"))
            db.execute(text(f"ALTER TABLE closer_calls ADD CONSTRAINT {name} CHECK ({expr})"))
        db.commit()
    print("apply_closer_calls_buyer_entity: done.")


if __name__ == "__main__":
    main()
