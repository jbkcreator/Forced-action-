"""T-B13-02 — allow the 'outcome.recorded' event type on the outbox.

The events table's ck_events_event_type CHECK allow-lists event types. Block 13
emits a new 'outcome.recorded' event from deal-capture, so widen the constraint
to include it. Additive: existing event types and their producers/consumers are
unaffected.

Idempotent (reads the current allowed set, adds the value, rewrites the check).
Usage:
    PYTHONPATH=. python migrations/apply_b13_02_outcome_event_type.py
"""

import re

from sqlalchemy import text
from src.core.database import Database

CONSTRAINT = "ck_events_event_type"
TABLE = "events"
COLUMN = "event_type"
REQUIRED = {"outcome.recorded"}


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        row = s.execute(
            text(
                "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint "
                "WHERE conname = :name AND conrelid = CAST(:table AS regclass)"
            ),
            {"name": CONSTRAINT, "table": TABLE},
        ).first()
        current = set(re.findall(r"'([^']+)'", row._mapping["def"])) if row else set()
        union = sorted(current | REQUIRED)
        values_sql = ",".join(f"'{v}'" for v in union)
        s.execute(text(f"ALTER TABLE {TABLE} DROP CONSTRAINT IF EXISTS {CONSTRAINT}"))
        s.execute(
            text(f"ALTER TABLE {TABLE} ADD CONSTRAINT {CONSTRAINT} "
                 f"CHECK ({COLUMN} IN ({values_sql}))")
        )
    print(f"{CONSTRAINT} now allows: {sorted(REQUIRED - current) or '(no new values)'}")


if __name__ == "__main__":
    main()
