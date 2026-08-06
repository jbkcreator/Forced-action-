"""
message_outcomes: atomic send-dedup (Follow-on 4, Direction 1a, of
system_decisions/lifecycle-notify-sweep-double-processing.md).

send_sms()/send_email() (src/agents/tools/write_tools.py) used a check-then-
insert dup check with no DB-level enforcement — two near-simultaneous calls
for the same (subscriber_id, template_id, variant_id) could both pass the
check and both send. This backs the dedup with a real unique index instead.

send_date buckets the previous rolling-24h window into a discrete calendar
day so a unique index can express it — a real constraint can't encode
"within the last 24 hours" directly. COALESCE(variant_id, '') is required
because Postgres never treats NULL = NULL as a match in a plain unique
constraint: a non-A/B-tested send (variant_id IS NULL, the common case)
would otherwise get zero protection from a naive column-list constraint.
message_type is included to close a second, pre-existing gap: the original
dup_q never filtered on it, so an SMS and an email sharing the same
template_id/variant_id could already false-positive against each other.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_message_outcomes_dedup_constraint.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

DDL = [
    "ALTER TABLE message_outcomes ADD COLUMN IF NOT EXISTS send_date date",
    "UPDATE message_outcomes SET send_date = date(created_at) WHERE send_date IS NULL",
    "DROP INDEX IF EXISTS uq_message_outcomes_dedup",
    """
    CREATE UNIQUE INDEX uq_message_outcomes_dedup ON message_outcomes (
        subscriber_id, template_id, COALESCE(variant_id, ''), message_type, send_date
    )
    """,
]


def main() -> None:
    with get_db_context() as db:
        for stmt in DDL:
            db.execute(text(stmt))
        db.commit()
    print("apply_message_outcomes_dedup_constraint: done.")


if __name__ == "__main__":
    main()
