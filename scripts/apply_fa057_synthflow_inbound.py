"""Apply fa057 synthflow inbound DDL directly via SQLAlchemy.

Alembic CLI is unusable (multi-head tree). Applies idempotently.

Usage: python scripts/apply_fa057_synthflow_inbound.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    """
    ALTER TABLE subscribers
    ADD COLUMN IF NOT EXISTS capture_complete BOOLEAN NOT NULL DEFAULT true
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_synthflow_inbound_event_id
    ON webhook_events (source, source_event_id)
    WHERE source = 'synthflow_inbound'
    """,
    """
    ALTER TABLE sms_opt_ins
    DROP CONSTRAINT IF EXISTS check_opt_in_source
    """,
    """
    ALTER TABLE sms_opt_ins
    ADD CONSTRAINT check_opt_in_source CHECK (
        source IN (
            'double_opt_in', 'manual', 'import', 'widget',
            'waitlist_form', 'synthflow_inbound', 'missed_call_inbound'
        )
    )
    """,
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("fa057 DDL applied: subscribers.capture_complete added, inbound idempotency index created.")


if __name__ == "__main__":
    main()
