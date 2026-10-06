"""
WP-GL-5: Booking Gate & Nurture Routing — durable gate state.

fa_max_booking_gates stores one row per gate evaluation attempt.
A booking can only proceed when the most-recent row for its tracked_link_id
has result='pass'. Gate answers are stored as enum codes only — never free
text, never in the relay payload — to stay clear of the _FINANCIAL_TERMS
voice-intake regex and the ck_relay_fa_max_no_financial_payload DB CHECK.

Idempotent: safe to re-run. Run after apply_fa_max_bookings_integrity.py.

Run:
  PYTHONPATH=. python migrations/apply_fa_max_booking_gates.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS fa_max_booking_gates (
        gate_id         TEXT        PRIMARY KEY,
        tracked_link_id BIGINT      REFERENCES tracked_links(id) ON DELETE SET NULL,
        person_id       BIGINT,
        answers         JSONB       NOT NULL DEFAULT '{}',
        result          VARCHAR(10) NOT NULL,
        failed_field    VARCHAR(60),
        list_key        VARCHAR(80),
        rules_version   VARCHAR(20) NOT NULL DEFAULT '1.0',
        captured_by     VARCHAR(120),
        evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_booking_gates_result
            CHECK (result IN ('pass', 'fail'))
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_booking_gates_link
        ON fa_max_booking_gates (tracked_link_id, evaluated_at DESC)
        WHERE tracked_link_id IS NOT NULL;
    """,
    # Add gate_id FK to fa_max_bookings so each booking records which gate cleared it.
    """
    ALTER TABLE fa_max_bookings
        ADD COLUMN IF NOT EXISTS gate_id TEXT
            REFERENCES fa_max_booking_gates(gate_id) ON DELETE SET NULL;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_bookings_gate
        ON fa_max_bookings (gate_id)
        WHERE gate_id IS NOT NULL;
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_fa_max_booking_gates: done")


if __name__ == "__main__":
    main()
