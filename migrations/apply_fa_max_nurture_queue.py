"""
WP-GL-5: Lending nurture queue.

Holds gate-fail records pending routing to the real nurture destination.
The nurture destination (GHL pipeline, Slack channel, etc.) is an open
question (Q6 in task-analysis). Until it is answered, failed-gate records
are stored here and surfaced to EXCEPTIONS so nothing is silently dropped.

OPEN: once Q6 is answered, add a `routed_to` column and a worker/cron
that drains this queue into the real destination. See:
  src/services/calendar/nurture.py — NurtureRouter (stub)

Idempotent: safe to re-run. Run after apply_fa_max_booking_gates.py.

Run:
  PYTHONPATH=. python migrations/apply_fa_max_nurture_queue.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS fa_max_nurture_queue (
        id              BIGSERIAL   PRIMARY KEY,
        gate_id         TEXT        NOT NULL REFERENCES fa_max_booking_gates(gate_id),
        tracked_link_id BIGINT      REFERENCES tracked_links(id) ON DELETE SET NULL,
        person_id       BIGINT,
        failed_field    VARCHAR(60),
        fail_reason     VARCHAR(60),
        list_key        VARCHAR(80),
        -- OPEN (Q6): destination is unknown; status starts 'pending_routing'.
        -- Once routing is live, status transitions to 'routed' or 'error'.
        status          VARCHAR(30) NOT NULL DEFAULT 'pending_routing',
        enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        routed_at       TIMESTAMPTZ,
        CONSTRAINT ck_fa_max_nurture_queue_status
            CHECK (status IN ('pending_routing', 'routed', 'error'))
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_nurture_queue_status
        ON fa_max_nurture_queue (status, enqueued_at)
        WHERE status = 'pending_routing';
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fa_max_nurture_queue_gate
        ON fa_max_nurture_queue (gate_id);
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_fa_max_nurture_queue: done")


if __name__ == "__main__":
    main()
