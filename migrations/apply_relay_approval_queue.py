"""
Provision relay_approval_queue — Josh's approval queue for RELAY-executed
outreach (RELAY-v2.2 sub-task R1).

One row per proposed outreach action. Cora (Phase 2, not yet built) will
write 'pending' rows here; R1 seeds rows directly (via
`python -m src.services.relay --seed`) to build/prove the engine now —
identical schema, zero change when Cora lands. A pending row is posted to
Slack as an interactive approve/reject message; the button press (a
signature-verified webhook) flips status to 'approved'/'rejected'. The
Relay cron sweep (src/services/relay/sweep.py) then reads 'approved' rows
as a batch and executes them via src/services/relay/engine.py.

idempotency_key is UNIQUE — the no-double-send guarantee. Written via the
normal app DB role; vera_readonly (if present) is granted SELECT only, so
Vera can audit approved-vs-sent state without any write access.

    PYTHONPATH=. python migrations/apply_relay_approval_queue.py
    PYTHONPATH=. python migrations/apply_relay_approval_queue.py --dry-run
"""
import argparse
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS relay_approval_queue (
    id                BIGSERIAL     PRIMARY KEY,
    idempotency_key   VARCHAR(120)  NOT NULL UNIQUE,
    batch_id          VARCHAR(64),
    thread_id         VARCHAR(64),
    channel           VARCHAR(30)   NOT NULL,
    recipient         TEXT          NOT NULL,
    payload           JSONB         NOT NULL,
    status            VARCHAR(20)   NOT NULL DEFAULT 'pending',
    slack_message_ts  VARCHAR(30),
    decided_by        VARCHAR(120),
    decided_at        TIMESTAMPTZ,
    error             TEXT,
    dispatched_at     TIMESTAMPTZ,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT ck_relay_approval_queue_status
        CHECK (status IN ('pending', 'approved', 'rejected', 'sent', 'failed', 'skipped'))
)
"""

CREATE_INDEX_STATUS_SQL = (
    "CREATE INDEX IF NOT EXISTS ix_relay_approval_queue_status "
    "ON relay_approval_queue (status)"
)

CREATE_INDEX_BATCH_STATUS_SQL = (
    "CREATE INDEX IF NOT EXISTS ix_relay_approval_queue_batch_status "
    "ON relay_approval_queue (batch_id, status)"
)

GRANT_READONLY_SQL = """
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vera_readonly') THEN
      GRANT SELECT ON relay_approval_queue TO vera_readonly;
   END IF;
END $$;
"""

TABLE_EXISTS_SQL = (
    "SELECT 1 FROM information_schema.tables WHERE table_name = 'relay_approval_queue'"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with get_db_context() as db:
        if args.dry_run:
            exists = db.execute(text(TABLE_EXISTS_SQL)).scalar()
            print(
                f"dry-run: table relay_approval_queue "
                f"{'already exists' if exists else 'would be created'}; "
                f"would create ix_relay_approval_queue_status + "
                f"ix_relay_approval_queue_batch_status + GRANT SELECT to "
                f"vera_readonly (if role present)"
            )
            return 0

        db.execute(text(CREATE_TABLE_SQL))
        db.execute(text(CREATE_INDEX_STATUS_SQL))
        db.execute(text(CREATE_INDEX_BATCH_STATUS_SQL))
        db.execute(text(GRANT_READONLY_SQL))
        db.commit()
        print(
            "relay_approval_queue ready: table + ix_relay_approval_queue_status + "
            "ix_relay_approval_queue_batch_status + SELECT granted to "
            "vera_readonly (if present)."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
