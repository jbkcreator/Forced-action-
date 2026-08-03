"""
Add dbpr_contacts.tracerfy_queue_id — tracks an in-flight Tracerfy batch
submission so a crashed/interrupted enrichment run can resume polling an
already-paid-for queue instead of resubmitting (and double-billing) it.

Also extends check_dbpr_enrichment_status to allow the new intermediate
'tracerfy_submitted' status (discovered live — the existing constraint only
allowed pending/enriched/failed/skipped and rejected writes without this).

    PYTHONPATH=. python migrations/apply_dbpr_tracerfy_queue_id.py

Idempotent: ADD COLUMN IF NOT EXISTS; the constraint is dropped and
recreated unconditionally each run (Postgres has no ADD CONSTRAINT IF NOT
EXISTS equivalent for CHECK), which is a no-op if it already matches.

The CHECK constraint list below is kept in sync with the superset also
written by apply_dbpr_tracerfy_address_fallback.py (which adds
'awaiting_address_only'). These two scripts run alphabetically
(address_fallback before queue_id) and each unconditionally drops +
recreates the same constraint, so whichever runs last wins — if this file
only listed its own statuses, re-running it after address_fallback would
silently drop 'awaiting_address_only' support and break any batch that
persists that status. Both scripts must list the full status set so the
end state is the same regardless of run order.
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context


def main() -> int:
    with get_db_context() as db:
        db.execute(text("""
            ALTER TABLE dbpr_contacts
            ADD COLUMN IF NOT EXISTS tracerfy_queue_id VARCHAR(50)
        """))
        db.execute(text("""
            ALTER TABLE dbpr_contacts
            DROP CONSTRAINT IF EXISTS check_dbpr_enrichment_status
        """))
        db.execute(text("""
            ALTER TABLE dbpr_contacts
            ADD CONSTRAINT check_dbpr_enrichment_status
            CHECK (enrichment_status IN (
                'pending', 'enriched', 'failed', 'skipped',
                'tracerfy_submitted', 'awaiting_address_only'
            ))
        """))
        db.commit()
        print("apply_dbpr_tracerfy_queue_id: column ensured, enrichment_status constraint extended")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
