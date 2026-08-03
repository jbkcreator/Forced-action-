"""
Add dbpr_contacts.tracerfy_mode + extend check_dbpr_enrichment_status with
'awaiting_address_only' — supports the address-only fallback retry: a
normal-mode (name+address) Tracerfy miss becomes 'awaiting_address_only'
instead of terminal 'failed', so a later run can retry it address-only
before giving up. tracerfy_mode ('normal'/'advanced') is persisted
alongside tracerfy_queue_id so a resumed/crashed run knows which mode's
miss-handling applies on resolution.

Also widens enrichment_status from VARCHAR(20) to VARCHAR(30) — discovered
live: 'awaiting_address_only' is 21 characters, one over the original
column limit.

    PYTHONPATH=. python migrations/apply_dbpr_tracerfy_address_fallback.py

Idempotent: ADD COLUMN IF NOT EXISTS / ALTER COLUMN TYPE (widening is
always safe, re-running is a no-op); the constraint is dropped and
recreated unconditionally each run (Postgres has no ADD CONSTRAINT IF NOT
EXISTS equivalent for CHECK), which is a no-op if it already matches.
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
            ADD COLUMN IF NOT EXISTS tracerfy_mode VARCHAR(10)
        """))
        db.execute(text("""
            ALTER TABLE dbpr_contacts
            ALTER COLUMN enrichment_status TYPE VARCHAR(30)
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
        print("apply_dbpr_tracerfy_address_fallback: column ensured, enrichment_status constraint extended")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
