"""
PropertyRadar handoff review fix: closes a duplicate-person race.

Two overlapping runs of the PropertyRadar handoff task (manual re-run
overlapping a scheduled one, or a retry over a slow/hung previous run) could
both decide the same staged record was new and both INSERT INTO
fa_max_persons before either committed — nothing at the DB layer stopped it.
The decisions table's unique index only protects the audit trail; it never
stopped the second person row from being created.

source_reference is deterministic per source (e.g.
"property_radar:<radar_id>" for the PropertyRadar handoff,
selfserve_sessions.py's session token for the selfserve flow), so a partial
unique index on (source, source_reference) makes the second concurrent
INSERT fail with an IntegrityError instead of silently succeeding twice.
lead_handoff.py's SqlHandoffStore.create_lead already wraps the insert in a
savepoint with `except Exception: savepoint.rollback(); raise`, and
run_handoff() already catches that and records the record as
skipped/handoff_error — so this constraint is the only change needed; no
application code changes.

Idempotent: safe to re-run (CREATE UNIQUE INDEX IF NOT EXISTS). Not
CONCURRENTLY — fa_max_persons is ~126 rows today, so the brief ACCESS
EXCLUSIVE lock this takes is effectively instant. Revisit if this table
grows large enough for that lock to matter.

Run:
  PYTHONPATH=. python migrations/apply_fa_max_persons_source_reference_unique.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_fa_max_persons_source_reference
        ON fa_max_persons (source, source_reference)
        WHERE source_reference IS NOT NULL;
"""


def main() -> None:
    with get_db_context() as session:
        session.execute(text(_DDL))
        session.commit()
    print("apply_fa_max_persons_source_reference_unique: done")


if __name__ == "__main__":
    main()
