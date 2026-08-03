"""
CLONE-v2.2 — widen lifecycle_playbook to a fleet-wide playbook/anti-playbook store.

Adds two columns to the existing `lifecycle_playbook` table (fa036) so it can
hold entries authored by any agent/domain (Vera/Cora/Hunter/fleet-wide), not
just Lifecycle, and either polarity (playbook or anti-playbook) per the fleet
constitutions' "playbooks at 3+ proofs; anti-playbooks at 3+ failures;
inherited at birth" rule (docs/constitutions/*.md). This is a widening, not a
replacement — no existing column, index, or constraint is touched, and every
pre-existing row is backfilled to agent_domain='lifecycle',
entry_kind='playbook' so nothing already written changes meaning.

NOT applied by this agent — flagged for manual review since this is a shared
Postgres used by dev/test/prod (single-shared-db). Run once:

    PYTHONPATH=. python migrations/apply_lifecycle_playbook_fleet_widen.py

Idempotent (ADD COLUMN IF NOT EXISTS / index IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS agent_domain VARCHAR(40) NOT NULL DEFAULT 'lifecycle'",
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS entry_kind VARCHAR(20) NOT NULL DEFAULT 'playbook'",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'check_lifecycle_playbook_entry_kind'
        ) THEN
            ALTER TABLE lifecycle_playbook
                ADD CONSTRAINT check_lifecycle_playbook_entry_kind
                CHECK (entry_kind IN ('playbook', 'anti_playbook'));
        END IF;
    END $$;
    """,
    "CREATE INDEX IF NOT EXISTS idx_lifecycle_playbook_agent_domain_kind ON lifecycle_playbook (agent_domain, entry_kind, status)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'lifecycle_playbook'
            ORDER BY ordinal_position
        """)).fetchall()
    print("lifecycle_playbook columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
