"""
Provision vera_promises — the mutable store of open commitments Vera tracks
(Constitution standing job #3, VERA-v2.2 sub-task V4).

Unlike vera_facts (append-only), a promise's status flips open ->
closed/cancelled in place. Vera writes here via the normal app role
(src/agents/vera/promises.py:record_promise); vera_readonly only reads.

vera_readonly's ALTER DEFAULT PRIVILEGES (apply_vera_readonly_role.py) only
auto-grants tables created by the role that ran THAT migration — this script
may run as a different role, so SELECT is granted explicitly below. Idempotent
and re-runnable.

    PYTHONPATH=. python migrations/apply_vera_promises.py
    PYTHONPATH=. python migrations/apply_vera_promises.py --dry-run
"""
import argparse
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vera_promises (
    id                BIGSERIAL     PRIMARY KEY,
    thread_id         VARCHAR(64),
    description       TEXT          NOT NULL,
    owner             VARCHAR(120)  NOT NULL,
    source            VARCHAR(60)   NOT NULL,
    mrr_at_risk_cents BIGINT,
    status            VARCHAR(20)   NOT NULL DEFAULT 'open',
    due_at            TIMESTAMPTZ,
    observed_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    closed_at         TIMESTAMPTZ,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now()
)
"""

CREATE_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS ix_vera_promises_status_due "
    "ON vera_promises (status, due_at)"
)

GRANT_READONLY_SQL = """
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vera_readonly') THEN
      GRANT SELECT ON vera_promises TO vera_readonly;
   END IF;
END $$;
"""

TABLE_EXISTS_SQL = (
    "SELECT 1 FROM information_schema.tables WHERE table_name = 'vera_promises'"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with get_db_context() as db:
        if args.dry_run:
            exists = db.execute(text(TABLE_EXISTS_SQL)).scalar()
            print(
                f"dry-run: table vera_promises "
                f"{'already exists' if exists else 'would be created'}; "
                f"would create ix_vera_promises_status_due + GRANT SELECT to "
                f"vera_readonly (if role present)"
            )
            return 0

        db.execute(text(CREATE_TABLE_SQL))
        db.execute(text(CREATE_INDEX_SQL))
        db.execute(text(GRANT_READONLY_SQL))
        db.commit()
        print(
            "vera_promises ready: table + ix_vera_promises_status_due + "
            "SELECT granted to vera_readonly (if present)."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
