"""
Create vera_facts — the Agent Lane's facts store (VERA-v2.2 sub-task V1).

Vera (the read-only truth/verification agent) writes one dated, sourced,
freshness-classed row per verified fact here. Append-only: a fact is never
updated in place, only re-verified with a new row (fact_key, observed_at DESC
gives the current value). Freshness is computed at read time from
freshness_class + observed_at, not by a background expiry job.

Written through the normal app DB role (this migration's role), never through
vera_readonly — see migrations/apply_vera_readonly_role.py, which grants that
role SELECT only, on every table including this one.

Idempotent: CREATE TABLE ... IF NOT EXISTS, CREATE INDEX ... IF NOT EXISTS.

    PYTHONPATH=. python migrations/apply_vera_facts.py
    PYTHONPATH=. python migrations/apply_vera_facts.py --dry-run
"""
import argparse
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS vera_facts (
        id              BIGSERIAL     PRIMARY KEY,
        fact_key        VARCHAR(120)  NOT NULL,
        fact_value      TEXT          NOT NULL,
        value_numeric   NUMERIC       NULL,
        county_id       VARCHAR(50)   NULL,
        source          VARCHAR(60)   NOT NULL,
        method          TEXT          NOT NULL,
        freshness_class VARCHAR(20)   NOT NULL,
        confidence      SMALLINT      NULL,
        observed_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
        created_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_vera_facts_key_observed ON vera_facts (fact_key, observed_at DESC)",
    "CREATE INDEX IF NOT EXISTS ix_vera_facts_county ON vera_facts (county_id) WHERE county_id IS NOT NULL",
]

COUNT_SQL = "SELECT count(*) FROM information_schema.tables WHERE table_name = 'vera_facts'"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with get_db_context() as db:
        if args.dry_run:
            exists = db.execute(text(COUNT_SQL)).scalar()
            print(f"dry-run: would create vera_facts table + 2 indexes "
                  f"(currently {'exists' if exists else 'absent'})")
            return 0

        for stmt in DDL_STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        total = db.execute(text("SELECT count(*) FROM vera_facts")).scalar()
        print(f"vera_facts ready; total rows={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
