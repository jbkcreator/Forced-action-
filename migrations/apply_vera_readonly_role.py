"""
Provision vera_readonly — the read-only Postgres role Vera (the Agent Lane's
truth/verification agent) connects as for every check she runs (VERA-v2.2
sub-task V1).

The role is granted SELECT on every table/sequence in the public schema and
nothing else — no INSERT/UPDATE/DELETE/TRUNCATE, no DDL. This is the
database-level enforcement of Vera's immutable "permanently read-only" core
(Agent Lane v2.2 Part 2): even a bug in Vera's own code cannot write business
data, because the role itself structurally cannot. She writes exactly one
thing (rows in vera_facts, via the normal app role — see
migrations/apply_vera_facts.py), never through this connection.

Database name is derived from the live connection (SELECT current_database())
rather than hard-coded, so this migration is portable across dev/staging/prod
without editing the script.

Caveats (documented, not fixed here — inherent to Postgres role/privilege
model):
  - Requires the migration to run as a role with CREATEROLE (or superuser).
    The app's normal runtime role may not have this — run as the DB
    admin/owner if it errors on CREATE ROLE.
  - ALTER DEFAULT PRIVILEGES only covers objects later created BY THE ROLE
    THAT RUNS THIS STATEMENT. Run this migration as the same role that owns/
    creates the platform's tables (the app/migration role), or tables
    created after this runs won't be auto-granted to vera_readonly — the
    fix at that point is re-running `GRANT SELECT ON ALL TABLES ...`.

Idempotent: CREATE ROLE only if absent; GRANT / ALTER DEFAULT PRIVILEGES are
naturally re-runnable.

Requires VERA_DB_PASSWORD in the environment (the password vera_readonly logs
in with — pair it with VERA_DATABASE_URL in .env once provisioned). Refuses
to run (non-dry-run) if unset.

    PYTHONPATH=. python migrations/apply_vera_readonly_role.py
    PYTHONPATH=. python migrations/apply_vera_readonly_role.py --dry-run
"""
import argparse
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from config.settings import get_settings
from src.core.database import get_db_context

ROLE_NAME = "vera_readonly"

CREATE_ROLE_SQL = """
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vera_readonly') THEN
      CREATE ROLE vera_readonly LOGIN PASSWORD :vera_pw;
   END IF;
END $$;
"""

# {db} is the identifier derived from current_database(), quoted; never
# user-supplied, so simple identifier-quoting is safe here.
GRANT_STATEMENTS_TEMPLATE = [
    'GRANT CONNECT ON DATABASE {db} TO vera_readonly',
    "GRANT USAGE ON SCHEMA public TO vera_readonly",
    "GRANT SELECT ON ALL TABLES IN SCHEMA public TO vera_readonly",
    "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO vera_readonly",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO vera_readonly",
]

# Belt-and-suspenders: force read-only at the session level for this role,
# so even a role-owner mistake later (an accidental extra GRANT) can't turn
# into a write without an explicit ALTER ROLE reverting this.
FORCE_READONLY_SQL = "ALTER ROLE vera_readonly SET default_transaction_read_only = on"

ROLE_EXISTS_SQL = "SELECT 1 FROM pg_roles WHERE rolname = 'vera_readonly'"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with get_db_context() as db:
        current_db = db.execute(text("SELECT current_database()")).scalar()
        db_ident = _quote_ident(current_db)

        if args.dry_run:
            exists = db.execute(text(ROLE_EXISTS_SQL)).scalar()
            print(f"dry-run: database={current_db!r}; role vera_readonly "
                  f"{'already exists' if exists else 'would be created'}; "
                  f"would grant SELECT-only on all tables/sequences in schema public "
                  f"+ default-privilege future grants + force read-only session default")
            return 0

        vera_pw_secret = get_settings().vera_db_password
        vera_pw = vera_pw_secret.get_secret_value() if vera_pw_secret else None
        if not vera_pw:
            print("ERROR: VERA_DB_PASSWORD not set in environment — refusing to "
                  "create/alter the role without an explicit password.", file=sys.stderr)
            return 1

        db.execute(text(CREATE_ROLE_SQL), {"vera_pw": vera_pw})
        for stmt in GRANT_STATEMENTS_TEMPLATE:
            db.execute(text(stmt.format(db=db_ident)))
        db.execute(text(FORCE_READONLY_SQL))
        db.commit()
        print(f"vera_readonly role ready on database={current_db!r}: "
              f"SELECT-only on all tables/sequences in schema public, "
              f"future tables auto-granted, no write privileges anywhere.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
