"""Provision lending_app — the Postgres role the lending service connects as.

Least privilege: full rights on the ``lending`` schema only, read access to the
few FA tables lending consults, and INSERT on the two FA opt-out tables (a
dialer opt-out must also block SMS and email). Nothing else in ``public``.

MUST be run by a Postgres superuser (or a role with CREATEROLE that can GRANT
on these objects); the app's normal role cannot. Pass that role's DSN with
--admin-url. Run it after migrations/apply_lending_compliance.py and
apply_lending_call_dispositions.py so the tables exist to be granted.

Requires LENDING_DB_PASSWORD; pair it with LENDING_DATABASE_URL in .env.
Idempotent: the role is created only if absent; grants are re-runnable.

    PYTHONPATH=. python migrations/apply_lending_app_role.py --admin-url postgresql://admin:...@host/db
    PYTHONPATH=. python migrations/apply_lending_app_role.py --admin-url ... --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROLE = "lending_app"
LENDING_SCHEMA = "lending"
FA_READ_TABLES = ("dnc_phone_checks", "sms_opt_outs", "email_opt_outs", "subscribers", "dbpr_contacts")
FA_INSERT_TABLES = ("sms_opt_outs", "email_opt_outs")


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def statements(database: str, owner: str) -> list[str]:
    """Grant statements. ``owner`` is the role that creates lending tables (the app migration role)."""
    out = [
        f"GRANT CONNECT ON DATABASE {_q(database)} TO {ROLE}",
        f"CREATE SCHEMA IF NOT EXISTS {LENDING_SCHEMA} AUTHORIZATION {_q(owner)}",
        f"GRANT USAGE, CREATE ON SCHEMA {LENDING_SCHEMA} TO {ROLE}",
        f"GRANT ALL ON ALL TABLES IN SCHEMA {LENDING_SCHEMA} TO {ROLE}",
        f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {LENDING_SCHEMA} TO {ROLE}",
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {_q(owner)} IN SCHEMA {LENDING_SCHEMA} GRANT ALL ON TABLES TO {ROLE}",
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {_q(owner)} IN SCHEMA {LENDING_SCHEMA} GRANT ALL ON SEQUENCES TO {ROLE}",
        f"GRANT USAGE ON SCHEMA public TO {ROLE}",
    ]
    out += [f"GRANT SELECT ON public.{t} TO {ROLE}" for t in FA_READ_TABLES]
    out += [f"GRANT INSERT ON public.{t} TO {ROLE}" for t in FA_INSERT_TABLES]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--admin-url", required=True, help="DSN of a superuser / CREATEROLE role")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    owner = make_url(get_settings().database_url).username
    engine = create_engine(args.admin_url)
    with engine.begin() as conn:
        database = conn.execute(text("SELECT current_database()")).scalar()
        stmts = statements(database, owner)
        exists = conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": ROLE}).scalar()
        if args.dry_run:
            print(f"dry-run: database={database!r} owner={owner!r} role {'exists' if exists else 'would be created'}")
            print("\n".join(stmts))
            return 0

        password = get_settings().lending_db_password
        if not exists:
            if not password:
                print("ERROR: LENDING_DB_PASSWORD is not set; refusing to create the role.", file=sys.stderr)
                return 1
            create = conn.execute(
                text("SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', CAST(:r AS text), CAST(:p AS text))"),
                {"r": ROLE, "p": password.get_secret_value()},
            ).scalar()
            conn.execute(text(create))
        for stmt in stmts:
            conn.execute(text(stmt))
        for table in FA_INSERT_TABLES:
            seq = conn.execute(
                text("SELECT pg_get_serial_sequence(:t, 'id')"), {"t": f"public.{table}"}
            ).scalar()
            if seq:
                conn.execute(text(f"GRANT USAGE ON SEQUENCE {seq} TO {ROLE}"))
    logger.info("%s ready on database %s (owner of lending objects: %s).", ROLE, database, owner)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
