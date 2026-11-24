"""R1 0.2: the ``lending_caller`` Postgres role, the database layer of the Backflip Wall.

A NOLOGIN role that may read every ``lending`` table except the ones holding borrower documents
(``src.lending.security.rbac.DOCUMENT_TABLES``). The app's login is made a member, so a caller's work runs as it
with ``SET LOCAL ROLE lending_caller`` (the staff API for a caller token, Cora's free-form SQL for a non-approver):
even a missed check in code cannot read a document table.

Run it as a Postgres SUPERUSER (or as the app's login when it has CREATEROLE and owns the lending tables), naming
the app's login, after apply_lending_document_access.py. A non-owner's GRANT/REVOKE only warns and changes nothing,
so the script ends by checking the result with has_table_privilege and fails loudly if a caller could still read a
document table (including through a PUBLIC grant) or could not read a status table:

    PYTHONPATH=. python migrations/apply_lending_caller_db_role.py --app-role <DATABASE_URL user> [--dry-run]

Idempotent: the role is created only if absent; GRANT / REVOKE / ALTER DEFAULT PRIVILEGES are re-runnable. Like the
Vera role, new lending tables are granted to the role automatically (default privileges for the app login).
"""
from __future__ import annotations

import argparse
import sys

sys.path.insert(0, ".")

from sqlalchemy import text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import DBAPIError

from src.lending.security.rbac import CALLER_DB_ROLE, DOCUMENT_TABLES, INSUFFICIENT_PRIVILEGE, pg_error_code

LENDING_SCHEMA = "lending"


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def statements(conn: Connection, *, schema: str, role: str, app_role: str) -> list[str]:
    """Every statement, in order. Only document tables that exist are revoked."""
    existing = [t for t in DOCUMENT_TABLES
                if conn.execute(text("SELECT to_regclass(:name)"), {"name": f"{_q(schema)}.{_q(t)}"}).scalar()]
    create = (f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role.replace(chr(39), '')}') "
              f"THEN CREATE ROLE {_q(role)} NOLOGIN; END IF; END $$")
    return [
        create,
        f"GRANT USAGE ON SCHEMA {_q(schema)} TO {_q(role)}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {_q(schema)} TO {_q(role)}",
        *[f"REVOKE ALL ON {_q(schema)}.{_q(t)} FROM {_q(role)}" for t in existing],
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {_q(app_role)} IN SCHEMA {_q(schema)} GRANT SELECT ON TABLES TO {_q(role)}",
        f"GRANT {_q(role)} TO {_q(app_role)}",
    ]


STATUS_TABLE = "doc_chase_items"  # a caller must still read document status


class CallerRoleNotEffective(RuntimeError):
    """The grants ran but the role's privileges are not what the wall needs."""


def verify(conn: Connection, *, schema: str = LENDING_SCHEMA, role: str = CALLER_DB_ROLE) -> None:
    def can_read(table: str) -> bool:
        return bool(conn.execute(text("SELECT has_table_privilege(:r, :t, 'SELECT')"),
                                 {"r": role, "t": f"{_q(schema)}.{_q(table)}"}).scalar())

    def exists(table: str) -> bool:
        return conn.execute(text("SELECT to_regclass(:t)"), {"t": f"{_q(schema)}.{_q(table)}"}).scalar() is not None

    open_tables = [t for t in DOCUMENT_TABLES if exists(t) and can_read(t)]
    if open_tables:
        raise CallerRoleNotEffective(f"{role} can still read {', '.join(open_tables)} (a PUBLIC grant, or the "
                                     "script did not run as the tables' owner or a superuser)")
    if exists(STATUS_TABLE) and not can_read(STATUS_TABLE):
        raise CallerRoleNotEffective(f"{role} cannot read {STATUS_TABLE}: the grants did not take effect")


def apply_to(conn: Connection, *, schema: str = LENDING_SCHEMA, role: str = CALLER_DB_ROLE, app_role: str) -> None:
    for statement in statements(conn, schema=schema, role=role, app_role=app_role):
        conn.execute(text(statement))
    verify(conn, schema=schema, role=role)


def main() -> int:
    from config.settings import get_settings
    from src.core.database import get_db_context

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--app-role", default=make_url(str(get_settings().database_url)).username,
                        help="the login the lending apps connect as (default: the DATABASE_URL user)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.app_role:
        print("ERROR: no --app-role and DATABASE_URL has no user", file=sys.stderr)
        return 1
    with get_db_context() as db:
        conn = db.connection()
        if args.dry_run:
            for statement in statements(conn, schema=LENDING_SCHEMA, role=CALLER_DB_ROLE, app_role=args.app_role):
                print(statement)
            return 0
        try:
            apply_to(conn, app_role=args.app_role)
            db.commit()
        except CallerRoleNotEffective as exc:
            db.rollback()
            print(f"ERROR: {exc}; nothing was committed", file=sys.stderr)
            return 1
        except DBAPIError as exc:
            db.rollback()
            if pg_error_code(exc) == INSUFFICIENT_PRIVILEGE:
                print("ERROR: run this as a Postgres superuser: --app-role <the app's login>", file=sys.stderr)
                return 1
            raise
    print(f"{CALLER_DB_ROLE} ready: reads schema {LENDING_SCHEMA} except {', '.join(DOCUMENT_TABLES)}; "
          f"{args.app_role} may SET ROLE to it")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
