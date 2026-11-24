"""R1 0.2: personal tokens for the staff document API (``Authorization: Bearer ndl_...``).

Only the SHA-256 of a token is stored. The raw token is printed once by ``--issue``; a lost token is revoked and a
new one issued. A token is valid for 90 days (``config.lending_closing.STAFF_TOKEN_TTL``) unless revoked earlier.

    python -m src.lending.security.staff_tokens --issue "David" --role caller --by sarath
    python -m src.lending.security.staff_tokens --list
    python -m src.lending.security.staff_tokens --revoke 3
"""
from __future__ import annotations

import argparse
import secrets
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_closing import STAFF_TOKEN_TTL
from src.lending.partners import hash_token
from src.lending.security.rbac import Role

TOKEN_PREFIX = "ndl_"
TOKEN_BYTES = 32

_VALID = ("revoked_at IS NULL AND expires_at > :now")
_BY_HASH = text(f"SELECT id, name, role FROM lending.staff_api_tokens WHERE token_hash = :hash AND {_VALID}")
_BY_ID = text(f"SELECT id, name, role FROM lending.staff_api_tokens WHERE id = :id AND {_VALID}")
_INSERT = text("INSERT INTO lending.staff_api_tokens (name, role, token_hash, created_by, created_at, expires_at) "
               "VALUES (:name, :role, :hash, :by, :now, :expires)")
_LIST = text("SELECT id, name, role, created_by, created_at, expires_at, revoked_at FROM lending.staff_api_tokens "
             "ORDER BY id")
_REVOKE = text("UPDATE lending.staff_api_tokens SET revoked_at = :now WHERE id = :id AND revoked_at IS NULL")


@dataclass(frozen=True)
class StaffToken:
    id: int
    name: str
    role: Role


def _token(row: Any) -> Optional[StaffToken]:
    return StaffToken(row.id, row.name, Role(row.role)) if row is not None else None


def issue_token(db, name: str, role: Role, created_by: str, now: datetime) -> str:
    """Store a new token's hash; returns the raw token, which exists nowhere else."""
    raw = TOKEN_PREFIX + secrets.token_urlsafe(TOKEN_BYTES)
    db.execute(_INSERT, {"name": name.strip()[:80], "role": role.value, "hash": hash_token(raw),
                         "by": created_by.strip()[:80], "now": now, "expires": now + STAFF_TOKEN_TTL})
    return raw


def token_for(db, raw: str, now: datetime) -> Optional[StaffToken]:
    """The token behind a raw value, or None when it is unknown, revoked or expired."""
    if not raw.startswith(TOKEN_PREFIX):
        return None
    return _token(db.execute(_BY_HASH, {"hash": hash_token(raw), "now": now}).first())


def token_by_id(db, token_id: int, now: datetime) -> Optional[StaffToken]:
    return _token(db.execute(_BY_ID, {"id": token_id, "now": now}).first())


def list_tokens(db) -> list[dict[str, Any]]:
    """Every token without its hash."""
    return [dict(row) for row in db.execute(_LIST).mappings()]


def revoke_token(db, token_id: int, now: datetime) -> bool:
    return db.execute(_REVOKE, {"id": token_id, "now": now}).rowcount == 1


def main(argv: Optional[list[str]] = None) -> int:
    from src.lending.db import lending_session

    parser = argparse.ArgumentParser(description="Staff document API tokens")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--issue", metavar="NAME")
    action.add_argument("--list", action="store_true")
    action.add_argument("--revoke", metavar="ID", type=int)
    parser.add_argument("--role", choices=[role.value for role in Role])
    parser.add_argument("--by", help="who is issuing it (recorded)")
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc)
    if args.issue and not (args.role and args.by):
        parser.error("--issue needs --role and --by")
    with lending_session() as db:
        if args.issue:
            raw = issue_token(db, args.issue, Role(args.role), args.by, now)
            sys.stdout.write(f"{raw}\nStore it now; it is not shown again. Expires {(now + STAFF_TOKEN_TTL):%Y-%m-%d}.\n")
        elif args.list:
            for row in list_tokens(db):
                state = "revoked" if row["revoked_at"] else ("expired" if row["expires_at"] <= now else "active")
                sys.stdout.write(f"{row['id']}\t{row['name']}\t{row['role']}\t{state}\t"
                                 f"expires {row['expires_at']:%Y-%m-%d}\tissued by {row['created_by']}\n")
        elif not revoke_token(db, args.revoke, now):
            sys.stdout.write(f"no active token {args.revoke}\n")
            return 1
        else:
            sys.stdout.write(f"token {args.revoke} revoked\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
