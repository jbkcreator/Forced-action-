"""R1 0.2 Backflip Wall: who may open a borrower's stored documents.

Two roles. ``admin`` is a Slack user listed in ``LENDING_CORA_APPROVER_USER_IDS`` (Josh) or a staff API token issued
with that role; ``caller`` is everyone else (David, Cora operators, caller tokens). Callers open no document at all:
they see status summaries only.

The wall has two layers. The API / Cora tool checks the role before a link is issued and again when it is opened.
Underneath, a caller's database work runs as the NOLOGIN role ``lending_caller`` (``SET LOCAL ROLE``), which
``migrations/apply_lending_caller_db_role.py`` leaves without any privilege on :data:`DOCUMENT_TABLES`, so a missed
check in code still cannot read them.
"""
from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import text

from config.settings import get_settings
from packages.agent_core.config import parse_user_ids

logger = logging.getLogger(__name__)

# Tables in schema ``lending`` a caller may not read: stored documents and the hashes of the tokens that open or
# upload them. A new such table is added here; its own migration revokes it (rbac.revoke_from_caller_role).
DOCUMENT_TABLES: tuple[str, ...] = ("doc_chase_uploads", "doc_view_links", "partner_file_uploads",
                                    "staff_api_tokens", "doc_chase_links")
CALLER_DB_ROLE = "lending_caller"

SLACK_ISSUER = "slack:"
TOKEN_ISSUER = "token:"


class Role(str, Enum):
    ADMIN = "admin"
    CALLER = "caller"


class DocumentAccessDenied(PermissionError):
    """The person or token asking is not an admin."""


class CallerDbRoleUnavailable(RuntimeError):
    """``SET ROLE lending_caller`` failed: the role is missing or the app login is not a member. Callers are refused."""


def role_for_slack_user(user_id: str) -> Role:
    """Read on every call (no cache): removing someone from the approvers takes their access away at once."""
    approvers = parse_user_ids(get_settings().lending_cora_approver_user_ids)
    return Role.ADMIN if user_id and user_id in approvers else Role.CALLER


def role_of_issuer(db, issued_to: str, now: datetime) -> Role:
    """The current role of whoever a link was issued to: ``slack:<user id>`` or ``token:<staff token id>``."""
    if issued_to.startswith(SLACK_ISSUER):
        return role_for_slack_user(issued_to[len(SLACK_ISSUER):])
    if issued_to.startswith(TOKEN_ISSUER) and issued_to[len(TOKEN_ISSUER):].isdigit():
        from src.lending.security.staff_tokens import token_by_id

        token = token_by_id(db, int(issued_to[len(TOKEN_ISSUER):]), now)
        return token.role if token is not None else Role.CALLER
    return Role.CALLER


INSUFFICIENT_PRIVILEGE = "42501"


def pg_error_code(exc: BaseException) -> Optional[str]:
    """The SQLSTATE of a database error, whichever driver raised it (psycopg 3 ``sqlstate``, psycopg2 ``pgcode``)."""
    orig = getattr(exc, "orig", exc)
    return getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def revoke_from_caller_role(conn, schema: str, tables: tuple[str, ...], role: str = CALLER_DB_ROLE) -> None:
    """For a migration creating a document table: close it to ``role`` when that role already exists (otherwise the
    role migration, run later, closes it)."""
    if conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": role}).first() is None:
        return
    for table in tables:
        conn.execute(text(f"REVOKE ALL ON {_quote_ident(schema)}.{_quote_ident(table)} FROM {_quote_ident(role)}"))


def assume_caller_db_role(session, role: str = CALLER_DB_ROLE) -> None:
    """Run the rest of this transaction as ``role``. Raises :class:`CallerDbRoleUnavailable` when that fails."""
    try:
        with session.begin_nested():
            session.execute(text(f"SET LOCAL ROLE {_quote_ident(role)}"))
    except Exception as exc:  # class only: the message can carry the statement
        logger.error("event=lending_caller_db_role_unavailable role=%s error=%s", role, type(exc).__name__)
        raise CallerDbRoleUnavailable(role) from exc
