"""R1 0.2: the lending_caller Postgres role cannot read document tables.

Everything (schema, role, grants) is created inside closing_db's transaction and rolled back: roles are
transactional in Postgres, so nothing is left in the shared database.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from migrations.apply_lending_caller_db_role import apply_to
from src.lending.security import rbac


def test_caller_role_cannot_read_document_tables(caller_role_db):
    db, role = caller_role_db
    with pytest.raises(DBAPIError) as caught:
        with db.begin_nested():
            rbac.assume_caller_db_role(db, role)
            db.execute(text("SELECT count(*) FROM lending.doc_chase_uploads"))
    assert rbac.pg_error_code(caught.value) == rbac.INSUFFICIENT_PRIVILEGE


def test_caller_role_can_read_status_tables(caller_role_db):
    db, role = caller_role_db
    nested = db.begin_nested()
    rbac.assume_caller_db_role(db, role)
    assert db.execute(text("SELECT count(*) FROM lending.doc_chase_items")).scalar() == 0
    assert db.execute(text("SELECT current_user")).scalar() == role
    nested.rollback()


def test_caller_role_cannot_write(caller_role_db):
    db, role = caller_role_db
    with pytest.raises(DBAPIError):
        with db.begin_nested():
            rbac.assume_caller_db_role(db, role)
            db.execute(text("DELETE FROM lending.doc_chase_items"))


def test_apply_is_idempotent(caller_role_db):
    db, role = caller_role_db
    with db.begin_nested():
        apply_to(db.connection(), schema=db.info["schema"], role=role,
                 app_role=db.execute(text("SELECT session_user")).scalar())


def test_missing_role_raises_unavailable(closing_db):
    with pytest.raises(rbac.CallerDbRoleUnavailable):
        rbac.assume_caller_db_role(closing_db, "lending_caller_absent_role_x")
    assert closing_db.execute(text("SELECT 1")).scalar() == 1  # the outer transaction is still usable
