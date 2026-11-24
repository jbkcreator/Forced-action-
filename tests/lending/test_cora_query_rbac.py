"""R1 0.2: Cora's free-form SQL. A non-admin's lookup runs as lending_caller; storage locations never reach Slack."""
from __future__ import annotations

from contextlib import contextmanager

import pytest

from config.settings import get_settings
from packages.agent_core.tools import ToolContext, ToolInputError
from src.lending import cora_tools
from src.lending.cora_tools import CoraToolkit, QueryInput, check_read_only_sql
from src.lending.security import rbac

JOSH = ToolContext(user_id="U_JOSH", is_approver=True, channel="C", thread_ts="1.0", tool_use_id="toolu_1")
DAVID = ToolContext(user_id="U_DAVID", is_approver=False, channel="C", thread_ts="1.0", tool_use_id="toolu_2")


@pytest.fixture(autouse=True)
def josh_is_admin(monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_cora_approver_user_ids", "U_JOSH", raising=False)


def _spy_toolkit(monkeypatch, session, roles):
    toolkit = CoraToolkit(engine=None, memory=None)  # type: ignore[arg-type]

    @contextmanager
    def read_only(db_role=None):
        roles.append(db_role)
        if db_role is not None:
            rbac.assume_caller_db_role(session, db_role)
        yield session

    monkeypatch.setattr(toolkit, "_read_only", read_only)
    monkeypatch.setattr(cora_tools, "relations_outside_schema", lambda plan: set())  # throwaway schema, not lending
    return toolkit


def test_query_refuses_sql_naming_storage_columns():
    for sql in ("SELECT stored_path AS p FROM lending.doc_chase_uploads",
                "SELECT token_hash FROM lending.staff_api_tokens",
                "SELECT u.STORED_PATH FROM lending.doc_chase_uploads u"):
        with pytest.raises(ToolInputError, match="file locations"):
            check_read_only_sql(sql)


def test_caller_query_runs_under_caller_db_role(monkeypatch):
    roles = []
    toolkit = CoraToolkit(engine=None, memory=None)  # type: ignore[arg-type]

    @contextmanager
    def read_only(db_role=None):
        roles.append(db_role)
        raise ToolInputError("stop here")
        yield

    monkeypatch.setattr(toolkit, "_read_only", read_only)
    for context in (DAVID, JOSH):
        with pytest.raises(ToolInputError):
            toolkit.query_lending_data(QueryInput(sql="SELECT 1 AS x"), context)
    assert roles == [rbac.CALLER_DB_ROLE, None]


def test_caller_query_fails_closed_when_role_missing(documents_db, monkeypatch):
    roles = []
    toolkit = _spy_toolkit(monkeypatch, documents_db.db, roles)
    monkeypatch.setattr(rbac, "CALLER_DB_ROLE", "lending_caller_absent_role_x")
    monkeypatch.setattr(cora_tools, "CALLER_DB_ROLE", "lending_caller_absent_role_x")
    with pytest.raises(ToolInputError, match="not set up"):
        toolkit.query_lending_data(QueryInput(sql="SELECT 1 AS x"), DAVID)


def test_admin_select_star_blanks_storage_columns(documents_db, monkeypatch):
    documents_db.add(1, "Mark Smith", "+18135550101")
    toolkit = _spy_toolkit(monkeypatch, documents_db.db, [])
    result = toolkit.query_lending_data(QueryInput(sql="SELECT * FROM lending.doc_chase_uploads"), JOSH)
    (row,) = result["rows"]
    assert row["stored_path"] == cora_tools.STORAGE_HIDDEN and str(documents_db.tmp_path) not in str(result)
    assert row["item_code"] == "bank_statements"


def test_caller_reading_uploads_is_refused_by_postgres(caller_role_db, monkeypatch):
    db, role = caller_role_db
    toolkit = _spy_toolkit(monkeypatch, db, [])
    monkeypatch.setattr(cora_tools, "CALLER_DB_ROLE", role)
    with db.begin_nested() as nested:
        with pytest.raises(ToolInputError, match="only an admin"):
            toolkit.query_lending_data(QueryInput(sql="SELECT count(*) FROM lending.doc_chase_uploads"), DAVID)
        nested.rollback()
