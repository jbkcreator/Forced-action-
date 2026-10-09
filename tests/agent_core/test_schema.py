"""The PostgreSQL DDL, the lending models and the library's SQL must describe the same tables."""
from __future__ import annotations

import re

import pytest

from packages.agent_core.pending_actions import ActionStatus
from packages.agent_core.schema import ACTION_STATUSES, base_statements, ddl_statements, safeguard_statements
from src.lending.models import LendingAgentHaltState, LendingPendingAction

DDL = "\n".join(ddl_statements("lending"))


def _ddl_columns(table: str) -> set[str]:
    created = re.search(rf'"lending"\.{table} \((.*?)\n\s*\)\s*$', DDL, re.S | re.M).group(1)
    columns = {line.split()[0] for line in created.strip().splitlines()
               if line.strip() and not line.strip().startswith("CONSTRAINT")}
    columns |= set(re.findall(rf'"lending"\.{table} ADD COLUMN IF NOT EXISTS (\w+)', DDL))
    return columns


@pytest.mark.parametrize("model", [LendingAgentHaltState, LendingPendingAction])
def test_model_columns_match_ddl(model) -> None:
    assert {column.name for column in model.__table__.columns} == _ddl_columns(model.__tablename__)


def test_model_indexes_match_ddl() -> None:
    created = set(re.findall(r"CREATE (?:UNIQUE )?INDEX IF NOT EXISTS (\w+) ON \"lending\"\.pending_actions", DDL))
    assert {index.name for index in LendingPendingAction.__table__.indexes} == created


def test_status_vocabulary_matches_everywhere() -> None:
    assert set(ACTION_STATUSES) == {status.value for status in ActionStatus}
    model_check = next(constraint for constraint in LendingPendingAction.__table__.constraints
                       if getattr(constraint, "name", "") == "ck_pending_actions_status")
    assert set(re.findall(r"'(\w+)'", str(model_check.sqltext))) == set(ACTION_STATUSES)


def test_ddl_is_rerunnable_and_schema_qualified() -> None:
    for statement in base_statements("lending"):
        assert "IF NOT EXISTS" in statement
    for statement in safeguard_statements("lending"):
        # A re-added constraint is preceded by its DROP ... IF EXISTS.
        assert "IF NOT EXISTS" in statement or "IF EXISTS" in statement or "ADD CONSTRAINT" in statement
    assert all('"lending".' in statement or "SCHEMA" in statement for statement in ddl_statements("lending"))


def test_constraint_is_dropped_before_it_is_re_added() -> None:
    statements = safeguard_statements("lending")
    drop = next(i for i, s in enumerate(statements) if "DROP CONSTRAINT IF EXISTS ck_pending_actions_status" in s)
    add = next(i for i, s in enumerate(statements) if "ADD CONSTRAINT ck_pending_actions_status" in s)
    assert drop < add
