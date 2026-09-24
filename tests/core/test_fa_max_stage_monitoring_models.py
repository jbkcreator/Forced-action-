"""tests/core/test_fa_max_stage_monitoring_models.py

Finding 7: CLAUDE.md's schema-change process makes src/core/models.py the
source of truth for tests' create_all. WP-T2-6 shipped the migration without
the two ORM classes. This test keeps them in step by diffing the model's
columns/constraints against the migration script's own DDL text -- no live
Postgres needed, so it runs in the default suite.
"""
from __future__ import annotations

import inspect
import re

from src.core.models import FaMaxDocumentRequests, FaMaxFileState


def _migration_columns(table: str) -> set[str]:
    import migrations.apply_fa_max_wp_t2_6_stage_monitoring as migration

    source = inspect.getsource(migration)
    body = source.split(f"CREATE TABLE IF NOT EXISTS {table} (", 1)[1]
    body = body.split('"""', 1)[0]
    columns = set()
    for line in body.splitlines():
        line = line.strip()
        match = re.match(r"^([a-z_]+)\s+(UUID|TEXT|TIMESTAMPTZ|BIGSERIAL)\b", line)
        if match:
            columns.add(match.group(1))
    assert columns, f"failed to parse columns for {table}"
    return columns


def test_file_state_model_columns_match_migration():
    assert set(FaMaxFileState.__table__.columns.keys()) == _migration_columns(
        "fa_max_file_state"
    )


def test_document_requests_model_columns_match_migration():
    assert set(FaMaxDocumentRequests.__table__.columns.keys()) == _migration_columns(
        "fa_max_document_requests"
    )


def test_file_state_model_mirrors_migration_constraints_and_indexes():
    table = FaMaxFileState.__table__
    assert table.name == "fa_max_file_state"
    assert table.columns["opportunity_id"].unique is True
    assert table.columns["contact_email"].nullable is True
    assert table.columns["backflip_stage"].nullable is False
    assert {c.name for c in table.constraints if c.name} >= {
        "ck_fa_max_file_state_stage"
    }
    assert {i.name for i in table.indexes} == {
        "idx_fa_max_file_state_stall",
        "idx_fa_max_file_state_touch",
    }
    assert [fk.target_fullname for fk in table.columns["opportunity_id"].foreign_keys] == [
        "fa_max_opportunities.opportunity_id"
    ]
    assert [fk.target_fullname for fk in table.columns["person_id"].foreign_keys] == [
        "fa_max_persons.person_id"
    ]


def test_document_requests_model_mirrors_migration_constraints_and_indexes():
    table = FaMaxDocumentRequests.__table__
    assert table.name == "fa_max_document_requests"
    assert {c.name for c in table.constraints if c.name} >= {
        "ck_fa_max_doc_request_source",
        "uq_fa_max_doc_request_idempotency",
    }
    assert {i.name for i in table.indexes} == {
        "idx_fa_max_doc_requests_outstanding",
        "idx_fa_max_doc_requests_chase_due",
    }
    assert table.columns["document_name"].nullable is False
    assert table.columns["received_at"].nullable is True
