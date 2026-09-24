"""tests/migrations/test_apply_fa_max_wp_t2_6_stage_monitoring.py

Runs the migration against a throwaway schema-only check: it must be
idempotent (safe to run twice) and must not error if the underlying
fa_max_opportunities/fa_max_persons tables don't exist in whatever ad-hoc
engine is under test -- this test only checks idempotent DDL shape via
information_schema, matching the style of other migration tests in this
repo (no live Postgres assumed at unit-test time; skip if unavailable).
"""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="requires a live Postgres DATABASE_URL",
)


def test_migration_is_idempotent():
    from migrations.apply_fa_max_wp_t2_6_stage_monitoring import apply

    engine = create_engine(os.environ["DATABASE_URL"])
    apply(engine=engine)
    apply(engine=engine)  # must not raise on second run

    with engine.connect() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name IN ('fa_max_file_state', 'fa_max_document_requests')"
                )
            )
        }
    assert tables == {"fa_max_file_state", "fa_max_document_requests"}
