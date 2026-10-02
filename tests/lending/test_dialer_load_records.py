"""lending.dialer_load_records schema.

Structural checks run everywhere. The DDL checks run against a throwaway
schema (never the shared ``lending`` schema) and drop it afterwards; they
need a live Postgres DATABASE_URL in the environment.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from src.lending.models import LENDING_SCHEMA, LendingDialerLoadRecord

needs_db = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="requires a live Postgres DATABASE_URL",
)

PHONE = "+18135550100"
PHONE_HASH = "a" * 64


class TestModel:
    def test_table_lives_in_lending_schema(self):
        assert LendingDialerLoadRecord.__table__.schema == LENDING_SCHEMA

    def test_one_active_row_per_phone_index(self):
        index = next(i for i in LendingDialerLoadRecord.__table__.indexes
                     if i.name == "uq_lending_dialer_load_records_active_phone")
        assert index.unique
        assert [c.name for c in index.columns] == ["phone"]
        assert str(index.dialect_options["postgresql"]["where"]) == "active"

    def test_call_time_lookup_index(self):
        index = next(i for i in LendingDialerLoadRecord.__table__.indexes
                     if i.name == "idx_lending_dialer_load_records_phone_loaded")
        assert [c.name for c in index.columns] == ["phone", "loaded_at"]


@pytest.fixture
def schema_engine():
    from migrations.apply_lending_dialer_load_records import apply

    engine = create_engine(os.environ["DATABASE_URL"])
    schema = f"lending_t_{uuid.uuid4().hex[:8]}"
    apply(engine=engine, schema=schema)
    yield engine, schema
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    engine.dispose()


def _insert(conn, schema, *, run_id="run-1", active=True, loaded_at=None, contact_id=None):
    conn.execute(
        text(
            f'INSERT INTO "{schema}".dialer_load_records '
            "(run_id, pool, source_record_ref, phone, phone_hash, active, loaded_at, "
            " deactivated_at, dialer_contact_id) "
            "VALUES (:run_id, 'builders', 'property:1', :phone, :hash, :active, "
            " COALESCE(:loaded_at, now()), CASE WHEN :active THEN NULL ELSE now() END, :contact_id)"
        ),
        {"run_id": run_id, "phone": PHONE, "hash": PHONE_HASH, "active": active,
         "loaded_at": loaded_at, "contact_id": contact_id},
    )


@needs_db
class TestSchema:
    def test_apply_is_idempotent(self, schema_engine):
        from migrations.apply_lending_dialer_load_records import apply

        engine, schema = schema_engine
        apply(engine=engine, schema=schema)
        with engine.connect() as conn:
            count = conn.execute(text(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = :s AND table_name = 'dialer_load_records'"
            ), {"s": schema}).scalar()
        assert count == 1

    def test_second_active_row_for_same_phone_is_rejected(self, schema_engine):
        engine, schema = schema_engine
        with engine.begin() as conn:
            _insert(conn, schema)
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                _insert(conn, schema, run_id="run-2")

    def test_inactive_history_rows_are_allowed(self, schema_engine):
        engine, schema = schema_engine
        with engine.begin() as conn:
            _insert(conn, schema, run_id="run-1", active=False)
            _insert(conn, schema, run_id="run-2", active=False)
            _insert(conn, schema, run_id="run-3", active=True)
            total = conn.execute(text(f'SELECT count(*) FROM "{schema}".dialer_load_records')).scalar()
        assert total == 3

    def test_inactive_row_requires_deactivated_at(self, schema_engine):
        engine, schema = schema_engine
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(
                    f'INSERT INTO "{schema}".dialer_load_records '
                    "(run_id, pool, source_record_ref, phone, phone_hash, active) "
                    "VALUES ('run-1', 'builders', 'property:1', :phone, :hash, false)"
                ), {"phone": PHONE, "hash": PHONE_HASH})

    def test_call_time_lookup_picks_latest_row_loaded_before_the_call(self, schema_engine):
        engine, schema = schema_engine
        now = datetime.now(timezone.utc)
        with engine.begin() as conn:
            _insert(conn, schema, run_id="old", active=False, loaded_at=now - timedelta(days=2), contact_id=1)
            _insert(conn, schema, run_id="current", active=False, loaded_at=now - timedelta(hours=1), contact_id=2)
            _insert(conn, schema, run_id="later", active=True, loaded_at=now + timedelta(hours=1), contact_id=3)
            run_id = conn.execute(text(
                f'SELECT run_id FROM "{schema}".dialer_load_records '
                "WHERE phone = :phone AND loaded_at <= :call_started_at "
                "ORDER BY loaded_at DESC LIMIT 1"
            ), {"phone": PHONE, "call_started_at": now}).scalar()
        assert run_id == "current"
