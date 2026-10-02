"""Vendor-neutral id columns (same names as PR #319): run on a throwaway schema."""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)


@pytest.fixture
def env():
    engine = create_engine(os.environ["DATABASE_URL"])
    schema = f"lending_t_{uuid.uuid4().hex[:8]}"
    with engine.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{schema}"'))
        c.execute(text(f'CREATE TABLE "{schema}".call_dispositions (id serial PRIMARY KEY, '
                       "aircall_call_id varchar NOT NULL UNIQUE, aircall_contact_id varchar, caller_line varchar, "
                       "raw_event jsonb NOT NULL DEFAULT '{}')"))
        c.execute(text(f'CREATE TABLE "{schema}".dialer_load_records (id bigserial PRIMARY KEY, aircall_contact_id bigint)'))
        c.execute(text(f'INSERT INTO "{schema}".dialer_load_records (aircall_contact_id) VALUES (12345)'))
    yield engine, schema
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    engine.dispose()


def _cols(engine, schema, table):
    with engine.connect() as c:
        return dict(c.execute(text("SELECT column_name, data_type FROM information_schema.columns "
                                   "WHERE table_schema = :s AND table_name = :t"), {"s": schema, "t": table}).fetchall())


def test_renames_to_the_pr_319_names_and_keeps_data(env):
    from migrations.apply_lending_dialer_neutral_ids import apply
    engine, schema = env
    apply(engine=engine, schema=schema)
    apply(engine=engine, schema=schema)   # idempotent
    calls = _cols(engine, schema, "call_dispositions")
    assert {"dialer_call_id", "dialer_contact_id", "caller_id_number"} <= set(calls)
    assert not {"aircall_call_id", "aircall_contact_id", "caller_line"} & set(calls)
    loads = _cols(engine, schema, "dialer_load_records")
    assert loads["dialer_contact_id"] == "character varying" and "aircall_contact_id" not in loads
    with engine.connect() as c:
        assert c.execute(text(f'SELECT dialer_contact_id FROM "{schema}".dialer_load_records')).scalar() == "12345"
        assert c.execute(text("SELECT count(*) FROM pg_constraint WHERE conname = 'call_dispositions_dialer_call_id_key' "
                              f"AND conrelid = '\"{schema}\".call_dispositions'::regclass")).scalar() == 1
