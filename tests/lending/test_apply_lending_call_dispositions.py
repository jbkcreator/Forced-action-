"""Go Live call log: lending.call_dispositions carries queue / source_tag / seat_group."""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)

GO_LIVE_COLUMNS = {"queue", "source_tag", "seat_group"}


@pytest.fixture
def env():
    engine = create_engine(os.environ["DATABASE_URL"])
    schema = f"lending_t_{uuid.uuid4().hex[:8]}"
    yield engine, schema
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    engine.dispose()


def _columns(engine, schema):
    with engine.connect() as c:
        return {r[0] for r in c.execute(
            text("SELECT column_name FROM information_schema.columns "
                 "WHERE table_schema = :s AND table_name = 'call_dispositions'"), {"s": schema})}


def test_apply_creates_call_log_with_go_live_columns(env):
    from migrations.apply_lending_call_dispositions import apply

    engine, schema = env
    apply(engine=engine, schema=schema)
    cols = _columns(engine, schema)
    assert GO_LIVE_COLUMNS <= cols
    assert {"dialer_call_id", "phone", "direction", "disposition", "call_ended_at",
            "recording_disclosure_logged", "raw_event"} <= cols


def test_apply_adds_missing_columns_to_an_existing_call_log(env):
    from migrations.apply_lending_call_dispositions import apply

    engine, schema = env
    with engine.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{schema}"'))
        c.execute(text(f'CREATE TABLE "{schema}".call_dispositions ('
                       "id serial PRIMARY KEY, dialer_call_id varchar NOT NULL UNIQUE, phone varchar, "
                       "direction varchar, disposition varchar, call_ended_at timestamptz, "
                       "recording_disclosure_logged boolean NOT NULL DEFAULT false, "
                       "raw_event jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), "
                       "updated_at timestamptz NOT NULL DEFAULT now())"))
    apply(engine=engine, schema=schema)
    assert GO_LIVE_COLUMNS <= _columns(engine, schema)


def test_apply_twice_is_safe(env):
    from migrations.apply_lending_call_dispositions import apply

    engine, schema = env
    apply(engine=engine, schema=schema)
    apply(engine=engine, schema=schema)
    assert GO_LIVE_COLUMNS <= _columns(engine, schema)
