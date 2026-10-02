"""Pool staging moves into the lending schema; the old name keeps working as a view."""
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
    tag = uuid.uuid4().hex[:8]
    src, dst = f"pub_t_{tag}", f"lending_t_{tag}"
    with engine.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{src}"'))
        c.execute(text(f'CREATE SCHEMA "{dst}"'))
        c.execute(text(f'CREATE TABLE "{src}".lending_calling_pool_staging (id bigserial PRIMARY KEY, '
                       "run_id uuid NOT NULL, pool_name text NOT NULL, source_tag text)"))
        c.execute(text(f'CREATE INDEX idx_t_run ON "{src}".lending_calling_pool_staging (run_id)'))
        c.execute(text(f'INSERT INTO "{src}".lending_calling_pool_staging (run_id, pool_name) '
                       "VALUES (gen_random_uuid(), 'active_builder')"))
    yield engine, src, dst
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS "{src}" CASCADE'))
        c.execute(text(f'DROP SCHEMA IF EXISTS "{dst}" CASCADE'))
    engine.dispose()


def _kind(engine, schema, name):
    with engine.connect() as c:
        return c.execute(text("SELECT table_type FROM information_schema.tables "
                              "WHERE table_schema = :s AND table_name = :n"), {"s": schema, "n": name}).scalar()


def test_moves_the_table_keeps_data_and_leaves_a_working_view(env):
    from migrations.apply_lending_pool_staging_schema import apply
    engine, src, dst = env
    apply(engine=engine, source_schema=src, target_schema=dst)
    apply(engine=engine, source_schema=src, target_schema=dst)   # idempotent
    assert _kind(engine, dst, "calling_pool_staging") == "BASE TABLE"
    assert _kind(engine, src, "lending_calling_pool_staging") == "VIEW"
    with engine.begin() as c:
        assert c.execute(text(f'SELECT count(*) FROM "{dst}".calling_pool_staging')).scalar() == 1
        c.execute(text(f'INSERT INTO "{src}".lending_calling_pool_staging (run_id, pool_name) '
                       "VALUES (gen_random_uuid(), 'mortgage_broker')"))          # old writers still work
        assert c.execute(text(f'SELECT count(*) FROM "{dst}".calling_pool_staging')).scalar() == 2
        idx = c.execute(text("SELECT count(*) FROM pg_indexes WHERE schemaname = :s AND indexname = 'idx_t_run'"),
                        {"s": dst}).scalar()
        assert idx == 1


def test_model_points_at_the_lending_schema():
    from src.core.models import LendingCallingPoolStaging as M
    assert (M.__table__.schema, M.__table__.name) == ("lending", "calling_pool_staging")
