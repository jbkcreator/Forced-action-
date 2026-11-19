"""The GHL sync migration widens a status column created too narrow by its first run."""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

from migrations.apply_lending_dialer_load_ghl_sync import apply
from src.lending.ghl_load_sync import SyncStatus

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)


@pytest.fixture
def schema():
    engine = create_engine(os.environ["DATABASE_URL"])
    name = f"lending_t_{uuid.uuid4().hex[:8]}"
    with engine.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{name}"'))
        c.execute(text(f'CREATE TABLE "{name}".dialer_load_records (id bigserial PRIMARY KEY, pool text NOT NULL, '
                       "active boolean NOT NULL DEFAULT true, loaded_at timestamptz NOT NULL DEFAULT now(), "
                       "ghl_sync_status VARCHAR(20))"))
        c.execute(text(f"INSERT INTO \"{name}\".dialer_load_records (pool) VALUES ('partners'), ('partners')"))
    yield engine, name
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
    engine.dispose()


def test_reload_skip_records_alongside_a_push_after_rerun(schema):
    engine, name = schema
    apply(engine, schema=name)
    apply(engine, schema=name)  # idempotent
    with engine.begin() as c:
        c.execute(text(f'UPDATE "{name}".dialer_load_records AS d SET ghl_sync_status = o.status '
                       "FROM unnest(CAST(:ids AS bigint[]), CAST(:statuses AS text[])) AS o(id, status) WHERE d.id = o.id"),
                  {"ids": [1, 2], "statuses": [SyncStatus.PUSHED.value, SyncStatus.SKIPPED_ALREADY_IN_GHL.value]})
        got = c.execute(text(f'SELECT ghl_sync_status FROM "{name}".dialer_load_records ORDER BY id')).scalars().all()
    assert got == ["pushed", "skipped_already_in_ghl"]
