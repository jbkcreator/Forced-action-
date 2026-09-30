"""Pool records for the queue report and dialer load come from ONE staging run."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def _stage(db, run_id, at, phone, tag="list_3", pool="active_builder"):
    db.execute(text(
        "INSERT INTO lending_calling_pool_staging (run_id, pool_name, aircall_campaign_tag, source_table, source_tag, "
        "normalized_phone, phone_available, borrower_name, entity_name, target_property_address, estimated_loan_value, "
        "state, entity_status, parcel_id, created_at) VALUES (:r, :pool, 'X', 'building_permits', :tag, :p, :pa, "
        "'Jane Roe', 'Roe LLC', '1 Main St, TAMPA FL 33602', 250000, 'FL', 'LLC', 'P1', :at)"),
        {"r": run_id, "pool": pool, "tag": tag, "p": phone, "pa": phone is not None, "at": at})


def test_reads_only_the_named_run(db):
    from src.lending.pool_source import staged_pool_records
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    _stage(db, a, datetime.now(timezone.utc), "+18135559001")
    _stage(db, b, datetime.now(timezone.utc), "+18135559002")
    records = staged_pool_records(db, run_id=a)
    assert [r["phone"] for r in records] == ["+18135559001"]
    r = records[0]
    assert r["source_tag"] == "list_3" and r["entity_name"] == "Roe LLC" and r["state"] == "FL"
    assert r["property_address"].startswith("1 Main St") and r["source_record_ref"]


def test_defaults_to_the_latest_run(db):
    from src.lending.pool_source import latest_run_id
    old, new = str(uuid.uuid4()), str(uuid.uuid4())
    future = datetime.now(timezone.utc) + timedelta(days=365)
    _stage(db, old, future, "+18135559003")
    _stage(db, new, future + timedelta(minutes=1), "+18135559004")
    assert latest_run_id(db) == new


def test_dialer_load_input_keeps_only_launch_queue_records(db):
    from src.tasks.lending_dialer_load import launch_queue_records
    records = [{"source_tag": "list_3", "phone": "+1"}, {"source_tag": "list_4", "phone": "+2"},
               {"source_tag": None, "phone": "+3"}, {"source_tag": "list_9", "phone": "+4"}]
    out = launch_queue_records(records)
    assert [(r["phone"], r["pool"]) for r in out] == [("+1", "builders"), ("+4", "transaction_ready")]
