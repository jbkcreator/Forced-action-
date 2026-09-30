"""Go Live G14/G16: nine source lists map to three launch queues; count report per queue."""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config import lending_queues as q
from src.lending.backflip_conflict import BackflipIdentifierIndex
from src.lending.models import LendingDialerLoadRecord
from src.lending.queues import assign_queue, queue_count_report


@pytest.mark.parametrize("tag,queue", [
    ("list_1", q.VERIFIED_MATURITY), ("list_5", q.VERIFIED_MATURITY), ("list_8", q.VERIFIED_MATURITY),
    ("list_9", q.TRANSACTION_READY), ("list_6", q.TRANSACTION_READY),
    ("list_3", q.BUILDERS), ("list_7", q.BUILDERS),
])
def test_source_tags_map_to_the_three_launch_queues(tag, queue):
    out = assign_queue({"source_tag": tag, "phone": "+18135550001"})
    assert out["queue"] == queue and out["pool"] == queue and out["source_tag"] == tag


@pytest.mark.parametrize("tag", ["list_2", "list_4"])
def test_nurture_lists_are_dialed_but_never_bookable(tag):
    out = assign_queue({"source_tag": tag, "phone": "+18135550001"})
    assert out["queue"] == q.NURTURE and out["pool"] == q.NURTURE and out["bookable"] is False


@pytest.mark.parametrize("tag", ["list_1", "list_9", "list_3"])
def test_launch_queue_records_are_bookable(tag):
    assert assign_queue({"source_tag": tag, "phone": "+18135550001"})["bookable"] is True


@pytest.mark.parametrize("tag", ["list_99", None])
def test_unknown_tags_get_no_queue(tag):
    out = assign_queue({"source_tag": tag, "phone": "+18135550001"})
    assert out["queue"] is None and out["bookable"] is False


def test_config_is_valid_and_shares_sum_to_one():
    q.validate_queue_config()
    assert q.DIAL_SHARE == {q.VERIFIED_MATURITY: 0.5, q.TRANSACTION_READY: 0.3, q.BUILDERS: 0.2}
    assert not (q.NURTURE_ONLY_TAGS & set(q.SOURCE_TAG_QUEUES))


pytestmark_db = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL"
)


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    LendingDialerLoadRecord.__table__.create(conn, checkfirst=True)
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


@pytestmark_db
def test_count_report_walks_raw_to_eligible_per_queue(db, monkeypatch):
    from src.lending import dialer_load
    monkeypatch.setattr(dialer_load, "load_backflip_identifier_index",
                        lambda _db: BackflipIdentifierIndex(block_reason=None))
    p_ok, p_dup = "+18135558301", "+18135558302"
    for phone in (p_ok, p_dup):
        db.execute(text("INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) "
                        "VALUES (:p, false, false, now() - interval '1 day', 'test')"), {"p": phone})
    def rec(ref, phone, tag):
        return {"source_record_ref": ref, "phone": phone, "source_tag": tag, "parcel_id": f"P-{ref}",
                "entity_name": f"{ref} LLC", "borrower_name": "B", "property_address": "1 St"}
    records = [
        rec("a", p_ok, "list_1"), rec("b", p_dup, "list_3"), rec("c", p_dup, "list_7"),  # same phone twice in builders
        rec("d", "not-a-phone", "list_9"), rec("e", "+18135558399", "list_2"),           # nurture-only
    ]
    report = queue_count_report(records, db, tracerfy_balance=7313)
    assert report["queues"][q.VERIFIED_MATURITY]["raw"] == 1
    assert report["queues"][q.VERIFIED_MATURITY]["eligible"] == 1
    b = report["queues"][q.BUILDERS]
    assert (b["raw"], b["traced"], b["eligible"]) == (2, 2, 1)   # de-duplicated by phone
    t = report["queues"][q.TRANSACTION_READY]
    assert (t["raw"], t["traced"], t["eligible"]) == (1, 0, 0)   # invalid phone -> not traced
    assert report["queues"][q.NURTURE]["raw"] == 1
    assert report["tracerfy_balance"] == 7313
