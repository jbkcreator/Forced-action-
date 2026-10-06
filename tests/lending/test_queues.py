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
    ("list_2", q.TRANSACTION_READY), ("list_9", q.TRANSACTION_READY), ("list_6", q.TRANSACTION_READY),
    ("list_3", q.BUILDERS), ("list_7", q.BUILDERS),
    ("list_4", q.PARTNERS),
])
def test_source_tags_map_to_the_four_ranked_queues(tag, queue):
    out = assign_queue({"source_tag": tag, "phone": "+18135550001"})
    assert out["queue"] == queue and out["pool"] == queue and out["source_tag"] == tag


def test_cash_buyers_list_2_are_bookable_in_rank_2_not_nurture():
    """Josh, Oct 4 §2: "cash buyers move out of Nurture into rank 2, because
    delayed financing is a real borrower conversation."."""
    out = assign_queue({"source_tag": "list_2", "phone": "+18135550001"})
    assert out["queue"] == q.TRANSACTION_READY and out["bookable"] is True


def test_partners_list_4_are_dialed_as_rank_4_but_never_bookable():
    """Oct 4 §2: "partner script only, never pitched as borrowers" — a real ranked
    queue now, not the old nurture-only bucket, but still never booked."""
    out = assign_queue({"source_tag": "list_4", "phone": "+18135550001"})
    assert out["queue"] == q.PARTNERS and out["pool"] == q.PARTNERS and out["bookable"] is False


@pytest.mark.parametrize("tag", ["list_1", "list_2", "list_9", "list_3"])
def test_launch_queue_records_are_bookable(tag):
    assert assign_queue({"source_tag": tag, "phone": "+18135550001"})["bookable"] is True


@pytest.mark.parametrize("tag", ["list_99", None])
def test_unknown_tags_get_no_queue(tag):
    out = assign_queue({"source_tag": tag, "phone": "+18135550001"})
    assert out["queue"] is None and out["bookable"] is False


def test_config_is_valid_and_shares_sum_to_one():
    q.validate_queue_config()
    assert q.DIAL_SHARE == {
        q.VERIFIED_MATURITY: 0.5, q.TRANSACTION_READY: 0.25, q.BUILDERS: 0.2, q.PARTNERS: 0.05,
    }
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
        rec("d", "not-a-phone", "list_9"), rec("e", "+18135558399", "list_2"),           # rank 2: cash buyers
    ]
    report = queue_count_report(records, db, tracerfy_balance=7313)
    assert report["queues"][q.VERIFIED_MATURITY]["raw"] == 1
    assert report["queues"][q.VERIFIED_MATURITY]["eligible"] == 1
    b = report["queues"][q.BUILDERS]
    assert (b["raw"], b["traced"], b["eligible"]) == (2, 2, 1)   # de-duplicated by phone
    t = report["queues"][q.TRANSACTION_READY]
    assert (t["raw"], t["traced"], t["eligible"]) == (2, 1, 0)   # list_9 invalid phone; list_2 traced but unscrubbed
    assert q.NURTURE not in report["queues"]
    assert report["tracerfy_balance"] == 7313
    assert (b["scrubbed"], b["after_backflip"]) == (2, 2)   # both builder numbers pass DNC and Backflip


def test_stage_counts_split_scrub_blocks_from_backflip_blocks():
    from src.lending.queues import stage_counts
    counts = stage_counts(traced=10, excluded_by_reason={
        "NATIONAL_DNC": 3, "LITIGATOR": 1, "INVALID_PHONE": 2, "BACKFLIP_CONFLICT": 2, "BACKFLIP_FEED_STALE": 1})
    assert counts == {"scrubbed": 6, "after_backflip": 3}


@pytestmark_db
def test_tracerfy_hit_rate_comes_from_the_usage_ledger(db):
    from src.lending.queues import tracerfy_hit_rate
    for ok in (True, True, False, True):
        db.execute(text("INSERT INTO enrichment_usage_logs (vendor, purpose, success, cost_cents, target_address, "
                        "created_at, quality_discounted) VALUES ('tracerfy', 'skip_trace', :s, 0, 'hit-rate-test', "
                        "now() + interval '100 years', false)"), {"s": ok})
    rate = tracerfy_hit_rate(db, since=__import__("datetime").datetime(2126, 1, 1, tzinfo=__import__("datetime").timezone.utc))
    assert rate == 0.75


@pytestmark_db
def test_an_unscrubbed_number_is_never_counted_as_scrubbed_even_when_backflip_blocks_it(db, monkeypatch):
    from src.lending import dialer_load
    monkeypatch.setattr(dialer_load, "load_backflip_identifier_index",
                        lambda _db: BackflipIdentifierIndex(block_reason="BACKFLIP_FEED_STALE"))
    record = {"source_record_ref": "u1", "phone": "+18135558399", "source_tag": "list_9", "parcel_id": "P-u1",
              "entity_name": "U LLC", "borrower_name": "B", "property_address": "1 St"}
    t = queue_count_report([record], db)["queues"][q.TRANSACTION_READY]
    assert (t["traced"], t["needs_scrub"], t["scrubbed"]) == (1, 1, 0)
