"""Go Live: every staged pool record carries its brief source list (List 1-9)."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from src.services.lending import pool_extraction as pe

TODAY = date(2026, 9, 30)


@pytest.mark.parametrize("pool,source_table,stalled,tag", [
    # Pool 2 is one row per contractor (builder), whatever table supplied it.
    ("active_builder", "building_permits", False, "list_3"),
    ("active_builder", "dbpr_contacts", False, "list_3"),
    ("mortgage_broker", "ofr_mortgage_brokers", False, "list_4"),
    ("wholesaler_flipper", "buyer_entities", True, "list_9"),
    ("wholesaler_flipper", "buyer_entities", False, None),
    ("auction_winner", "tax_deed_auctions", False, "list_6"),
])
def test_source_tag_by_pool_and_source(pool, source_table, stalled, tag):
    assert pe.source_tag_for(pool, source_table, stalled=stalled) == tag


@pytest.mark.parametrize("bought,resold,stalled", [
    (date(2026, 1, 1), False, True),     # 272 days, still held
    (date(2026, 7, 2), False, True),     # exactly 90 days
    (date(2026, 7, 3), False, False),    # 89 days
    (date(2025, 1, 1), True, False),     # resold: flip completed
    (None, False, False),                # no purchase date: not provable
])
def test_stalled_flip_rule(bought, resold, stalled):
    assert pe.is_stalled_flip(bought, resold=resold, today=TODAY) is stalled


def test_auction_winner_row_becomes_a_phoneless_list_6_record():
    row = SimpleNamespace(sold_to="ACME HOLDINGS LLC", sold_amount=150000, property_id=9, parcel_id="P-9",
                          county_id="hillsborough", county_name="Hillsborough", prop_address="1 Main St",
                          prop_city="Tampa", prop_state="FL", prop_zip="33602", auction_id=77)
    rec = pe.auction_winner_record(row)
    assert rec.pool_name == "auction_winner" and rec.source_tag == "list_6"
    assert rec.entity_name == "ACME HOLDINGS LLC" and rec.entity_status == "LLC"
    assert rec.phone_available is False and rec.normalized_phone is None
    assert rec.source_table == "tax_deed_auctions" and rec.target_property_address.startswith("1 Main St")


@pytest.mark.skipif(not __import__("os").environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")
def test_migration_adds_source_tag_and_allows_auction_winners():
    import os
    import uuid
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
    from migrations.apply_lending_pool_source_tags import apply

    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    try:
        session = Session(bind=conn)
        apply(session)
        apply(session)  # idempotent
        session.execute(text(
            "INSERT INTO lending_calling_pool_staging (run_id, pool_name, aircall_campaign_tag, source_table, source_tag) "
            "VALUES (:r, 'auction_winner', 'DESK_CAPITAL_LOOP', 'tax_deed_auctions', 'list_6')"), {"r": str(uuid.uuid4())})
    finally:
        tx.rollback()
        conn.close()
        engine.dispose()
