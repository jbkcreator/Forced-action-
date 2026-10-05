"""Go Live: every staged pool record carries its brief source list (List 1-9)."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
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
    # Not stalled -> List 2 "cash buyers" (#318/#320 reconciliation 2026-10-02): the
    # original source_tag_for returned None here, silently excluding non-stalled
    # wholesalers from every brief list. Pool 1's own data source (total_cash_volume)
    # matches Josh's "cash buyers" label — see source_tag_for's own docstring.
    ("wholesaler_flipper", "buyer_entities", False, "list_2"),
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
                          prop_city="Tampa", prop_state="FL", prop_zip="33602", auction_id=77,
                          homestead_exempt=None)
    rec = pe.auction_winner_record(row)
    assert rec.pool_name == "auction_winner" and rec.source_tag == "list_6"
    assert rec.entity_name == "ACME HOLDINGS LLC" and rec.entity_status == "LLC"
    assert rec.phone_available is False and rec.normalized_phone is None
    assert rec.source_table == "tax_deed_auctions" and rec.target_property_address.startswith("1 Main St")
    assert rec.homestead_exempt is None


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
            "INSERT INTO lending.calling_pool_staging (run_id, pool_name, aircall_campaign_tag, source_table, source_tag) "
            "VALUES (:r, 'auction_winner', 'DESK_CAPITAL_LOOP', 'tax_deed_auctions', 'list_6')"), {"r": str(uuid.uuid4())})
    finally:
        tx.rollback()
        conn.close()
        engine.dispose()


def test_orm_model_matches_the_source_tag_migration():
    from migrations.apply_lending_pool_source_tags import POOLS
    from src.core.models import LendingCallingPoolStaging as M

    table = M.__table__
    assert "source_tag" in table.c
    assert "idx_lcps_source_tag" in {i.name for i in table.indexes}
    checks = [str(c.sqltext) for c in table.constraints if c.name == "lending_calling_pool_staging_pool_name_check"]
    assert checks and all(p in checks[0] for p in POOLS)


@pytest.mark.skipif(not __import__("os").environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")
def test_staging_write_persists_every_column_including_source_tag():
    import os
    import uuid
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session

    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    try:
        session = Session(bind=conn, join_transaction_mode="create_savepoint")
        run_id = str(uuid.uuid4())
        row = SimpleNamespace(sold_to="ACME HOLDINGS LLC", sold_amount=150000, property_id=None, parcel_id="P-9",
                              county_id="hillsborough", county_name="Hillsborough", prop_address="1 Main St",
                              prop_city="Tampa", prop_state="FL", prop_zip="33602", auction_id=77,
                              homestead_exempt=None)
        rec = pe.auction_winner_record(row)
        rec.run_id = run_id
        assert pe._write_to_staging(session, [rec]) == 1
        got = session.execute(text("SELECT pool_name, source_tag, entity_name, phone_available "
                                   "FROM lending.calling_pool_staging WHERE run_id = CAST(:r AS uuid)"),
                              {"r": run_id}).fetchall()
        assert got == [("auction_winner", "list_6", "ACME HOLDINGS LLC", False)]
    finally:
        tx.rollback()
        conn.close()
        engine.dispose()


# ── List 7: owners pulling construction permits (NOCs / permits) ──

def _permit_row(**over):
    row = dict(owner_name="SUNSHINE HOMES LLC", owner_phone="(813) 555-7701", owner_email="Owner@Example.com",
               job_value=400000, permit_type="Residential New Construction and Additions",
               issue_date=date(2026, 8, 1), permit_number="BP-77", county_id="hillsborough",
               county_name="Hillsborough", source_property_id=5, parcel_id="P-77",
               prop_address="77 Oak St", prop_city="Tampa", prop_state="FL", prop_zip="33602",
               homestead_exempt=None)
    row.update(over)
    return SimpleNamespace(**row)


def test_a_permit_owner_becomes_a_list_7_builders_record():
    rec = pe.permit_owner_record(_permit_row())
    assert (rec.pool_name, rec.source_tag, rec.source_table) == ("permit_owner", "list_7", "building_permits")
    assert rec.normalized_phone == "+18135557701" and rec.phone_available is True
    assert rec.entity_name == "SUNSHINE HOMES LLC" and rec.entity_status == "LLC"
    assert rec.estimated_loan_value == Decimal("340000.00")          # 85% LTC of the permit value
    assert "New Construction" in rec.recent_permit_details and rec.permit_number == "BP-77"


def test_a_permit_owners_homestead_status_flows_through_to_the_record():
    """F8: the gate checks CallingPoolRecord.homestead_exempt, so it must actually
    carry the property's real status through, not just default silently to None."""
    rec = pe.permit_owner_record(_permit_row(homestead_exempt=True))
    assert rec.homestead_exempt is True
    rec = pe.permit_owner_record(_permit_row(homestead_exempt=False))
    assert rec.homestead_exempt is False


def test_a_permit_owner_without_a_phone_is_staged_for_tracing():
    rec = pe.permit_owner_record(_permit_row(owner_phone=None))
    assert rec.normalized_phone is None and rec.phone_available is False


def test_permit_owner_pool_maps_to_list_7_and_the_builders_queue():
    from config import lending_queues as q
    from src.lending.queues import assign_queue
    assert pe.source_tag_for("permit_owner", "building_permits") == "list_7"
    assert assign_queue({"source_tag": "list_7"})["queue"] == q.BUILDERS


def test_a_phone_already_claimed_by_a_higher_pool_is_not_repeated():
    kept = pe.drop_claimed_phones([pe.permit_owner_record(_permit_row()),
                                   pe.permit_owner_record(_permit_row(owner_phone="8135557702"))],
                                  claimed={"+18135557701"})
    assert [r.normalized_phone for r in kept] == ["+18135557702"]


def test_the_pool_check_allows_permit_owners():
    from migrations.apply_lending_pool_source_tags import POOLS
    from src.core.models import LendingCallingPoolStaging as M
    assert "permit_owner" in POOLS
    check = [str(c.sqltext) for c in M.__table__.constraints if c.name == "lending_calling_pool_staging_pool_name_check"][0]
    assert "permit_owner" in check
