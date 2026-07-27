"""
Regression tests for four Hunter (HUNTER-01/02) production bugs found in
code review against src/services/buyer_entity_resolution.py,
scripts/backfill_buyer_entities.py, src/tasks/hunter_nightly_sweep.py, and
src/connectors/whale_auction_fast_follow.py.

Runs against the real Postgres DB (get_db_context / DATABASE_URL), same
convention as test_contact_triangulation_e2e.py: every test seeds its own
rows under a sentinel county_id/name token so nothing outside the seeded
fixtures is ever read into a sweep, and everything seeded is deleted in
teardown.

Coverage:
  - cross-county buyer resolution: the same buyer appearing in two counties
    must resolve to ONE buyer_entities row, not two (issue #1)
  - portfolio aggregation: a corrective re-recording (two deeds rows on one
    property) must not double-count cash_volume (issue #2)
  - backfill resumability: re-running the backfill after a prior run
    already committed must not raise a unique-constraint error and must
    not create duplicate entities (issue #3)
  - kill switch: an active "STOP Hunter" override must make both
    cron-triggered writers (hunter_nightly_sweep, whale_auction_fast_follow)
    return without mutating the DB (issue #4)

Run:
    pytest tests/scenarios/test_hunter_resolution_fixes.py -v -m scenario
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import delete, text

from src.core.database import get_db_context
from src.core.models import BuyerEntity, BuyerEntityLink, Deed, Owner, Property
from src.services.buyer_entity_resolution import refresh_portfolio_aggregates, run_incremental

pytestmark = pytest.mark.scenario

_COUNTY_A = "ztest-hunter-hillsborough"
_COUNTY_B = "ztest-hunter-pinellas"


def _uid() -> str:
    return uuid.uuid4().hex[:10]


def _seed_property_owner(session, *, county_id: str, owner_name: str, mailing_address: str,
                          parcel_prefix: str) -> tuple[Property, Owner]:
    prop = Property(
        parcel_id=f"{parcel_prefix}-{_uid()}", address="1 Test Way", city="Tampa",
        state="FL", zip="00001", county_id=county_id,
    )
    session.add(prop)
    session.flush()

    owner = Owner(
        property_id=prop.id, owner_name=owner_name, mailing_address=mailing_address,
        owner_type="Individual", county_id=county_id,
    )
    session.add(owner)
    session.flush()
    return prop, owner


def _cleanup_buyer_entities_for_links(session, source_table: str, source_ids: list[int]) -> None:
    """Delete any buyer_entities this test's links attached to (cascade deletes the links)."""
    if not source_ids:
        return
    entity_ids = [
        row[0] for row in session.execute(
            text(
                "SELECT DISTINCT buyer_entity_id FROM buyer_entity_links "
                "WHERE source_table = :st AND source_id = ANY(:ids)"
            ),
            {"st": source_table, "ids": source_ids},
        ).fetchall()
    ]
    if entity_ids:
        session.execute(delete(BuyerEntity).where(BuyerEntity.id.in_(entity_ids)))
    session.commit()


# ── Issue #1 — cross-county buyer resolution ───────────────────────────────

def test_cross_county_buyer_merges_to_one_entity():
    """
    Same buyer (identical name + mailing address) shows up first in county A,
    then county B. Before the fix, run_incremental's second (county B) call
    loaded existing-entity anchors scoped to county_id=B only, so it never
    saw the entity county A's call just created -- producing a second,
    duplicate buyer_entities row for the same real person.
    """
    token = f"ZQXCROSS{_uid()}".upper()
    owner_name = f"{token} CROSSCOUNTY BUYER"
    mailing_address = f"500 SENTINEL ST, TAMPA FL 00001"

    owner_ids: list[int] = []
    try:
        with get_db_context() as session:
            _, owner_a = _seed_property_owner(
                session, county_id=_COUNTY_A, owner_name=owner_name,
                mailing_address=mailing_address, parcel_prefix="ZQX-XCTY-A",
            )
            owner_ids.append(owner_a.id)
            run_incremental(session, county_id=_COUNTY_A)

            _, owner_b = _seed_property_owner(
                session, county_id=_COUNTY_B, owner_name=owner_name,
                mailing_address=mailing_address, parcel_prefix="ZQX-XCTY-B",
            )
            owner_ids.append(owner_b.id)
            run_incremental(session, county_id=_COUNTY_B)

            entity_ids = {
                row[0] for row in session.execute(
                    text(
                        "SELECT buyer_entity_id FROM buyer_entity_links "
                        "WHERE source_table = 'owners' AND source_id = ANY(:ids)"
                    ),
                    {"ids": owner_ids},
                ).fetchall()
            }

        assert len(entity_ids) == 1, (
            f"expected one buyer_entities row for the same buyer across two counties, "
            f"got {len(entity_ids)}: {entity_ids}"
        )
    finally:
        with get_db_context() as session:
            _cleanup_buyer_entities_for_links(session, "owners", owner_ids)
            session.execute(delete(Owner).where(Owner.id.in_(owner_ids)))
            session.execute(
                delete(Property).where(Property.parcel_id.like("ZQX-XCTY-%"))
            )
            session.commit()


# ── Issue #2 — corrective re-recording double-counts cash volume ──────────

def test_corrective_deed_does_not_double_count_cash_volume():
    """
    Two deeds rows for the SAME property (a corrective re-recording) must
    contribute exactly one property to purchase_count and exactly one
    sale_price to cash_volume -- not sum both rows' prices.
    """
    entity = None
    deed_ids: list[int] = []
    prop = None
    try:
        with get_db_context() as session:
            prop = Property(parcel_id=f"ZQX-DUPDEED-{_uid()}", address="2 Test Way",
                             city="Tampa", state="FL", zip="00001", county_id=_COUNTY_A)
            session.add(prop)
            session.flush()

            entity = BuyerEntity(
                canonical_name=f"ZQX DUPDEED BUYER {_uid()}", entity_type="Individual",
                confidence_score=100, verification_status="verified", county_id=_COUNTY_A,
            )
            session.add(entity)
            session.flush()

            original = Deed(
                property_id=prop.id, instrument_number=f"ZQX-ORIG-{_uid()}",
                grantee=entity.canonical_name, record_date=date.today() - timedelta(days=10),
                sale_price=300_000, deed_type="Deed", county_id=_COUNTY_A,
            )
            corrective = Deed(
                property_id=prop.id, instrument_number=f"ZQX-CORR-{_uid()}",
                grantee=entity.canonical_name, record_date=date.today() - timedelta(days=3),
                sale_price=300_000, deed_type="(D) DEED", county_id=_COUNTY_A,
            )
            session.add_all([original, corrective])
            session.flush()
            deed_ids = [original.id, corrective.id]

            session.execute(text(
                "INSERT INTO buyer_entity_links (buyer_entity_id, source_table, source_id, "
                "match_confidence, match_method) VALUES (:eid, 'deeds', :did, 100, 'manual')"
            ), {"eid": entity.id, "did": original.id})
            session.execute(text(
                "INSERT INTO buyer_entity_links (buyer_entity_id, source_table, source_id, "
                "match_confidence, match_method) VALUES (:eid, 'deeds', :did, 100, 'manual')"
            ), {"eid": entity.id, "did": corrective.id})
            session.commit()

            refresh_portfolio_aggregates(session, entity_ids=[entity.id])

            # session_scope() runs with expire_on_commit=False (database.py),
            # so the in-memory `entity` object is never invalidated by
            # refresh_portfolio_aggregates' internal commit -- read back via
            # a fresh query, not the stale ORM-cached object.
            purchase_count, cash_volume = session.execute(
                text("SELECT total_purchase_count, total_cash_volume FROM buyer_entities WHERE id = :id"),
                {"id": entity.id},
            ).one()
            assert purchase_count == 1, (
                f"expected 1 distinct property, got {purchase_count}"
            )
            assert float(cash_volume) == 300_000.0, (
                f"expected cash_volume deduped to 300000, got {cash_volume} "
                f"(600000 would mean the corrective row's price got summed a second time)"
            )
    finally:
        with get_db_context() as session:
            if deed_ids:
                session.execute(delete(Deed).where(Deed.id.in_(deed_ids)))
            if entity is not None:
                session.execute(delete(BuyerEntity).where(BuyerEntity.id == entity.id))
            if prop is not None:
                session.execute(delete(Property).where(Property.id == prop.id))
            session.commit()


# ── Issue #3 — interrupted backfill must be resumable ──────────────────────

def test_rerunning_backfill_after_commit_does_not_raise():
    """
    scripts/backfill_buyer_entities.run_backfill, called twice in a row
    against the same county, must not raise a unique-constraint violation
    on the second call (simulating a resume after a completed/interrupted
    prior run) and must not create duplicate entities for already-resolved
    records.
    """
    from scripts.backfill_buyer_entities import run_backfill

    county = f"ztest-hunter-resume-{_uid()}"
    owner_ids: list[int] = []
    parcel_prefix = f"ZQX-RESUME-{_uid()}"
    try:
        with get_db_context() as session:
            _, owner = _seed_property_owner(
                session, county_id=county, owner_name=f"ZQX RESUME BUYER {_uid()}",
                mailing_address="9 Sentinel Ave, Tampa FL 00001", parcel_prefix=parcel_prefix,
            )
            owner_ids.append(owner.id)
            session.commit()

        # First run: everything is unresolved -- resolves normally.
        run_backfill(county_id=county, dry_run=False)

        with get_db_context() as session:
            first_entity_ids = {
                row[0] for row in session.execute(
                    text(
                        "SELECT buyer_entity_id FROM buyer_entity_links "
                        "WHERE source_table = 'owners' AND source_id = ANY(:ids)"
                    ),
                    {"ids": owner_ids},
                ).fetchall()
            }
        assert len(first_entity_ids) == 1, "first backfill run should resolve the seeded owner to one entity"

        # Second run (the "resume" after the DB already has committed work):
        # must NOT raise IntegrityError on buyer_entity_links' unique
        # (source_table, source_id) constraint.
        run_backfill(county_id=county, dry_run=False)

        with get_db_context() as session:
            second_entity_ids = {
                row[0] for row in session.execute(
                    text(
                        "SELECT buyer_entity_id FROM buyer_entity_links "
                        "WHERE source_table = 'owners' AND source_id = ANY(:ids)"
                    ),
                    {"ids": owner_ids},
                ).fetchall()
            }
        assert second_entity_ids == first_entity_ids, (
            "re-running backfill must not create a duplicate entity for an already-resolved owner"
        )
    finally:
        with get_db_context() as session:
            _cleanup_buyer_entities_for_links(session, "owners", owner_ids)
            session.execute(delete(Owner).where(Owner.id.in_(owner_ids)))
            session.execute(delete(Property).where(Property.parcel_id.like(f"{parcel_prefix}%")))
            session.commit()


# ── Issue #4 — kill switch must halt scheduled writers ─────────────────────

def test_nightly_sweep_skips_when_hunter_halted():
    from src.tasks.hunter_nightly_sweep import run_sweep

    with patch("src.tasks.hunter_nightly_sweep.hunter_halted", return_value=True):
        result = run_sweep(county_id=_COUNTY_A)

    assert result.get("halted") is True
    assert "resolution" not in result, "a halted run must skip resolution/aggregation/whale-scoring entirely"


def test_auction_fast_follow_skips_when_hunter_halted():
    from src.connectors.whale_auction_fast_follow import run_whale_fast_follow

    with patch("src.connectors.whale_auction_fast_follow.hunter_halted", return_value=True):
        with get_db_context() as session:
            result = run_whale_fast_follow(session, _COUNTY_A)

    assert result["halted"] is True
    assert result["examined"] == 0
    assert result["rescored_entities"] == 0
