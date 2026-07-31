"""
Integration tests for HUNTER-03 (buyer_type_classification) and HUNTER-04
(portfolio_profiling), run against the real Postgres DB (get_db_context /
DATABASE_URL) -- same convention as test_hunter_resolution_fixes.py: every
test seeds its own rows under a sentinel prefix and deletes everything in
teardown. refresh_portfolio_profiling/classify_buyer_types self-commit
(matching whale_detection.refresh_whale_flags's own convention), so this
coverage cannot safely live under a fresh_db-savepoint-rollback fixture --
the rollback would be a no-op against an already-committed write.

Coverage:
  - a flipper (fast acquisition -> resale pair) classifies correctly end to
    end, with portfolio_evidence/buyer_type_evidence both persisted
  - a normal investor with NO distressed-acquisition history still gets
    classified -- proves H3 reads full purchase history, not a
    distressed-only subset (the original design bug this plan's first
    review round caught)
  - a mortgage-type deed recorded between an acquisition and its real exit
    is skipped over, not mistaken for the exit itself
  - a still-held (no exit) acquisition past the flip window classifies as
    buy-and-hold evidence, not silently dropped
  - refresh_portfolio_profiling/classify_buyer_types scoped to entity_ids
    never touches an entity outside that scope

Run:
    pytest tests/scenarios/test_hunter_profiling_scenarios.py -v -m scenario
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import delete, text

from src.agents.hunter.buyer_type_classification import classify_buyer_types
from src.agents.hunter.portfolio_profiling import FLIP_MAX_HOLD_DAYS, refresh_portfolio_profiling
from src.core.database import get_db_context
from src.core.models import BuyerEntity, BuyerEntityLink, Deed, Property

pytestmark = pytest.mark.scenario

_COUNTY = "ztest-hunter-profiling"


def _uid() -> str:
    return uuid.uuid4().hex[:10]


def _mk_entity(session, *, total_purchase_count: int = 1) -> BuyerEntity:
    entity = BuyerEntity(
        canonical_name=f"ZQX PROFILING BUYER {_uid()}", entity_type="Individual",
        confidence_score=100, verification_status="verified", county_id=_COUNTY,
        total_purchase_count=total_purchase_count,
    )
    session.add(entity)
    session.flush()
    return entity


def _mk_property(session, prefix: str) -> Property:
    prop = Property(
        parcel_id=f"{prefix}-{_uid()}", address="1 Test Way", city="Tampa",
        state="FL", zip="00001", county_id=_COUNTY,
    )
    session.add(prop)
    session.flush()
    return prop


def _mk_deed(session, property_id, instrument_number, **overrides) -> Deed:
    defaults = dict(
        property_id=property_id, county_id=_COUNTY, instrument_number=instrument_number,
        record_date=date(2026, 1, 1), sale_price=100_000, deed_type="Warranty Deed",
        grantor="SELLER", grantee="BUYER",
    )
    defaults.update(overrides)
    d = Deed(**defaults)
    session.add(d)
    session.flush()
    return d


def _link(session, entity_id: int, deed_id: int) -> None:
    session.execute(text(
        "INSERT INTO buyer_entity_links (buyer_entity_id, source_table, source_id, "
        "match_confidence, match_method) VALUES (:eid, 'deeds', :did, 100, 'manual')"
    ), {"eid": entity_id, "did": deed_id})


def _cleanup(entity_ids: list[int], deed_ids: list[int], property_ids: list[int]) -> None:
    with get_db_context() as session:
        if deed_ids:
            session.execute(delete(Deed).where(Deed.id.in_(deed_ids)))
        if entity_ids:
            session.execute(delete(BuyerEntity).where(BuyerEntity.id.in_(entity_ids)))
        if property_ids:
            session.execute(delete(Property).where(Property.id.in_(property_ids)))
        session.commit()


def test_flipper_classifies_end_to_end():
    entity = deed_a = deed_b = prop = None
    try:
        with get_db_context() as session:
            entity = _mk_entity(session, total_purchase_count=1)
            prop = _mk_property(session, "ZQX-FLIP")
            deed_a = _mk_deed(
                session, prop.id, f"ZQX-FLIP-ACQ-{_uid()}",
                record_date=date(2026, 1, 1), sale_price=150_000,
            )
            deed_b = _mk_deed(
                session, prop.id, f"ZQX-FLIP-RESALE-{_uid()}",
                record_date=date(2026, 3, 1), sale_price=200_000,
            )
            _link(session, entity.id, deed_a.id)
            session.commit()

            profiled = refresh_portfolio_profiling(session, entity_ids=[entity.id])
            assert profiled == 1
            classified = classify_buyer_types(session, entity_ids=[entity.id])
            assert classified == 1

            row = session.execute(
                text("SELECT buyer_type, avg_hold_days, portfolio_evidence, buyer_type_evidence "
                     "FROM buyer_entities WHERE id = :id"),
                {"id": entity.id},
            ).one()
            assert row.buyer_type == "flipper"
            assert row.avg_hold_days == (date(2026, 3, 1) - date(2026, 1, 1)).days
            assert row.portfolio_evidence["exit_within_730_days"] == 1
            assert row.buyer_type_evidence["flip_count"] == 1
    finally:
        _cleanup(
            [entity.id] if entity else [],
            [d.id for d in (deed_a, deed_b) if d],
            [prop.id] if prop else [],
        )


def test_normal_investor_without_distressed_history_still_classifies():
    """Plain 'Warranty Deed' acquisition + resale, no Certificate of
    Title/Tax Deed/Sheriff's Deed anywhere -- must still classify."""
    entity = deed_a = deed_b = prop = None
    try:
        with get_db_context() as session:
            entity = _mk_entity(session)
            prop = _mk_property(session, "ZQX-NORMAL")
            deed_a = _mk_deed(session, prop.id, f"ZQX-NORMAL-ACQ-{_uid()}",
                               deed_type="Warranty Deed", record_date=date(2026, 1, 1))
            deed_b = _mk_deed(session, prop.id, f"ZQX-NORMAL-RESALE-{_uid()}",
                               deed_type="Warranty Deed", record_date=date(2026, 2, 1))
            _link(session, entity.id, deed_a.id)
            session.commit()

            refresh_portfolio_profiling(session, entity_ids=[entity.id])
            classify_buyer_types(session, entity_ids=[entity.id])

            buyer_type = session.execute(
                text("SELECT buyer_type FROM buyer_entities WHERE id = :id"), {"id": entity.id},
            ).scalar()
            assert buyer_type is not None
    finally:
        _cleanup([entity.id] if entity else [], [d.id for d in (deed_a, deed_b) if d], [prop.id] if prop else [])


def test_mortgage_row_between_acquisition_and_exit_is_not_treated_as_exit():
    entity = deed_a = deed_mortgage = deed_b = prop = None
    try:
        with get_db_context() as session:
            entity = _mk_entity(session)
            prop = _mk_property(session, "ZQX-MTG")
            deed_a = _mk_deed(session, prop.id, f"ZQX-MTG-ACQ-{_uid()}", record_date=date(2026, 1, 1))
            deed_mortgage = _mk_deed(
                session, prop.id, f"ZQX-MTG-MORTGAGE-{_uid()}",
                deed_type="Mortgage", record_date=date(2026, 1, 5), sale_price=None,
            )
            deed_b = _mk_deed(session, prop.id, f"ZQX-MTG-RESALE-{_uid()}", record_date=date(2026, 6, 1))
            _link(session, entity.id, deed_a.id)
            session.commit()

            refresh_portfolio_profiling(session, entity_ids=[entity.id])

            row = session.execute(
                text("SELECT avg_hold_days, financing_signal FROM buyer_entities WHERE id = :id"),
                {"id": entity.id},
            ).one()
            assert row.avg_hold_days == (date(2026, 6, 1) - date(2026, 1, 1)).days
            assert row.financing_signal == "financed"
    finally:
        _cleanup(
            [entity.id] if entity else [],
            [d.id for d in (deed_a, deed_mortgage, deed_b) if d],
            [prop.id] if prop else [],
        )


def test_still_held_past_window_is_buy_and_hold():
    entity = deed_a = prop = None
    try:
        with get_db_context() as session:
            entity = _mk_entity(session)
            prop = _mk_property(session, "ZQX-HOLD")
            deed_a = _mk_deed(
                session, prop.id, f"ZQX-HOLD-ACQ-{_uid()}",
                record_date=date.today() - timedelta(days=FLIP_MAX_HOLD_DAYS + 60),
            )
            _link(session, entity.id, deed_a.id)
            session.commit()

            refresh_portfolio_profiling(session, entity_ids=[entity.id])
            classify_buyer_types(session, entity_ids=[entity.id])

            row = session.execute(
                text("SELECT buyer_type, portfolio_evidence FROM buyer_entities WHERE id = :id"),
                {"id": entity.id},
            ).one()
            assert row.buyer_type == "buy-and-hold"
            assert row.portfolio_evidence["still_held_past_730_days"] == 1
    finally:
        _cleanup([entity.id] if entity else [], [deed_a.id] if deed_a else [], [prop.id] if prop else [])


def test_entity_ids_scope_does_not_touch_other_entities():
    entity_in_scope = entity_out_of_scope = deed_a = deed_b = deed_c = prop_a = prop_b = None
    try:
        with get_db_context() as session:
            entity_in_scope = _mk_entity(session)
            entity_out_of_scope = _mk_entity(session)
            prop_a = _mk_property(session, "ZQX-SCOPE-A")
            prop_b = _mk_property(session, "ZQX-SCOPE-B")
            deed_a = _mk_deed(session, prop_a.id, f"ZQX-SCOPE-A-ACQ-{_uid()}", record_date=date(2026, 1, 1))
            deed_b = _mk_deed(session, prop_a.id, f"ZQX-SCOPE-A-RESALE-{_uid()}", record_date=date(2026, 2, 1))
            deed_c = _mk_deed(session, prop_b.id, f"ZQX-SCOPE-B-ACQ-{_uid()}", record_date=date(2026, 1, 1))
            _link(session, entity_in_scope.id, deed_a.id)
            _link(session, entity_out_of_scope.id, deed_c.id)
            session.commit()

            refresh_portfolio_profiling(session, entity_ids=[entity_in_scope.id])

            in_scope_profiled = session.execute(
                text("SELECT portfolio_profiled_at FROM buyer_entities WHERE id = :id"),
                {"id": entity_in_scope.id},
            ).scalar()
            out_of_scope_profiled = session.execute(
                text("SELECT portfolio_profiled_at FROM buyer_entities WHERE id = :id"),
                {"id": entity_out_of_scope.id},
            ).scalar()
            assert in_scope_profiled is not None
            assert out_of_scope_profiled is None
    finally:
        _cleanup(
            [e.id for e in (entity_in_scope, entity_out_of_scope) if e],
            [d.id for d in (deed_a, deed_b, deed_c) if d],
            [p.id for p in (prop_a, prop_b) if p],
        )


def test_empty_entity_ids_is_a_no_op_not_full_table():
    """entity_ids=[] must never fall through to full-table scope."""
    with get_db_context() as session:
        assert refresh_portfolio_profiling(session, entity_ids=[]) == 0
        assert classify_buyer_types(session, entity_ids=[]) == 0
