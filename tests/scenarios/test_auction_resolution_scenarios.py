"""
Integration tests for HUNTER-05 (auction_resolution), run against the real
Postgres DB (get_db_context / DATABASE_URL) -- same convention as
test_hunter_resolution_fixes.py: every test seeds its own rows under a
sentinel prefix and deletes everything in teardown.
resolve_tax_deed_winners self-commits, so this coverage cannot safely live
under a fresh_db-savepoint-rollback fixture.

Coverage:
  - exactly one existing-entity name match attaches (verified)
  - more than one match is left unresolved (ambiguous), never guessed
  - no match creates one new low-confidence provisional entity
  - the SAME winner appearing twice in one batch (differently formatted)
    dedups to ONE entity, with a link row for EACH auction win
  - a still-unprocessed auction past the 24h SLA is flagged stale;
    'provisional'/'ambiguous' do NOT count as stale (processing != verified)
  - the kill switch halts this connector with no DB mutation
  - a scraper correction to sold_to (via TaxDeedAuctionLoader) after an
    auction was already resolved clears buyer_resolution_status + the old
    buyer_entity_links row, so a rerun re-attaches to the CORRECTED winner
    instead of staying permanently pinned to the old one

Run:
    pytest tests/scenarios/test_auction_resolution_scenarios.py -v -m scenario
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta
from unittest.mock import patch

import pandas as pd
import pytest
from sqlalchemy import delete, text

from src.agents.hunter.auction_resolution import (
    find_stale_unresolved_auctions,
    resolve_tax_deed_winners,
)
from src.core.database import get_db_context
from src.core.models import BuyerEntity, BuyerEntityLink, TaxDeedAuction
from src.loaders.tax_deed import TaxDeedAuctionLoader

pytestmark = pytest.mark.scenario

_COUNTY = "ztest-hunter-auction"


def _uid() -> str:
    return uuid.uuid4().hex[:10]


def _mk_entity(session, canonical_name: str) -> BuyerEntity:
    entity = BuyerEntity(
        canonical_name=canonical_name, entity_type="LLC",
        confidence_score=100, verification_status="verified", county_id=_COUNTY,
    )
    session.add(entity)
    session.flush()
    return entity


def _mk_auction(session, sold_to: str, *, auction_date=None, case_suffix: str = "") -> TaxDeedAuction:
    auction = TaxDeedAuction(
        county_id=_COUNTY, auction_date=auction_date or date.today(),
        case_number=f"ZQX-AUC-{_uid()}{case_suffix}", sold_to=sold_to,
    )
    session.add(auction)
    session.flush()
    return auction


def _cleanup(entity_ids: list[int], auction_ids: list[int]) -> None:
    with get_db_context() as session:
        if auction_ids:
            session.execute(delete(TaxDeedAuction).where(TaxDeedAuction.id.in_(auction_ids)))
        if entity_ids:
            session.execute(delete(BuyerEntity).where(BuyerEntity.id.in_(entity_ids)))
        session.commit()


def test_exact_match_attaches_verified():
    entity = auction = None
    try:
        with get_db_context() as session:
            entity = _mk_entity(session, f"ZQX SUNSHINE PROPERTY GROUP {_uid()}")
            auction = _mk_auction(session, entity.canonical_name)
            session.commit()

            resolve_tax_deed_winners(session, _COUNTY)

            status = session.execute(
                text("SELECT buyer_resolution_status FROM tax_deed_auctions WHERE id = :id"),
                {"id": auction.id},
            ).scalar()
            assert status == "verified"

            link = session.execute(
                text("SELECT buyer_entity_id, match_method FROM buyer_entity_links "
                     "WHERE source_table = 'tax_deed_auctions' AND source_id = :id"),
                {"id": auction.id},
            ).one()
            assert link.buyer_entity_id == entity.id
            assert link.match_method == "exact_name_only"
    finally:
        _cleanup([entity.id] if entity else [], [auction.id] if auction else [])


def test_ambiguous_multiple_matches_left_unresolved():
    entity_a = entity_b = auction = None
    try:
        with get_db_context() as session:
            shared_name = f"ZQX AMBIGUOUS HOLDINGS {_uid()}"
            entity_a = _mk_entity(session, shared_name)
            entity_b = _mk_entity(session, shared_name)
            auction = _mk_auction(session, shared_name)
            session.commit()

            resolve_tax_deed_winners(session, _COUNTY)

            status = session.execute(
                text("SELECT buyer_resolution_status FROM tax_deed_auctions WHERE id = :id"),
                {"id": auction.id},
            ).scalar()
            assert status == "ambiguous"

            link_count = session.execute(
                text("SELECT COUNT(*) FROM buyer_entity_links "
                     "WHERE source_table = 'tax_deed_auctions' AND source_id = :id"),
                {"id": auction.id},
            ).scalar()
            assert link_count == 0, "an ambiguous match must never guess and create a link"
    finally:
        _cleanup([e.id for e in (entity_a, entity_b) if e], [auction.id] if auction else [])


def test_no_match_creates_low_confidence_provisional_entity():
    auction = new_entity_id = None
    try:
        with get_db_context() as session:
            winner_name = f"ZQX BRAND NEW WINNER {_uid()}"
            auction = _mk_auction(session, winner_name)
            session.commit()

            resolve_tax_deed_winners(session, _COUNTY)

            status = session.execute(
                text("SELECT buyer_resolution_status FROM tax_deed_auctions WHERE id = :id"),
                {"id": auction.id},
            ).scalar()
            link = session.execute(
                text("SELECT buyer_entity_id, match_method FROM buyer_entity_links "
                     "WHERE source_table = 'tax_deed_auctions' AND source_id = :id"),
                {"id": auction.id},
            ).one()
            assert status == "provisional"
            assert link.match_method == "auction_name_only_unverified"
            new_entity_id = link.buyer_entity_id

            entity_row = session.execute(
                text("SELECT confidence_score, verification_status FROM buyer_entities WHERE id = :id"),
                {"id": new_entity_id},
            ).one()
            assert entity_row.confidence_score < 70  # gating.UNVERIFIED_FLOOR
            assert entity_row.verification_status == "unverified"
    finally:
        _cleanup([new_entity_id] if new_entity_id else [], [auction.id] if auction else [])


def test_repeated_winner_in_one_batch_dedups_to_one_entity():
    auction_a = auction_b = new_entity_id = None
    try:
        with get_db_context() as session:
            suffix = _uid()
            # two different raw spellings of the same real winner
            auction_a = _mk_auction(session, f"Sunshine Group {suffix}, LLC", case_suffix="A")
            auction_b = _mk_auction(session, f"SUNSHINE GROUP {suffix} LLC", case_suffix="B")
            session.commit()

            resolve_tax_deed_winners(session, _COUNTY)

            entity_ids = {
                row.buyer_entity_id for row in session.execute(
                    text("SELECT buyer_entity_id FROM buyer_entity_links "
                         "WHERE source_table = 'tax_deed_auctions' AND source_id = ANY(:ids)"),
                    {"ids": [auction_a.id, auction_b.id]},
                ).fetchall()
            }
            assert len(entity_ids) == 1, f"expected one entity for the same winner, got {entity_ids}"
            new_entity_id = entity_ids.pop()

            link_count = session.execute(
                text("SELECT COUNT(*) FROM buyer_entity_links "
                     "WHERE source_table = 'tax_deed_auctions' AND source_id = ANY(:ids)"),
                {"ids": [auction_a.id, auction_b.id]},
            ).scalar()
            assert link_count == 2, "each auction win must keep its own traceable link row"
    finally:
        _cleanup([new_entity_id] if new_entity_id else [], [a.id for a in (auction_a, auction_b) if a])


def test_stale_unprocessed_auction_is_flagged():
    auction = None
    try:
        with get_db_context() as session:
            auction = _mk_auction(
                session, f"ZQX STALE WINNER {_uid()}",
                auction_date=date.today() - timedelta(days=3),
            )
            session.commit()
            # deliberately do NOT call resolve_tax_deed_winners -- simulating
            # an auction that has sat unprocessed past the SLA

            stale = find_stale_unresolved_auctions(session, _COUNTY)
            assert any(s["id"] == auction.id for s in stale)
    finally:
        _cleanup([], [auction.id] if auction else [])


def test_provisional_status_does_not_count_as_stale():
    """Reaching 'provisional' satisfies the processing SLA even though the
    identity is unverified -- processing and verification are different
    guarantees."""
    auction = new_entity_id = None
    try:
        with get_db_context() as session:
            auction = _mk_auction(
                session, f"ZQX OLD BUT RESOLVED {_uid()}",
                auction_date=date.today() - timedelta(days=3),
            )
            session.commit()

            resolve_tax_deed_winners(session, _COUNTY)
            link = session.execute(
                text("SELECT buyer_entity_id FROM buyer_entity_links "
                     "WHERE source_table = 'tax_deed_auctions' AND source_id = :id"),
                {"id": auction.id},
            ).one()
            new_entity_id = link.buyer_entity_id

            stale = find_stale_unresolved_auctions(session, _COUNTY)
            assert not any(s["id"] == auction.id for s in stale)
    finally:
        _cleanup([new_entity_id] if new_entity_id else [], [auction.id] if auction else [])


def test_sold_to_correction_resolves_to_new_buyer_not_old():
    """A later re-scrape that CORRECTS tax_deed_auctions.sold_to (via the
    real loader, not a direct DB write) after the auction was already
    resolved must not leave it permanently attributed to the old winner:
    the loader's update must clear buyer_resolution_status + the stale link,
    so rerunning resolve_tax_deed_winners re-attaches to the corrected buyer."""
    entity_a = entity_b = auction = None
    try:
        with get_db_context() as session:
            suffix = _uid()
            entity_a = _mk_entity(session, f"ZQX OLD WINNER {suffix}")
            entity_b = _mk_entity(session, f"ZQX NEW WINNER {suffix}")
            auction = _mk_auction(session, entity_a.canonical_name)
            session.commit()

            resolve_tax_deed_winners(session, _COUNTY)
            link = session.execute(
                text("SELECT buyer_entity_id FROM buyer_entity_links "
                     "WHERE source_table = 'tax_deed_auctions' AND source_id = :id"),
                {"id": auction.id},
            ).one()
            assert link.buyer_entity_id == entity_a.id

            # Scraper correction: a later re-scrape of the same case reports a
            # different winner. Goes through the real loader, not a raw UPDATE,
            # since this is the exact path the review found broken.
            loader = TaxDeedAuctionLoader(session, county_id=_COUNTY)
            df = pd.DataFrame([{
                "parcel_id": "", "case_number": auction.case_number,
                "auction_date": auction.auction_date.strftime("%m/%d/%Y"),
                "certificate_number": "", "certificate_year": "",
                "status": "", "auction_type": "", "opening_bid": "",
                "sold_amount": "", "sold_to": entity_b.canonical_name, "raw_fields": "",
            }])
            loader.load_from_dataframe(df)
            session.commit()

            corrected = session.execute(
                text("SELECT buyer_resolution_status, sold_to FROM tax_deed_auctions WHERE id = :id"),
                {"id": auction.id},
            ).one()
            assert corrected.sold_to == entity_b.canonical_name
            assert corrected.buyer_resolution_status is None, \
                "status must be reset so the resolver re-examines this row"

            stale_link_count = session.execute(
                text("SELECT COUNT(*) FROM buyer_entity_links "
                     "WHERE source_table = 'tax_deed_auctions' AND source_id = :id"),
                {"id": auction.id},
            ).scalar()
            assert stale_link_count == 0, "the old link to entity_a must be cleared, not left dangling"

            resolve_tax_deed_winners(session, _COUNTY)

            links_after = session.execute(
                text("SELECT buyer_entity_id FROM buyer_entity_links "
                     "WHERE source_table = 'tax_deed_auctions' AND source_id = :id"),
                {"id": auction.id},
            ).fetchall()
            assert len(links_after) == 1, "must not end up linked to both the old and new buyer"
            assert links_after[0].buyer_entity_id == entity_b.id
    finally:
        _cleanup([e.id for e in (entity_a, entity_b) if e], [auction.id] if auction else [])


def test_kill_switch_halts_with_no_mutation():
    auction = None
    try:
        with get_db_context() as session:
            auction = _mk_auction(session, f"ZQX HALTED WINNER {_uid()}")
            session.commit()

            with patch("src.agents.hunter.auction_resolution.hunter_halted", return_value=True):
                result = resolve_tax_deed_winners(session, _COUNTY)

            assert result["halted"] is True
            assert result["examined"] == 0

            status = session.execute(
                text("SELECT buyer_resolution_status FROM tax_deed_auctions WHERE id = :id"),
                {"id": auction.id},
            ).scalar()
            assert status is None
    finally:
        _cleanup([], [auction.id] if auction else [])
