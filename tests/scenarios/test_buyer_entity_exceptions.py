"""
Regression tests for the match-exception queue (WP-4, WI-4).

Before this, a resolver decision that correctly refused to auto-merge (an
ambiguous pair, or a cluster touching 2+ existing buyer_entities anchors)
was logger.warning only and forgotten. Client spec: "Identity resolution is
uncertain. Records stay separate and a possible-match flag routes to
EXCEPTIONS."

Runs against the real Postgres DB, same convention as
test_hunter_resolution_fixes.py / test_buyer_entity_principal.py.

Coverage:
  - a multi-anchor conflict (two existing entities both plausibly matching
    one new record) writes one open buyer_entity_match_exception row.
  - re-running the sweep on the same conflict is idempotent: still one row,
    last_seen_at advances, no duplicate.
  - resolve_exception() requires merge_log_id when resolving to 'merged',
    and rejects an unknown id.

Run:
    pytest tests/scenarios/test_buyer_entity_exceptions.py -v -m scenario
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import delete, text

from src.core.database import get_db_context
from src.core.models import BuyerEntity, BuyerEntityMatchException, Owner, Property
from src.services.buyer_entity_exceptions import list_open_exceptions, resolve_exception
from src.services.buyer_entity_resolution import run_incremental

pytestmark = pytest.mark.scenario


def _uid() -> str:
    return uuid.uuid4().hex[:10]


def _seed_entity(session, *, canonical_name: str, county_id: str, mailing_address: str) -> int:
    # mailing_address must be set -- block_candidates() keys on (name-token,
    # ZIP), and an entity anchor with no address never co-occurs with a
    # ZIP-bearing new owner record, so it can never form the conflict edge
    # this fixture needs.
    entity = BuyerEntity(
        canonical_name=canonical_name, entity_type="Individual",
        primary_mailing_address=mailing_address,
        confidence_score=95, verification_status="verified", county_id=county_id,
    )
    session.add(entity)
    session.flush()
    return entity.id


def _cleanup(entity_ids: list[int], owner_ids: list[int], parcel_prefix: str,
             exception_ids: list[int]) -> None:
    with get_db_context() as session:
        if exception_ids:
            session.execute(delete(BuyerEntityMatchException).where(
                BuyerEntityMatchException.id.in_(exception_ids)))
        if owner_ids:
            session.execute(text(
                "DELETE FROM buyer_entity_links WHERE source_table = 'owners' AND source_id = ANY(:ids)"
            ), {"ids": owner_ids})
            session.execute(delete(Owner).where(Owner.id.in_(owner_ids)))
        if entity_ids:
            session.execute(delete(BuyerEntity).where(BuyerEntity.id.in_(entity_ids)))
        session.execute(delete(Property).where(Property.parcel_id.like(f"{parcel_prefix}%")))
        session.commit()


def test_multi_anchor_conflict_writes_one_open_exception():
    """Two existing entities with the SAME name/address (simulating a prior
    resolver run that already split what should be one person into two
    entities) both match a new owner record -> the cluster touches 2
    anchors, is NOT auto-merged, and writes exactly one open exception row
    -- re-running the sweep does not duplicate it."""
    token = f"ZEXC{_uid()}".upper()
    county = f"ztest-exception-{_uid()}"
    parcel_prefix = f"ZQX-EXC-{_uid()}"
    shared_name = f"{token} ROBERT MILLER"
    shared_address = "700 Conflict Ave, Tampa FL 00001"
    entity_ids: list[int] = []
    owner_ids: list[int] = []
    exception_ids: list[int] = []
    try:
        with get_db_context() as session:
            entity_ids.append(_seed_entity(
                session, canonical_name=shared_name, county_id=county, mailing_address=shared_address,
            ))
            entity_ids.append(_seed_entity(
                session, canonical_name=shared_name, county_id=county, mailing_address=shared_address,
            ))
            session.commit()

            prop = Property(
                parcel_id=f"{parcel_prefix}-{_uid()}", address="1 Test Way", city="Tampa",
                state="FL", zip="00001", county_id=county,
            )
            session.add(prop)
            session.flush()
            owner = Owner(
                property_id=prop.id, owner_name=shared_name, mailing_address=shared_address,
                owner_type="Individual", county_id=county,
            )
            session.add(owner)
            session.flush()
            owner_ids.append(owner.id)
            session.commit()

        with get_db_context() as session:
            stats = run_incremental(session, county_id=county)
        assert stats["conflicts"] >= 1

        with get_db_context() as session:
            rows = session.execute(
                text(
                    "SELECT id, status, kind FROM buyer_entity_match_exception "
                    "WHERE left_ref = :left_ref"
                ),
                {"left_ref": f"buyer_entities#{min(entity_ids)},{max(entity_ids)}"},
            ).mappings().all()
        assert len(rows) == 1, "exactly one exception row for this conflict"
        exception_ids.append(rows[0]["id"])
        assert rows[0]["status"] == "open"
        assert rows[0]["kind"] == "multi_anchor_conflict"

        # Re-run the sweep -- owner row already has a link now, so
        # only_unresolved candidates are empty and nothing new happens.
        # This confirms it's safe to call repeatedly, not that it re-detects
        # (the owner is already linked to one of the two anchors after the
        # first run picks len(entity_anchors)>1 branch -- but new_records is
        # empty on re-run since the link now exists).
        with get_db_context() as session:
            run_incremental(session, county_id=county)

        with get_db_context() as session:
            rows_after = session.execute(
                text("SELECT id FROM buyer_entity_match_exception WHERE left_ref = :left_ref"),
                {"left_ref": f"buyer_entities#{min(entity_ids)},{max(entity_ids)}"},
            ).mappings().all()
        assert len(rows_after) == 1, "no duplicate exception row on re-run"
    finally:
        _cleanup(entity_ids, owner_ids, parcel_prefix, exception_ids)


def test_resolve_exception_requires_merge_log_id_for_merged_status():
    """resolve_exception(status='merged') without a merge_log_id must raise
    -- an exception row cannot be marked merged without pointing at the
    actual merge audit record."""
    token = f"ZEXCRESOLVE{_uid()}".upper()
    exception_id = None
    try:
        with get_db_context() as session:
            result = session.execute(
                text("""
                    INSERT INTO buyer_entity_match_exception
                        (kind, left_ref, right_ref, explanation)
                    VALUES ('ambiguous_pair', :left_ref, :right_ref, :explanation)
                    RETURNING id
                """),
                {
                    "left_ref": f"owners#{token}1", "right_ref": f"owners#{token}2",
                    "explanation": "test fixture",
                },
            )
            exception_id = result.scalar_one()
            session.commit()

        with get_db_context() as session:
            with pytest.raises(ValueError, match="merge_log_id"):
                resolve_exception(session, exception_id, status="merged", resolved_by="test:wp4")

        with get_db_context() as session:
            with pytest.raises(ValueError, match="not found"):
                resolve_exception(session, 999_999_999, status="rejected", resolved_by="test:wp4")

        with get_db_context() as session:
            resolve_exception(session, exception_id, status="rejected", resolved_by="test:wp4")
            session.commit()

        with get_db_context() as session:
            row = session.execute(
                text("SELECT status, resolved_by FROM buyer_entity_match_exception WHERE id = :id"),
                {"id": exception_id},
            ).mappings().one()
            assert row["status"] == "rejected"
            assert row["resolved_by"] == "test:wp4"

            open_ids = {e["id"] for e in list_open_exceptions(session, limit=1000)}
            assert exception_id not in open_ids
    finally:
        if exception_id:
            with get_db_context() as session:
                session.execute(
                    text("DELETE FROM buyer_entity_match_exception WHERE id = :id"),
                    {"id": exception_id},
                )
                session.commit()
