"""
Regression test for the LLC-only-cluster principal-naming gap (WP-4, WI-3).

Before the fix, a cluster of LLCs sharing a common managing member (Sunbiz
LLC-piercing) resolved correctly to ONE buyer_entities row, but that row's
canonical_name fell back to whichever LLC name in the cluster happened to be
longest -- the controlling PERSON's name was already known from the piercing
edge but was never carried into cluster assembly. The client's spec is
explicit: a borrower who owns twelve houses through nine LLCs "must resolve
to one person with twelve properties."

Runs against the real Postgres DB via run_backfill, same convention as
test_hunter_resolution_fixes.py.

Coverage:
  - two DIFFERENT LLCs, no direct Individual/Trust owner anywhere, sharing
    one managing member -> ONE buyer_entities row, principal_name set to
    the managing member, canonical_name is the PERSON's name (not an LLC
    name), entity_type stays 'LLC'.
  - an LLC-only cluster with NO pierced principal (single LLC, no shared
    member) keeps prior behavior: canonical_name falls back to the LLC's
    own name, principal_name is NULL.

Run:
    pytest tests/scenarios/test_buyer_entity_principal.py -v -m scenario
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import delete, text

from scripts.backfill_buyer_entities import run_backfill
from src.core.database import get_db_context
from src.core.models import BuyerEntity, Owner, Property

pytestmark = pytest.mark.scenario


def _uid() -> str:
    return uuid.uuid4().hex[:10]


def _seed_llc_owner(session, *, county_id: str, llc_name: str, member_name: str,
                     mailing_address: str, parcel_prefix: str) -> Owner:
    prop = Property(
        parcel_id=f"{parcel_prefix}-{_uid()}", address="1 Test Way", city="Tampa",
        state="FL", zip="00001", county_id=county_id,
    )
    session.add(prop)
    session.flush()

    owner = Owner(
        property_id=prop.id, owner_name=llc_name, mailing_address=mailing_address,
        owner_type="LLC", county_id=county_id,
        managing_members=[{"name": member_name, "role": "MGR"}],
    )
    session.add(owner)
    session.flush()
    return owner


def _cleanup(owner_ids: list[int], parcel_prefix: str) -> None:
    with get_db_context() as session:
        if owner_ids:
            entity_ids = [
                row[0] for row in session.execute(
                    text(
                        "SELECT DISTINCT buyer_entity_id FROM buyer_entity_links "
                        "WHERE source_table = 'owners' AND source_id = ANY(:ids)"
                    ),
                    {"ids": owner_ids},
                ).fetchall()
            ]
            if entity_ids:
                session.execute(delete(BuyerEntity).where(BuyerEntity.id.in_(entity_ids)))
            session.execute(delete(Owner).where(Owner.id.in_(owner_ids)))
        session.execute(delete(Property).where(Property.parcel_id.like(f"{parcel_prefix}%")))
        session.commit()


def test_llc_only_cluster_resolves_to_principal_name():
    """Two different LLCs, no Individual/Trust owner anywhere, same managing
    member -> one entity, principal_name = the member, canonical_name is the
    person's name (not either LLC's name), entity_type stays 'LLC'."""
    token = f"ZPRINCIPAL{_uid()}".upper()
    county = f"ztest-principal-{_uid()}"
    parcel_prefix = f"ZQX-PRIN-{_uid()}"
    member_name = f"FLAIG {token} SUSAN"
    owner_ids: list[int] = []
    try:
        with get_db_context() as session:
            owner_a = _seed_llc_owner(
                session, county_id=county, llc_name=f"{token} BARNZ WEST LLC",
                member_name=member_name, mailing_address="100 Investor Way, Tampa FL 00001",
                parcel_prefix=parcel_prefix,
            )
            owner_b = _seed_llc_owner(
                session, county_id=county, llc_name=f"{token} BARNZ LLC",
                member_name=member_name, mailing_address="200 Investor Way, Tampa FL 00001",
                parcel_prefix=parcel_prefix,
            )
            owner_ids.extend([owner_a.id, owner_b.id])
            session.commit()

        run_backfill(county_id=county, dry_run=False)

        with get_db_context() as session:
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
                "two LLCs sharing a managing member must resolve to ONE entity"
            )

            entity = session.execute(
                text("SELECT canonical_name, entity_type, principal_name FROM buyer_entities WHERE id = :id"),
                {"id": next(iter(entity_ids))},
            ).mappings().one()
            assert entity["principal_name"] == member_name
            assert entity["canonical_name"] == member_name, (
                "canonical_name must be the PERSON's name, not an LLC name, "
                "when the cluster has no Individual/Trust owner but a known principal"
            )
            assert entity["entity_type"] == "LLC", (
                "entity_type describes HOW the property is held and must stay 'LLC'"
            )
    finally:
        _cleanup(owner_ids, parcel_prefix)


def test_unpierced_llc_cluster_keeps_llc_name():
    """A single LLC with no shared managing member (nothing to pierce
    against) keeps the prior fallback behavior: canonical_name is the LLC's
    own name, principal_name is NULL."""
    token = f"ZNOPRINCIPAL{_uid()}".upper()
    county = f"ztest-noprincipal-{_uid()}"
    parcel_prefix = f"ZQX-NOPRIN-{_uid()}"
    llc_name = f"{token} SOLO INVESTMENTS LLC"
    owner_ids: list[int] = []
    try:
        with get_db_context() as session:
            prop = Property(
                parcel_id=f"{parcel_prefix}-{_uid()}", address="1 Test Way", city="Tampa",
                state="FL", zip="00001", county_id=county,
            )
            session.add(prop)
            session.flush()
            owner = Owner(
                property_id=prop.id, owner_name=llc_name,
                mailing_address="300 Solo Way, Tampa FL 00001",
                owner_type="LLC", county_id=county,
                managing_members=[{"name": f"{token} UNIQUE MEMBER ONLY", "role": "MGR"}],
            )
            session.add(owner)
            session.flush()
            owner_ids.append(owner.id)
            session.commit()

        run_backfill(county_id=county, dry_run=False)

        with get_db_context() as session:
            entity_id = session.execute(
                text(
                    "SELECT buyer_entity_id FROM buyer_entity_links "
                    "WHERE source_table = 'owners' AND source_id = :id"
                ),
                {"id": owner_ids[0]},
            ).scalar_one()

            entity = session.execute(
                text("SELECT canonical_name, principal_name FROM buyer_entities WHERE id = :id"),
                {"id": entity_id},
            ).mappings().one()
            assert entity["principal_name"] is None
            assert entity["canonical_name"] == llc_name
    finally:
        _cleanup(owner_ids, parcel_prefix)
