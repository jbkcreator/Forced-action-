"""
Integration tests for `scripts.backfill_sunbiz` — specifically the
representative-scrape → sibling-fanout step, and the active-lead-first
ordering.

The Playwright/Sunbiz network leg is out of scope here; tests stub the
representative scrape by directly populating fa031 columns on the chosen
representative Owner and asserting fanout copies them to siblings.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from src.core.models import DistressScore, Owner, Property
from scripts.backfill_sunbiz import (
    _active_lead_names,
    _fanout_to_siblings,
    _group_pending_by_normalized_name,
)
from src.services.owner_lookup import _normalize


def _mk_property(session, parcel: str, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_llc_owner(session, property_id: int, name: str, *, status: str = "pending") -> Owner:
    o = Owner(
        property_id=property_id,
        owner_name=name,
        owner_type="LLC",
        sunbiz_status=status,
    )
    session.add(o)
    session.flush()
    return o


def test_group_pending_buckets_by_normalized_name(fresh_db):
    suffix = str(int(datetime.utcnow().timestamp() * 1000))[-8:]
    base = f"ACMEBF-{suffix} HOLDINGS LLC"
    p1, p2, p3 = (_mk_property(fresh_db, f"X{i}-{suffix}") for i in range(3))
    o1 = _mk_llc_owner(fresh_db, p1.id, base)
    o2 = _mk_llc_owner(fresh_db, p2.id, base.replace(" LLC", ", LLC"))
    o3 = _mk_llc_owner(fresh_db, p3.id, base, status="matched")  # not pending
    fresh_db.flush()

    buckets = _group_pending_by_normalized_name(fresh_db, "hillsborough")
    norm = _normalize(base)
    assert norm in buckets
    bucket_ids = {o.id for o in buckets[norm]}
    assert o1.id in bucket_ids
    assert o2.id in bucket_ids
    assert o3.id not in bucket_ids  # already matched


def test_fanout_copies_fa031_columns_to_siblings(fresh_db):
    suffix = str(int(datetime.utcnow().timestamp() * 1000))[-8:]
    base = f"ACMEFAN-{suffix} HOLDINGS LLC"
    p1, p2, p3 = (_mk_property(fresh_db, f"F{i}-{suffix}") for i in range(3))
    rep = _mk_llc_owner(fresh_db, p1.id, base)
    sib1 = _mk_llc_owner(fresh_db, p2.id, base)
    sib2 = _mk_llc_owner(fresh_db, p3.id, base.replace(" LLC", ", LLC"))
    fresh_db.flush()

    # Simulate a successful scrape on the representative.
    rep.sunbiz_doc_number = f"L99{suffix}"
    rep.principal_address = "123 PRINCIPAL ST\nTAMPA, FL 33602"
    rep.registered_agent_name = "AGENT, A"
    rep.registered_agent_address = "456 AGENT WAY\nTAMPA, FL 33606"
    rep.registered_agent_email = "agent@example.com"
    rep.entity_status = "ACTIVE"
    rep.formation_date = date(2022, 1, 15)
    rep.managing_members = [{"name": "SMITH, JOHN", "role": "MGRM"}]
    rep.sunbiz_status = "matched"
    fresh_db.flush()

    touched = _fanout_to_siblings(fresh_db, rep, [rep, sib1, sib2])
    fresh_db.flush()

    assert touched == 2
    for sib in (sib1, sib2):
        fresh_db.refresh(sib)
        assert sib.sunbiz_doc_number == rep.sunbiz_doc_number
        assert sib.sunbiz_status == "matched"
        assert sib.registered_agent_email == "agent@example.com"
        assert sib.managing_members == rep.managing_members
        assert sib.sunbiz_enriched_at is not None


def test_fanout_skips_when_rep_still_pending(fresh_db):
    """No data to copy if the representative scrape didn't actually run."""
    suffix = str(int(datetime.utcnow().timestamp() * 1000))[-8:]
    base = f"NOSCRAPE-{suffix} LLC"
    p1, p2 = (_mk_property(fresh_db, f"N{i}-{suffix}") for i in range(2))
    rep = _mk_llc_owner(fresh_db, p1.id, base)
    sib = _mk_llc_owner(fresh_db, p2.id, base)
    fresh_db.flush()

    touched = _fanout_to_siblings(fresh_db, rep, [rep, sib])
    fresh_db.flush()
    fresh_db.refresh(sib)

    assert touched == 0
    assert sib.sunbiz_status == "pending"  # unchanged
    assert sib.sunbiz_doc_number is None


def test_active_lead_names_set_built_from_high_tier_scores(fresh_db):
    suffix = str(int(datetime.utcnow().timestamp() * 1000))[-8:]
    active_name = f"HOTLEAD-{suffix} LLC"
    cold_name = f"COLDLEAD-{suffix} LLC"
    p_active = _mk_property(fresh_db, f"H-{suffix}")
    p_cold = _mk_property(fresh_db, f"C-{suffix}")
    _mk_llc_owner(fresh_db, p_active.id, active_name)
    _mk_llc_owner(fresh_db, p_cold.id, cold_name)
    fresh_db.add(
        DistressScore(
            property_id=p_active.id,
            final_cds_score=88.0,
            lead_tier="Platinum",
        )
    )
    fresh_db.add(
        DistressScore(
            property_id=p_cold.id,
            final_cds_score=42.0,
            lead_tier="Bronze",
        )
    )
    fresh_db.flush()

    active = _active_lead_names(fresh_db, "hillsborough")
    assert _normalize(active_name) in active
    assert _normalize(cold_name) not in active
