"""
Unit + integration tests for the owner reverse-lookup helper.

Exercises:
  - portfolio_size: exact match, county scoping, empty input
  - portfolio_sizes_for_names: bulk variant, missing names absent from dict
  - properties_by_normalized_owner: trgm sweep + Python-side strict filter,
    suffix variant collapsing
  - llcs_managed_by: JSONB GIN @> probe
"""

from __future__ import annotations

from datetime import datetime

import pytest

from src.core.models import Owner, Property
from src.services.owner_lookup import (
    llcs_managed_by,
    portfolio_size,
    portfolio_sizes_for_names,
    properties_by_normalized_owner,
)


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_owner(
    session, property_id: int, name: str, *,
    owner_type: str = "LLC",
    sunbiz_status: str = "pending",
    managing_members: list | None = None,
) -> Owner:
    o = Owner(
        property_id=property_id,
        owner_name=name,
        owner_type=owner_type,
        sunbiz_status=sunbiz_status,
        managing_members=managing_members,
    )
    session.add(o)
    session.flush()
    return o


@pytest.fixture
def seeded(fresh_db):
    """Seed: 3-property LLC, 1-property LLC, person, all in same county."""
    suffix = str(int(datetime.utcnow().timestamp() * 1000))[-8:]
    p1 = _mk_property(fresh_db, f"A1-{suffix}")
    p2 = _mk_property(fresh_db, f"A2-{suffix}")
    p3 = _mk_property(fresh_db, f"A3-{suffix}")
    p4 = _mk_property(fresh_db, f"B1-{suffix}")
    p5 = _mk_property(fresh_db, f"C1-{suffix}", county_id="pinellas")

    acme_name = f"ACME-{suffix} HOLDINGS LLC"
    other_name = f"BAY-{suffix} VENTURES LLC"
    person_name = f"Smith-{suffix}, John"

    # ACME on p1/p2/p3 — exact-match portfolio = 3
    _mk_owner(fresh_db, p1.id, acme_name)
    _mk_owner(fresh_db, p2.id, acme_name)
    _mk_owner(fresh_db, p3.id, acme_name,
              managing_members=[{"name": person_name, "role": "MGRM"}])
    # Single-property LLC
    _mk_owner(fresh_db, p4.id, other_name)
    # Different-county property w/ same LLC name (for county-scope tests)
    _mk_owner(fresh_db, p5.id, acme_name)

    fresh_db.flush()
    return {
        "acme_name": acme_name,
        "other_name": other_name,
        "person_name": person_name,
        "hillsborough_props": [p1.id, p2.id, p3.id, p4.id],
        "pinellas_prop": p5.id,
    }


# ── portfolio_size ──────────────────────────────────────────────────────────


def test_portfolio_size_exact_match_counts_all_siblings(fresh_db, seeded):
    assert portfolio_size(fresh_db, seeded["acme_name"]) == 4  # 3 + 1 cross-county
    assert portfolio_size(fresh_db, seeded["other_name"]) == 1


def test_portfolio_size_scoped_by_county(fresh_db, seeded):
    assert portfolio_size(fresh_db, seeded["acme_name"], county_id="hillsborough") == 3
    assert portfolio_size(fresh_db, seeded["acme_name"], county_id="pinellas") == 1


def test_portfolio_size_empty_and_none_return_zero(fresh_db):
    assert portfolio_size(fresh_db, None) == 0
    assert portfolio_size(fresh_db, "") == 0


# ── portfolio_sizes_for_names (bulk) ────────────────────────────────────────


def test_portfolio_sizes_for_names_returns_one_query(fresh_db, seeded):
    out = portfolio_sizes_for_names(
        fresh_db,
        [seeded["acme_name"], seeded["other_name"], "NEVER_SEEN_LLC"],
    )
    assert out[seeded["acme_name"]] == 4
    assert out[seeded["other_name"]] == 1
    assert "NEVER_SEEN_LLC" not in out  # caller treats missing as 1


def test_portfolio_sizes_for_names_dedupes_input(fresh_db, seeded):
    out = portfolio_sizes_for_names(
        fresh_db,
        [seeded["acme_name"], seeded["acme_name"], None, ""],
    )
    assert out == {seeded["acme_name"]: 4}


# ── properties_by_normalized_owner ──────────────────────────────────────────


def test_properties_by_normalized_owner_collapses_suffix_variants(fresh_db, seeded):
    # Add a punctuation variant: "ACME-XXXX HOLDINGS, LLC" should normalize to
    # the same form as "ACME-XXXX HOLDINGS LLC".
    base = seeded["acme_name"]
    suffix = str(int(datetime.utcnow().timestamp() * 1000))[-8:]
    extra_prop = _mk_property(fresh_db, f"DUP-{suffix}")
    _mk_owner(fresh_db, extra_prop.id, base.replace(" LLC", ", LLC"))
    fresh_db.flush()

    ids = properties_by_normalized_owner(fresh_db, base)
    # All ACME rows across both punctuation variants AND counties.
    assert len(ids) == 5
    assert extra_prop.id in ids


def test_properties_by_normalized_owner_county_scope(fresh_db, seeded):
    ids = properties_by_normalized_owner(
        fresh_db, seeded["acme_name"], county_id="hillsborough"
    )
    assert seeded["pinellas_prop"] not in ids
    assert all(pid in seeded["hillsborough_props"] for pid in ids)


def test_properties_by_normalized_owner_empty_input(fresh_db):
    assert properties_by_normalized_owner(fresh_db, "") == []


# ── llcs_managed_by ─────────────────────────────────────────────────────────


def test_llcs_managed_by_finds_jsonb_member(fresh_db, seeded):
    rows = llcs_managed_by(fresh_db, seeded["person_name"])
    names = {o.owner_name for o in rows}
    assert seeded["acme_name"] in names
    assert seeded["other_name"] not in names


def test_llcs_managed_by_misses_when_name_differs(fresh_db, seeded):
    assert llcs_managed_by(fresh_db, "Doe, Jane (does not exist)") == []


def test_llcs_managed_by_empty_input(fresh_db):
    assert llcs_managed_by(fresh_db, "") == []
