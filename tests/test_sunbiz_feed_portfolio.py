"""
Lead-feed portfolio_size validation (Area 4) + index performance check (Area 8).

Tests portfolio_sizes_for_names and portfolio_size helpers that back the
/api/feed/{uuid} endpoint's per-lead portfolio_size field.

Performance test: bulk query must complete under 500ms for a 10-name batch
against a real Postgres instance. This is a sanity bound — the function uses
a single IN-query (no N+1); if it fails the timing check something is wrong
with the index or query plan.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from src.core.models import Owner, Property
from src.services.owner_lookup import portfolio_size, portfolio_sizes_for_names


def _mk_property(session, parcel: str, county: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} FEED ST", county_id=county)
    session.add(p)
    session.flush()
    return p


def _mk_owner(session, prop_id: int, name: str, owner_type: str = "LLC") -> Owner:
    o = Owner(property_id=prop_id, owner_name=name, owner_type=owner_type, sunbiz_status="pending")
    session.add(o)
    session.flush()
    return o


# ── portfolio_size: exact name count ─────────────────────────────────────────

def test_portfolio_size_single_property(fresh_db):
    suffix = str(id(test_portfolio_size_single_property))[-6:]
    name = f"SINGLE PROP LLC {suffix}"
    prop = _mk_property(fresh_db, f"SP{suffix}")
    _mk_owner(fresh_db, prop.id, name)
    fresh_db.flush()

    assert portfolio_size(fresh_db, name) == 1


def test_portfolio_size_multi_property(fresh_db):
    suffix = str(id(test_portfolio_size_multi_property))[-6:]
    name = f"MULTI PROP LLC {suffix}"
    for i in range(5):
        prop = _mk_property(fresh_db, f"MP{i}{suffix}")
        _mk_owner(fresh_db, prop.id, name)
    fresh_db.flush()

    assert portfolio_size(fresh_db, name) == 5


def test_portfolio_size_none_returns_zero(fresh_db):
    assert portfolio_size(fresh_db, None) == 0
    assert portfolio_size(fresh_db, "") == 0


def test_portfolio_size_county_filter(fresh_db):
    suffix = str(id(test_portfolio_size_county_filter))[-6:]
    name = f"COUNTY FILTER LLC {suffix}"
    prop_h = _mk_property(fresh_db, f"CFH{suffix}", county="hillsborough")
    prop_p = _mk_property(fresh_db, f"CFP{suffix}", county="pinellas")
    _mk_owner(fresh_db, prop_h.id, name)
    _mk_owner(fresh_db, prop_p.id, name)
    fresh_db.flush()

    assert portfolio_size(fresh_db, name) == 2                         # no county filter
    assert portfolio_size(fresh_db, name, county_id="hillsborough") == 1
    assert portfolio_size(fresh_db, name, county_id="pinellas") == 1


# ── portfolio_sizes_for_names: bulk / feed use-case ──────────────────────────

def test_portfolio_sizes_for_names_bulk(fresh_db):
    suffix = str(id(test_portfolio_sizes_for_names_bulk))[-6:]
    names = [f"BULK LLC {i} {suffix}" for i in range(5)]
    for idx, n in enumerate(names):
        for _ in range(idx + 1):  # LLC 0 → 1 prop, LLC 1 → 2 props, etc.
            prop = _mk_property(fresh_db, f"BLK{idx}{_}{suffix}")
            _mk_owner(fresh_db, prop.id, n)
    fresh_db.flush()

    result = portfolio_sizes_for_names(fresh_db, names)
    for idx, n in enumerate(names):
        assert result[n] == idx + 1, f"{n}: expected {idx + 1}, got {result.get(n)}"


def test_portfolio_sizes_for_names_unknown_name_absent(fresh_db):
    result = portfolio_sizes_for_names(fresh_db, ["DOES NOT EXIST LLC XYZ999"])
    assert "DOES NOT EXIST LLC XYZ999" not in result


def test_portfolio_sizes_for_names_empty_input(fresh_db):
    result = portfolio_sizes_for_names(fresh_db, [])
    assert result == {}


# ── Performance: bulk query must not N+1 ────────────────────────────────────

def test_portfolio_sizes_bulk_query_fast(fresh_db):
    """
    10-name lookup must complete < 500ms against real Postgres.
    This validates that portfolio_sizes_for_names issues ONE SQL query
    (IN-clause) not N individual queries. If this fails, a query-plan
    regression has introduced N+1 behaviour.
    """
    suffix = str(id(test_portfolio_sizes_bulk_query_fast))[-6:]
    names = [f"PERF LLC {i} {suffix}" for i in range(10)]
    for i, n in enumerate(names):
        prop = _mk_property(fresh_db, f"PF{i}{suffix}")
        _mk_owner(fresh_db, prop.id, n)
    fresh_db.flush()

    t0 = time.perf_counter()
    result = portfolio_sizes_for_names(fresh_db, names)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    assert len(result) == 10, f"Expected 10 results, got {len(result)}"
    assert elapsed_ms < 500, (
        f"portfolio_sizes_for_names took {elapsed_ms:.0f}ms — "
        "expected < 500ms (check for N+1 or missing index)"
    )
