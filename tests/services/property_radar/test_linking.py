"""Tests for the PropertyRadar -> FA property linker.

Seeds properties under a fake county slug mapped to a fake FIPS, so no live
county data is involved. Real Postgres via fresh_db (rolled back).
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import text

from src.core.models import Property
from src.services.property_radar import linking
from src.services.property_radar.linking import link_unlinked
from src.services.property_radar.staging import upsert_records

FAKE_FIPS = "99001"
FAKE_SLUG = "test-pr-county"
OTHER_FIPS = "99002"
OTHER_SLUG = "test-pr-other"


@pytest.fixture(autouse=True)
def _fake_counties(monkeypatch):
    monkeypatch.setattr(linking, "COUNTY_FIPS_TO_SLUG", {FAKE_FIPS: FAKE_SLUG, OTHER_FIPS: OTHER_SLUG})


def _property(session, *, parcel_id: str, county_id: str = FAKE_SLUG, address: str = "456 LINK TEST ST") -> Property:
    p = Property(parcel_id=parcel_id, address=address, county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _rec(n: int = 1, **overrides) -> dict:
    base = {
        "radar_id": f"TEST-PR-LINK-{n:03d}",
        "state_fips": "12",
        "county_fips": FAKE_FIPS,
        "apn": f"TEST-PR-LINK-APN-{n:03d}",
        "state": "FL",
        "county_name": "TEST COUNTY",
        "property_address": None,
        "city": "TAMPA",
        "zip": None,
        "owner_name": "LINK OWNER LLC",
        "campaign": "maturity_target_lender",
        "raw": {},
    }
    base.update(overrides)
    return base


def _link(session, radar_id: str) -> dict:
    return dict(session.execute(
        text("SELECT property_id, match_method FROM property_radar_records WHERE radar_id = :rid"),
        {"rid": radar_id},
    ).mappings().one())


def test_links_by_parcel_id(fresh_db):
    prop = _property(fresh_db, parcel_id="TEST-PR-LINK-APN-001")
    upsert_records(fresh_db, [_rec(1)])
    assert link_unlinked(fresh_db)["linked"] == 1
    assert _link(fresh_db, "TEST-PR-LINK-001") == {"property_id": prop.id, "match_method": "parcel_id"}


def test_unloaded_county_stays_unlinked(fresh_db):
    upsert_records(fresh_db, [_rec(1, county_fips="99999")])
    result = link_unlinked(fresh_db)
    assert result["linked"] == 0 and result["no_match"] == 0
    assert _link(fresh_db, "TEST-PR-LINK-001")["property_id"] is None


def test_property_in_other_county_is_not_matched(fresh_db):
    _property(fresh_db, parcel_id="TEST-PR-LINK-APN-001", county_id=OTHER_SLUG)
    upsert_records(fresh_db, [_rec(1)])
    result = link_unlinked(fresh_db)
    assert result["linked"] == 0 and result["no_match"] == 1
    assert _link(fresh_db, "TEST-PR-LINK-001")["property_id"] is None


def test_county_guard_discards_cross_county_match(fresh_db, monkeypatch):
    wrong = SimpleNamespace(id=1, county_id=OTHER_SLUG)
    monkeypatch.setattr(linking._MatchOnlyLoader, "find_property_cascade",
                        lambda self, **kw: (wrong, "parcel_id", 100))
    upsert_records(fresh_db, [_rec(1)])
    assert link_unlinked(fresh_db)["skipped_county_mismatch"] == 1
    assert _link(fresh_db, "TEST-PR-LINK-001")["property_id"] is None


def test_paging_processes_every_record(fresh_db):
    props = [_property(fresh_db, parcel_id=f"TEST-PR-LINK-APN-{n:03d}") for n in range(1, 6)]
    upsert_records(fresh_db, [_rec(n) for n in range(1, 6)])
    assert link_unlinked(fresh_db, batch_size=2)["linked"] == 5
    for n, prop in enumerate(props, 1):
        assert _link(fresh_db, f"TEST-PR-LINK-{n:03d}")["property_id"] == prop.id


def test_already_linked_records_are_not_reprocessed(fresh_db):
    _property(fresh_db, parcel_id="TEST-PR-LINK-APN-001")
    upsert_records(fresh_db, [_rec(1)])
    link_unlinked(fresh_db)
    assert link_unlinked(fresh_db) == {"linked": 0, "no_match": 0, "skipped_county_mismatch": 0}


def test_properties_and_counties_untouched(fresh_db):
    _property(fresh_db, parcel_id="TEST-PR-LINK-APN-001")
    before = fresh_db.execute(text("SELECT (SELECT COUNT(*) FROM properties), (SELECT COUNT(*) FROM counties)")).one()
    upsert_records(fresh_db, [_rec(1)])
    link_unlinked(fresh_db)
    after = fresh_db.execute(text("SELECT (SELECT COUNT(*) FROM properties), (SELECT COUNT(*) FROM counties)")).one()
    assert tuple(before) == tuple(after)
