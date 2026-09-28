"""Tests for PropertyRadar → FA property linker (linking.py).

Seeds a fake property in a fake county (pattern from CDE-08) so we don't
depend on live data. Real-DB rollback via fresh_db fixture.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from src.core.models import Property
from src.services.property_radar.linking import link_unlinked
from src.services.property_radar.staging import upsert_records

# ── helpers ──────────────────────────────────────────────────────────────────

# Fake county slug that maps to FIPS 12057 in our config. We override the map
# during the test so we can seed a fake Property without touching live counties.
_FAKE_FIPS = "12057"
_FAKE_SLUG = "hillsborough"
_FAKE_APN = "TEST-LINK-APN-001"
_FAKE_RADAR_ID = "TEST-LINK-RADAR-001"


def _insert_fake_property(session, *, parcel_id: str, county_id: str, address: str) -> Property:
    p = Property(parcel_id=parcel_id, address=address, county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _pr_record(**overrides) -> dict:
    base = {
        "radar_id": _FAKE_RADAR_ID,
        "state_fips": "12",
        "county_fips": _FAKE_FIPS,
        "apn": _FAKE_APN,
        "state": "FL",
        "county_name": "HILLSBOROUGH",
        "property_address": "456 LINK TEST ST",
        "city": "TAMPA",
        "zip": "33601",
        "property_type": "SFR",
        "owner_name": "LINK OWNER LLC",
        "lender_name": "TEST LENDER",
        "loan_doc_number": "LINK-DOC-001",
        "campaign": "maturity_target_lender",
        "raw": {},
    }
    base.update(overrides)
    return base


# ── tests ────────────────────────────────────────────────────────────────────

def test_link_by_parcel_id(fresh_db):
    """Record with matching APN in a loaded county gets property_id set."""
    prop = _insert_fake_property(
        fresh_db,
        parcel_id=_FAKE_APN,
        county_id=_FAKE_SLUG,
        address="456 LINK TEST ST",
    )
    upsert_records(fresh_db, [_pr_record()])

    result = link_unlinked(fresh_db)
    assert result["linked"] >= 1

    row = fresh_db.execute(
        text("SELECT property_id, match_method FROM property_radar_records WHERE radar_id = :rid"),
        {"rid": _FAKE_RADAR_ID},
    ).mappings().one()
    assert row["property_id"] == prop.id
    assert row["match_method"] == "parcel_id"


def test_no_link_for_unloaded_county(fresh_db):
    """Records from counties not in COUNTY_FIPS_TO_SLUG stay unlinked."""
    r = _pr_record(
        radar_id="TEST-NOLINK-001",
        apn="TEST-NOLINK-APN",
        county_fips="99999",  # not in config
    )
    upsert_records(fresh_db, [r])
    result = link_unlinked(fresh_db)
    assert result["linked"] == 0

    row = fresh_db.execute(
        text("SELECT property_id FROM property_radar_records WHERE radar_id = 'TEST-NOLINK-001'")
    ).mappings().one()
    assert row["property_id"] is None


def test_no_link_when_property_in_different_county(fresh_db):
    """Parcel exists but under a different county slug — cascade is county-scoped
    so it returns no match; property_id stays null."""
    _insert_fake_property(
        fresh_db,
        parcel_id=_FAKE_APN,
        county_id="pasco",  # different county than the staging record (hillsborough)
        address="456 LINK TEST ST",
    )
    upsert_records(fresh_db, [_pr_record()])

    result = link_unlinked(fresh_db)
    # Cascade is county-scoped: parcel+address search only within hillsborough,
    # so the pasco property is invisible → no_match, not mismatch.
    assert result["linked"] == 0
    assert result["no_match"] >= 1

    row = fresh_db.execute(
        text("SELECT property_id FROM property_radar_records WHERE radar_id = :rid"),
        {"rid": _FAKE_RADAR_ID},
    ).mappings().one()
    assert row["property_id"] is None


def test_already_linked_records_skipped(fresh_db):
    """link_unlinked only targets rows with property_id IS NULL."""
    prop = _insert_fake_property(
        fresh_db,
        parcel_id=_FAKE_APN,
        county_id=_FAKE_SLUG,
        address="456 LINK TEST ST",
    )
    upsert_records(fresh_db, [_pr_record()])
    # First pass links it
    link_unlinked(fresh_db)
    # Second pass should not re-process
    result = link_unlinked(fresh_db)
    assert result["linked"] == 0


def test_link_does_not_touch_properties_table(fresh_db):
    """properties row count is unchanged after linking."""
    prop = _insert_fake_property(
        fresh_db,
        parcel_id=_FAKE_APN,
        county_id=_FAKE_SLUG,
        address="456 LINK TEST ST",
    )
    before = fresh_db.execute(text("SELECT COUNT(*) FROM properties")).scalar()
    upsert_records(fresh_db, [_pr_record()])
    link_unlinked(fresh_db)
    after = fresh_db.execute(text("SELECT COUNT(*) FROM properties")).scalar()
    assert before == after
