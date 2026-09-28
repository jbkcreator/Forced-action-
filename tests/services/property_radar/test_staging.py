"""Tests for PropertyRadar upsert writer (staging.py).

Uses fresh_db (real Postgres, rolls back after each test).
All test rows use a TEST- radar_id prefix so they are trivially identifiable.
"""
from __future__ import annotations

import pytest

from src.services.property_radar.staging import normalize_owner, upsert_records

# ── fixtures ─────────────────────────────────────────────────────────────────

def _rec(**overrides) -> dict:
    base = {
        "radar_id": "TEST-RADAR-001",
        "state_fips": "12",
        "county_fips": "12057",
        "apn": "APN-TEST-001",
        "state": "FL",
        "county_name": "HILLSBOROUGH",
        "property_address": "123 TEST ST",
        "city": "TAMPA",
        "zip": "33614",
        "property_type": "SFR",
        "owner_name": "ACME INVESTMENTS LLC",
        "ownership_type": "Corporate",
        "mailing_address": "100 MAIN ST",
        "mailing_city": "BOSTON",
        "mailing_state": "MA",
        "mailing_zip": "02110",
        "principal_name": None,
        "lender_name": "KIAVI FUNDING INC",
        "loan_amount": 300000,
        "loan_recorded_date": "2025-01-15",
        "loan_term_years": "1",
        "est_maturity_date": "2026-01-15",
        "loan_doc_number": "DOC-001",
        "campaign": "maturity_target_lender",
        "raw": {"source": "test"},
    }
    base.update(overrides)
    return base


# ── normalize_owner unit tests ────────────────────────────────────────────────

def test_normalize_owner_expands_abbreviations():
    assert normalize_owner("KIAVI FNDG INC") == "KIAVI FUNDING INC"


def test_normalize_owner_uppercases_and_strips_punct():
    assert normalize_owner("Smith's LLC.") == "SMITHS LLC"


def test_normalize_owner_collapses_spaces():
    assert normalize_owner("ACME  SVCS  LLC") == "ACME SERVICES LLC"


def test_normalize_owner_none_returns_empty():
    assert normalize_owner(None) == ""


# ── upsert_records integration tests ─────────────────────────────────────────

def test_insert_new_record(fresh_db):
    ins, upd, skip = upsert_records(fresh_db, [_rec()])
    assert ins == 1 and upd == 0 and skip == 0


def test_idempotent_same_batch_twice(fresh_db):
    r = _rec()
    upsert_records(fresh_db, [r])
    ins2, upd2, skip2 = upsert_records(fresh_db, [r])
    assert ins2 == 0 and upd2 == 1 and skip2 == 0


def test_two_different_apns_same_county(fresh_db):
    r1 = _rec(radar_id="TEST-RADAR-001", apn="APN-A")
    r2 = _rec(radar_id="TEST-RADAR-002", apn="APN-B")
    ins, upd, skip = upsert_records(fresh_db, [r1, r2])
    assert ins == 2


def test_same_apn_different_counties_stored_as_two_rows(fresh_db):
    r1 = _rec(radar_id="TEST-RADAR-001", county_fips="12057", apn="SHARED-APN")
    r2 = _rec(radar_id="TEST-RADAR-002", county_fips="12101", apn="SHARED-APN")
    ins, _, _ = upsert_records(fresh_db, [r1, r2])
    assert ins == 2


def test_owner_change_flags_sold(fresh_db):
    upsert_records(fresh_db, [_rec(owner_name="ORIGINAL OWNER LLC")])
    ins, upd, skip = upsert_records(fresh_db, [_rec(owner_name="NEW BUYER CORP")])
    assert upd == 1

    from sqlalchemy import text
    row = fresh_db.execute(
        text("SELECT status, change_flags FROM property_radar_records WHERE radar_id = 'TEST-RADAR-001'")
    ).mappings().one()
    assert row["status"] == "sold"
    assert "owner_changed" in row["change_flags"]


def test_owner_abbreviation_does_not_trigger_false_change(fresh_db):
    upsert_records(fresh_db, [_rec(owner_name="KIAVI FNDG INC")])
    ins, upd, _ = upsert_records(fresh_db, [_rec(owner_name="KIAVI FUNDING INC")])
    assert upd == 1

    from sqlalchemy import text
    row = fresh_db.execute(
        text("SELECT status, change_flags FROM property_radar_records WHERE radar_id = 'TEST-RADAR-001'")
    ).mappings().one()
    assert row["status"] == "active"
    assert not row["change_flags"]


def test_loan_change_flags_refinanced(fresh_db):
    upsert_records(fresh_db, [_rec(loan_doc_number="DOC-OLD")])
    upsert_records(fresh_db, [_rec(loan_doc_number="DOC-NEW")])

    from sqlalchemy import text
    row = fresh_db.execute(
        text("SELECT status, change_flags, prior FROM property_radar_records WHERE radar_id = 'TEST-RADAR-001'")
    ).mappings().one()
    assert row["status"] == "refinanced"
    assert "loan_changed" in row["change_flags"]
    assert row["prior"]["loan_doc_number"] == "DOC-OLD"


def test_both_owner_and_loan_change_status_is_sold(fresh_db):
    upsert_records(fresh_db, [_rec(owner_name="OLD OWNER LLC", loan_doc_number="DOC-OLD")])
    upsert_records(fresh_db, [_rec(owner_name="NEW OWNER CORP", loan_doc_number="DOC-NEW")])

    from sqlalchemy import text
    row = fresh_db.execute(
        text("SELECT status, change_flags FROM property_radar_records WHERE radar_id = 'TEST-RADAR-001'")
    ).mappings().one()
    assert row["status"] == "sold"
    assert set(row["change_flags"]) == {"owner_changed", "loan_changed"}


def test_radar_id_collision_on_different_key_is_skipped(fresh_db):
    # Insert a row with radar_id TEST-RADAR-001 under apn APN-A
    upsert_records(fresh_db, [_rec(radar_id="TEST-RADAR-001", apn="APN-A")])
    # Try to insert a DIFFERENT apn with the same radar_id — should be skipped
    ins, upd, skip = upsert_records(fresh_db, [_rec(radar_id="TEST-RADAR-001", apn="APN-B")])
    assert skip == 1 and ins == 0


def test_empty_batch_returns_zeros(fresh_db):
    assert upsert_records(fresh_db, []) == (0, 0, 0)
