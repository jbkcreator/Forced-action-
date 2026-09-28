"""Tests for the PropertyRadar upsert writer. Real Postgres via fresh_db (rolled back)."""
from __future__ import annotations

from sqlalchemy import text

from src.services.property_radar.staging import normalize_owner, upsert_records

RID = "TEST-PR-RADAR-001"
APN = "TEST-PR-APN-001"


def _rec(**overrides) -> dict:
    base = {
        "radar_id": RID,
        "state_fips": "12",
        "county_fips": "12057",
        "apn": APN,
        "state": "FL",
        "county_name": "HILLSBOROUGH",
        "property_address": "123 TEST ST",
        "city": "TAMPA",
        "zip": "33614",
        "property_type": "SFR",
        "owner_name": "ACME INVESTMENTS LLC",
        "ownership_type": "Corporate",
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


def _row(session, radar_id: str = RID) -> dict:
    return dict(session.execute(
        text("SELECT status, change_flags, prior, changed_at, owner_name "
             "FROM property_radar_records WHERE radar_id = :rid"),
        {"rid": radar_id},
    ).mappings().one())


def _count(session) -> int:
    return session.execute(
        text("SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE 'TEST-PR-%'")
    ).scalar()


# ── normalize_owner ──────────────────────────────────────────────────────────

def test_normalize_owner_expands_abbreviations():
    assert normalize_owner("KIAVI FNDG INC") == "KIAVI FUNDING INC"


def test_normalize_owner_removes_punctuation():
    assert normalize_owner("Smith's LLC.") == "SMITHS LLC"


def test_normalize_owner_collapses_spaces():
    assert normalize_owner("ACME  SVCS  LLC") == "ACME SERVICES LLC"


def test_normalize_owner_none_returns_empty():
    assert normalize_owner(None) == ""


# ── upsert ───────────────────────────────────────────────────────────────────

def test_insert_new_record(fresh_db):
    assert upsert_records(fresh_db, [_rec()]) == (1, 0, 0)
    assert _row(fresh_db)["status"] == "active"


def test_same_batch_twice_leaves_row_count_unchanged(fresh_db):
    batch = [_rec(), _rec(radar_id="TEST-PR-RADAR-002", apn="TEST-PR-APN-002")]
    upsert_records(fresh_db, batch)
    before = _count(fresh_db)
    assert upsert_records(fresh_db, batch) == (0, 2, 0)
    assert _count(fresh_db) == before == 2


def test_same_apn_different_counties_stored_as_two_rows(fresh_db):
    upsert_records(fresh_db, [
        _rec(radar_id="TEST-PR-RADAR-001", county_fips="12057", apn="TEST-PR-SHARED"),
        _rec(radar_id="TEST-PR-RADAR-002", county_fips="12101", apn="TEST-PR-SHARED"),
    ])
    assert _count(fresh_db) == 2


def test_owner_change_flags_sold_and_keeps_prior(fresh_db):
    upsert_records(fresh_db, [_rec(owner_name="ORIGINAL OWNER LLC")])
    upsert_records(fresh_db, [_rec(owner_name="NEW BUYER CORP")])
    row = _row(fresh_db)
    assert row["status"] == "sold"
    assert row["change_flags"] == ["owner_changed"]
    assert row["prior"]["owner_name"] == "ORIGINAL OWNER LLC"
    assert row["owner_name"] == "NEW BUYER CORP"


def test_flags_persist_after_unchanged_restage(fresh_db):
    upsert_records(fresh_db, [_rec(owner_name="ORIGINAL OWNER LLC")])
    upsert_records(fresh_db, [_rec(owner_name="NEW BUYER CORP")])
    flagged = _row(fresh_db)
    upsert_records(fresh_db, [_rec(owner_name="NEW BUYER CORP")])
    row = _row(fresh_db)
    assert row["status"] == "sold"
    assert row["change_flags"] == ["owner_changed"]
    assert row["prior"] == flagged["prior"]
    assert row["changed_at"] == flagged["changed_at"]


def test_abbreviated_owner_is_not_a_change(fresh_db):
    upsert_records(fresh_db, [_rec(owner_name="KIAVI FNDG INC")])
    upsert_records(fresh_db, [_rec(owner_name="KIAVI FUNDING INC")])
    row = _row(fresh_db)
    assert row["status"] == "active" and not row["change_flags"]


def test_missing_owner_is_not_a_sale(fresh_db):
    upsert_records(fresh_db, [_rec(owner_name="ACME INVESTMENTS LLC")])
    upsert_records(fresh_db, [_rec(owner_name=None)])
    row = _row(fresh_db)
    assert row["status"] == "active" and not row["change_flags"]


def test_loan_change_flags_refinanced(fresh_db):
    upsert_records(fresh_db, [_rec(loan_doc_number="DOC-OLD")])
    upsert_records(fresh_db, [_rec(loan_doc_number="DOC-NEW")])
    row = _row(fresh_db)
    assert row["status"] == "refinanced"
    assert row["change_flags"] == ["loan_changed"]
    assert row["prior"]["loan_doc_number"] == "DOC-OLD"


def test_lender_fallback_ignores_abbreviation_difference(fresh_db):
    upsert_records(fresh_db, [_rec(loan_doc_number=None, lender_name="KIAVI FNDG INC")])
    upsert_records(fresh_db, [_rec(loan_doc_number=None, lender_name="Kiavi Funding, Inc")])
    row = _row(fresh_db)
    assert row["status"] == "active" and not row["change_flags"]


def test_lender_fallback_detects_new_loan(fresh_db):
    upsert_records(fresh_db, [_rec(loan_doc_number=None, loan_recorded_date="2025-01-15")])
    upsert_records(fresh_db, [_rec(loan_doc_number=None, loan_recorded_date="2026-03-01")])
    assert _row(fresh_db)["status"] == "refinanced"


def test_owner_and_loan_change_status_is_sold(fresh_db):
    upsert_records(fresh_db, [_rec(owner_name="OLD OWNER LLC", loan_doc_number="DOC-OLD")])
    upsert_records(fresh_db, [_rec(owner_name="NEW OWNER CORP", loan_doc_number="DOC-NEW")])
    row = _row(fresh_db)
    assert row["status"] == "sold"
    assert set(row["change_flags"]) == {"owner_changed", "loan_changed"}


def test_radar_id_held_by_other_key_is_skipped(fresh_db):
    upsert_records(fresh_db, [_rec(apn="TEST-PR-APN-A")])
    assert upsert_records(fresh_db, [_rec(apn="TEST-PR-APN-B")]) == (0, 0, 1)
    assert _count(fresh_db) == 1


def test_duplicate_key_in_one_batch_keeps_last(fresh_db):
    ins, upd, skip = upsert_records(fresh_db, [_rec(owner_name="FIRST LLC"), _rec(owner_name="SECOND LLC")])
    assert (ins, upd, skip) == (1, 0, 1)
    assert _row(fresh_db)["owner_name"] == "SECOND LLC"


def test_duplicate_radar_id_in_one_batch_is_skipped(fresh_db):
    ins, _, skip = upsert_records(fresh_db, [_rec(apn="TEST-PR-APN-A"), _rec(apn="TEST-PR-APN-B")])
    assert (ins, skip) == (1, 1)


def test_empty_batch_returns_zeros(fresh_db):
    assert upsert_records(fresh_db, []) == (0, 0, 0)
