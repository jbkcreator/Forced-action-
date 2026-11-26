"""Screening: who is screened, the fit inputs (never guessed), one package per fit, miss reasons, the fingerprint."""
from __future__ import annotations

import dataclasses
import json

import pytest

from config.lender_matrix import LENDER_MATRIX, SECONDARY_LENDER_KEYS
from config.lending_occupancy import HOMESTEAD_HOLD_REASON
from src.lending.contracts import LoanType
from src.lending.packaging import screen
from src.lending.packaging.screen import ELIGIBLE_SQL, FileRef, NotScreenable, fingerprint, fit_inputs, screen_file
from tests.lending.packaging.fakes import FakeSession

SECONDARY = tuple(r for r in LENDER_MATRIX if r.key in SECONDARY_LENDER_KEYS)
FILE = {"id": 7, "first_name": "Alice", "last_name": "Smith", "phone": "+18135550101", "product": "flip",
        "loan_amount": 250_000, "purchase_price": 300_000, "rehab_budget": 50_000, "arv": 420_000,
        "property_address": "1 Main St", "property_city": "Tampa", "property_state": "FL", "property_zip": "33601",
        "credit_band": "680-719", "credit_band_min_fico": None, "completed_projects": 4, "entity_name": None}


def _db(*, facts=FILE, flags=None, existing=()):
    return FakeSession({
        "SELECT occupancy, is_test FROM lending.loan_files": [flags or {"occupancy": None, "is_test": False}],
        "SELECT suppressed, is_test FROM lending.lendingflow_leads": [flags or {"suppressed": False, "is_test": False}],
        "FROM lending.loan_files f LEFT JOIN lending.entities": [facts],
        "FROM lending.lendingflow_leads WHERE id": [facts],
        "SELECT lender_key, screen_fingerprint FROM lending.lender_packages": list(existing),
        "INSERT INTO lending.lender_packages": lambda p: [{"id": 100 + len(p["lk"])}],
    })


def test_eligibility_reads_primary_declines_by_id_and_unassigned_files():
    sql = str(ELIGIBLE_SQL)
    assert "e.action = 'declined'" in sql and "is_primary" in sql and "e.lender_id =" in sql
    assert "lender_name" not in sql and "lender_key" not in sql  # never matched by name
    assert "NOT fr.is_test" in sql and "fr.status = ANY(:unassigned)" in sql
    assert "fr.lead_id" in sql and "fr.loan_file_id" in sql  # leads and partner files
    assert set(screen.EXCLUDED_HOLDS) == {"suppressed", HOMESTEAD_HOLD_REASON}
    assert set(screen.UNASSIGNED_STATUSES) == {"held", "unrouted", "exhausted"}


def test_eligible_files_maps_rows_to_file_refs():
    db = FakeSession({"FROM lending.file_routing fr": [{"lead_id": None, "loan_file_id": 7},
                                                      {"lead_id": 3, "loan_file_id": None}]})
    assert screen.eligible_files(db) == [FileRef("loan_file", 7), FileRef("lf_lead", 3)]


def test_partner_file_fit_inputs_use_the_partner_band_vocabulary():
    borrower, loan = fit_inputs("loan_file", FILE)
    assert (borrower.credit_band_min_fico, borrower.credit_band_max_fico) == (680, 719)
    assert loan.loan_type is LoanType.FIX_AND_FLIP and loan.state == "FL" and int(loan.loan_amount) == 250_000


def test_lead_fit_inputs_use_its_loan_type_and_band():
    lead = {**FILE, "product": "BRIDGE", "credit_band": "620-679"}
    borrower, loan = fit_inputs("lf_lead", lead)
    assert loan.loan_type is LoanType.BRIDGE and borrower.credit_band_min_fico == 620


@pytest.mark.parametrize("change", [{"product": "other"}, {"loan_amount": None}, {"property_state": None}])
def test_missing_facts_are_never_guessed(change):
    with pytest.raises(NotScreenable):
        fit_inputs("loan_file", {**FILE, **change})


def test_each_fitting_lender_gets_an_unconfirmed_draft_with_every_miss_listed():
    matrix = tuple(dataclasses.replace(r, approved_states=frozenset({"GA"}))
                   if r.key in ("temple_view", "abl") else r for r in SECONDARY)
    db = _db()
    ids = screen_file(db, FileRef("loan_file", 7), matrix=matrix, rules_version="v1")
    upserts = db.ran("INSERT INTO lending.lender_packages")
    assert len(ids) == 2 and {u["lk"] for u in upserts} == {"corevest", "rcn_capital"}
    assert all(u["confirmed"] is False for u in upserts)
    fit = json.loads(upserts[0]["fit"])
    assert {m["lender_key"] for m in fit["misses"]} == {"temple_view", "abl"}
    assert all(m["reasons"] for m in fit["misses"])


@pytest.mark.parametrize("flags", [{"occupancy": "owner_occupied", "is_test": False},
                                   {"occupancy": None, "is_test": True}])
def test_owner_occupied_and_test_files_are_not_screened(flags):
    db = _db(flags=flags)
    assert screen_file(db, FileRef("loan_file", 7), matrix=SECONDARY, rules_version="v1") == []
    assert not db.ran("INSERT INTO lending.lender_packages")


def test_suppressed_lead_is_not_screened():
    db = _db(flags={"suppressed": True, "is_test": False})
    assert screen_file(db, FileRef("lf_lead", 3), matrix=SECONDARY, rules_version="v1") == []


def test_unchanged_facts_and_rules_draft_nothing_new():
    fp = fingerprint(FILE, "v1")
    db = _db(existing=[{"lender_key": r.key, "screen_fingerprint": fp} for r in SECONDARY])
    assert screen_file(db, FileRef("loan_file", 7), matrix=SECONDARY, rules_version="v1") == []
    assert not db.ran("INSERT INTO lending.lender_packages")


def test_a_rule_change_redrafts():
    fp = fingerprint(FILE, "v1")
    db = _db(existing=[{"lender_key": r.key, "screen_fingerprint": fp} for r in SECONDARY])
    assert screen_file(db, FileRef("loan_file", 7), matrix=SECONDARY, rules_version="v2")


def test_upsert_targets_the_open_package_index_and_casts_json():
    sql = str(screen._UPSERT)
    assert "ON CONFLICT (source_type, source_id, lender_key) WHERE status NOT IN ('submitted', 'dismissed')" in sql
    assert "CAST(:fit AS jsonb)" in sql and "::jsonb" not in sql
