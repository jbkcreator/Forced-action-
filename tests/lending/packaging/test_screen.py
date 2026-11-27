"""Screening: who is screened, the fit inputs (never guessed), one package per fit, miss reasons, the fingerprint."""
from __future__ import annotations

import dataclasses
import json
from decimal import Decimal

import pytest

from config.lender_matrix import LENDER_MATRIX, SECONDARY_LENDER_KEYS
from config.lending_occupancy import HOMESTEAD_HOLD_REASON
from src.lending.contracts import LoanType
from src.lending.packaging import screen
from src.lending.packaging.screen import (
    ELIGIBLE_SQL,
    FileRef,
    NotScreenable,
    Screened,
    fingerprint,
    fit_inputs,
    screen_file,
)
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
    ids = screen_file(db, FileRef("loan_file", 7), matrix=matrix, rules_version="v1").changed
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
    assert screen_file(db, FileRef("loan_file", 7), matrix=SECONDARY, rules_version="v1") == Screened([], [])
    assert not db.ran("INSERT INTO lending.lender_packages")


def test_suppressed_lead_is_not_screened():
    db = _db(flags={"suppressed": True, "is_test": False})
    assert screen_file(db, FileRef("lf_lead", 3), matrix=SECONDARY, rules_version="v1") == Screened([], [])


def test_unchanged_facts_and_rules_draft_nothing_new():
    fp = fingerprint(FILE, "v1")
    db = _db(existing=[{"lender_key": r.key, "screen_fingerprint": fp} for r in SECONDARY])
    assert screen_file(db, FileRef("loan_file", 7), matrix=SECONDARY, rules_version="v1") == Screened([], [])
    assert not db.ran("INSERT INTO lending.lender_packages")


def test_a_rule_change_redrafts():
    fp = fingerprint(FILE, "v1")
    db = _db(existing=[{"lender_key": r.key, "screen_fingerprint": fp} for r in SECONDARY])
    assert screen_file(db, FileRef("loan_file", 7), matrix=SECONDARY, rules_version="v2").changed


def test_upsert_targets_the_open_package_index_and_casts_json():
    sql = str(screen._UPSERT)
    assert "ON CONFLICT (source_type, source_id, lender_key) WHERE status NOT IN ('submitted', 'dismissed')" in sql
    assert "CAST(:fit AS jsonb)" in sql and "::jsonb" not in sql


def test_possible_fit_gets_a_draft_that_names_the_missing_fact():
    db = _db(facts={**FILE, "arv": None, "purchase_price": None, "rehab_budget": None})
    ids = screen_file(db, FileRef("loan_file", 7), matrix=SECONDARY, rules_version="v1").changed
    upserts = db.ran("INSERT INTO lending.lender_packages")
    assert ids and len(upserts) == len(SECONDARY)
    for upsert in upserts:
        fit = json.loads(upsert["fit"])
        assert fit["needs"] and all(p["lender_key"] != upsert["lk"] for p in fit["possible"])


def test_confirmed_fit_has_no_needs():
    db = _db()
    screen_file(db, FileRef("loan_file", 7), matrix=SECONDARY, rules_version="v1")
    assert all(json.loads(u["fit"])["needs"] == [] for u in db.ran("INSERT INTO lending.lender_packages"))


# ── retiring packages that no longer fit, and the fit re-check at submission ──

def _ga_only(*keys):
    return tuple(dataclasses.replace(r, approved_states=frozenset({"GA"})) if r.key in keys else r for r in SECONDARY)


def _confirmed(matrix):
    return tuple(dataclasses.replace(r, verified=True, field_status={}) for r in matrix)


def test_an_open_package_whose_lender_now_misses_is_retired_and_ledgered():
    """A stricter rule (or a corrected fact) turns a fit into a miss: the open package is dismissed, not left ready."""
    fp = fingerprint(FILE, "v1")
    db = _db(existing=[{"lender_key": r.key, "screen_fingerprint": fp} for r in SECONDARY])
    db.answers["UPDATE lending.lender_packages SET status = 'dismissed'"] = [{"id": 41, "lender_key": "corevest"}]
    screened = screen_file(db, FileRef("loan_file", 7), matrix=_ga_only("corevest"), rules_version="v2")
    retire = db.ran("UPDATE lending.lender_packages SET status = 'dismissed'")[0]
    assert retire["keys"] == ["corevest"] and retire["st"] == "loan_file" and retire["sid"] == 7
    assert screened.retired == [41] and 41 not in screened.changed
    assert "corevest" not in {u["lk"] for u in db.ran("INSERT INTO lending.lender_packages")}
    ledger = db.ran("INSERT INTO lending.deal_audit_events")[0]
    assert ledger["type"] == "package_dismissed" and ledger["actor"] == "system:secondary_screen"


def test_retire_only_touches_open_packages_and_clears_the_fingerprint():
    sql = str(screen._RETIRE)
    assert "status NOT IN ('submitted', 'dismissed')" in sql and "screen_fingerprint = NULL" in sql


def _recheck_db(*, facts=FILE, eligible=True, flags=None):
    db = _db(facts=facts, flags=flags)
    db.answers["SELECT 1 FROM ("] = [{"one": 1}] if eligible else []
    return db


@pytest.fixture
def secondary_now(monkeypatch):
    def use(matrix):
        monkeypatch.setattr(screen, "secondary_matrix", lambda: (matrix, "v9"))
    return use


def test_recheck_passes_a_lender_that_still_fits_with_a_confirmed_box(secondary_now):
    secondary_now(_confirmed(SECONDARY))
    assert screen.current_fit_problem(_recheck_db(), "loan_file", 7, "corevest") is None


def test_recheck_refuses_a_placeholder_box(secondary_now):
    secondary_now(SECONDARY)
    assert "not confirmed" in screen.current_fit_problem(_recheck_db(), "loan_file", 7, "corevest")


def test_recheck_refuses_when_a_stricter_rule_now_misses(secondary_now):
    secondary_now(_confirmed(_ga_only("corevest")))
    assert "no longer fits" in screen.current_fit_problem(_recheck_db(), "loan_file", 7, "corevest")


def test_recheck_refuses_a_corrected_loan_amount_that_now_misses(secondary_now):
    secondary_now(tuple(dataclasses.replace(r, max_loan_amount=Decimal("200000")) if r.key == "corevest" else r
                        for r in _confirmed(SECONDARY)))
    assert screen.current_fit_problem(_recheck_db(), "loan_file", 7, "corevest") is not None  # $250k asked
    assert screen.current_fit_problem(_recheck_db(facts={**FILE, "loan_amount": 150_000}), "loan_file", 7,
                                      "corevest") is None


@pytest.mark.parametrize("kwargs, message", [
    ({"eligible": False}, "no longer declined or unassigned"),
    ({"flags": {"occupancy": "owner_occupied", "is_test": False}}, "can no longer be screened"),
    ({"facts": {**FILE, "loan_amount": None}}, "missing"),
])
def test_recheck_refuses_a_file_that_stopped_qualifying(secondary_now, kwargs, message):
    secondary_now(_confirmed(SECONDARY))
    assert message in screen.current_fit_problem(_recheck_db(**kwargs), "loan_file", 7, "corevest")
