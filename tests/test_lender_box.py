"""
Tests for src/services/lender_box.py — evaluate() and evaluate_batch().

Uses `fresh_db` (real Postgres, rolled back after each test) because
lender_box_programs.allowed_property_types is a PG TEXT[] column that SQLite
cannot handle.

Each test inserts its own program rows with a unique key prefix so tests are
fully self-contained.  The `lb_db` fixture deactivates migration seed programs
inside the rolled-back transaction so they never interfere with test assertions.
"""
from __future__ import annotations

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import text

from src.services.lender_box import DealInput, evaluate, evaluate_batch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def lb_db(fresh_db):
    """
    fresh_db with all migration seed programs deactivated for the duration of
    the test.  The deactivation is inside the rolled-back transaction so seed
    data is automatically restored when the test ends.
    """
    fresh_db.execute(
        text(
            "UPDATE lender_box_programs "
            "SET is_active = false "
            "WHERE program_key NOT LIKE 'test\\_%' ESCAPE '\\'"
        )
    )
    return fresh_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _program_key(suffix: str) -> str:
    return f"test_{uuid.uuid4().hex[:8]}_{suffix}"


def _insert_program(db, key: str, **overrides) -> None:
    defaults = dict(
        name="Test Fix & Flip",
        is_active=True,
        min_loan_amount=150_000,
        max_loan_amount=2_000_000,
        max_ltc=0.85,
        max_ltv=0.75,
        min_loan_term_months=6,
        max_loan_term_months=18,
        allowed_property_types=["single_family", "condo", "duplex", "triplex", "fourplex"],
        excluded_property_types=["commercial", "mobile_home", "multifamily_5plus"],
        min_borrower_prior_loans=0,
    )
    defaults.update(overrides)
    db.execute(
        text("""
            INSERT INTO lender_box_programs (
                program_key, name, is_active,
                min_loan_amount, max_loan_amount,
                max_ltc, max_ltv,
                min_loan_term_months, max_loan_term_months,
                allowed_property_types, excluded_property_types,
                min_borrower_prior_loans
            ) VALUES (
                :program_key, :name, :is_active,
                :min_loan_amount, :max_loan_amount,
                :max_ltc, :max_ltv,
                :min_loan_term_months, :max_loan_term_months,
                :allowed_property_types, :excluded_property_types,
                :min_borrower_prior_loans
            )
        """),
        {**defaults, "program_key": key},
    )


def _insert_geo(db, program_key: str, state: str, county: str | None = None, is_excluded: bool = False) -> None:
    db.execute(
        text("""
            INSERT INTO lender_box_geographies (program_key, state, county, is_excluded)
            VALUES (:pk, :state, :county, :is_excluded)
        """),
        {"pk": program_key, "state": state, "county": county, "is_excluded": is_excluded},
    )


def _clean_deal(**overrides) -> DealInput:
    """
    A fully-populated FL single-family deal that passes all default program
    rules when max_ltc is set to 0.86 or higher.

    Numbers:
      purchase=180k + rehab=55k  → total_cost=235k, ltc≈0.851
      arv=310k                   → ltv≈0.645
    """
    defaults = dict(
        property_type="single_family",
        state="FL",
        county="Hillsborough",
        proposed_loan_amount=Decimal("200000"),
        purchase_price=Decimal("180000"),
        rehab_estimate=Decimal("55000"),
        arv=Decimal("310000"),
        borrower_prior_loans=1,
    )
    defaults.update(overrides)
    return DealInput(**defaults)


# ---------------------------------------------------------------------------
# in_box cases
# ---------------------------------------------------------------------------

def test_clean_deal_is_in_box(lb_db):
    key = _program_key("ff")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(), lb_db)

    assert result.status == "in_box"
    assert result.matched_program == key
    assert result.fail_reasons == []
    assert result.uncertain_flags == []


def test_state_wide_geo_matches_any_county(lb_db):
    key = _program_key("geo")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")  # NULL county → state-wide

    result = evaluate(_clean_deal(county="Pinellas"), lb_db)

    assert result.status == "in_box"


def test_no_experience_requirement_passes_without_prior_loans(lb_db):
    key = _program_key("exp")
    _insert_program(lb_db, key, max_ltc=0.86, min_borrower_prior_loans=0)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(borrower_prior_loans=None), lb_db)

    assert result.status == "in_box"


def test_first_deal_passes_when_no_minimum_experience(lb_db):
    key = _program_key("exp0")
    _insert_program(lb_db, key, max_ltc=0.86, min_borrower_prior_loans=0)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(borrower_prior_loans=0), lb_db)

    assert result.status == "in_box"


def test_in_box_summary_contains_program_name(lb_db):
    key = _program_key("summary")
    _insert_program(lb_db, key, name="Fix & Flip Test", max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(), lb_db)

    assert "Fix & Flip Test" in result.summary()


# ---------------------------------------------------------------------------
# uncertain cases
# ---------------------------------------------------------------------------

def test_missing_arv_returns_uncertain(lb_db):
    key = _program_key("arv")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(arv=None), lb_db)

    assert result.status == "uncertain"
    assert "arv" in result.uncertain_flags


def test_missing_purchase_price_returns_uncertain(lb_db):
    key = _program_key("pp")
    _insert_program(lb_db, key)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(purchase_price=None), lb_db)

    assert result.status == "uncertain"
    assert "purchase_price_or_rehab_estimate" in result.uncertain_flags


def test_missing_rehab_estimate_returns_uncertain(lb_db):
    key = _program_key("rehab")
    _insert_program(lb_db, key)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(rehab_estimate=None), lb_db)

    assert result.status == "uncertain"
    assert "purchase_price_or_rehab_estimate" in result.uncertain_flags


def test_missing_arv_and_cost_returns_both_uncertain_flags(lb_db):
    key = _program_key("both")
    _insert_program(lb_db, key)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(arv=None, purchase_price=None), lb_db)

    assert result.status == "uncertain"
    assert "arv" in result.uncertain_flags
    assert "purchase_price_or_rehab_estimate" in result.uncertain_flags


def test_unknown_experience_is_uncertain_when_minimum_set(lb_db):
    key = _program_key("expunk")
    _insert_program(lb_db, key, max_ltc=0.86, min_borrower_prior_loans=2)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(borrower_prior_loans=None), lb_db)

    assert result.status == "uncertain"
    assert "borrower_prior_loans" in result.uncertain_flags


def test_no_active_programs_returns_uncertain(lb_db):
    # lb_db already deactivated seed programs; insert nothing → evaluate sees zero active programs.
    result = evaluate(_clean_deal(), lb_db)

    assert result.status == "uncertain"
    assert "no_active_programs" in result.uncertain_flags


# ---------------------------------------------------------------------------
# out_of_box cases
# ---------------------------------------------------------------------------

def test_explicitly_excluded_property_type(lb_db):
    key = _program_key("excl")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(property_type="commercial"), lb_db)

    assert result.status == "out_of_box"
    assert any("explicitly excluded" in r for r in result.fail_reasons)


def test_property_type_not_in_allowed_list(lb_db):
    key = _program_key("notallowed")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(property_type="warehouse"), lb_db)

    assert result.status == "out_of_box"
    assert any("not in allowed list" in r for r in result.fail_reasons)


def test_loan_below_minimum(lb_db):
    key = _program_key("lowloan")
    _insert_program(lb_db, key, max_ltc=0.86, min_loan_amount=150_000)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(proposed_loan_amount=Decimal("50000")), lb_db)

    assert result.status == "out_of_box"
    assert any("below minimum" in r for r in result.fail_reasons)


def test_loan_above_maximum(lb_db):
    key = _program_key("highloan")
    _insert_program(lb_db, key, max_ltc=0.90, max_loan_amount=2_000_000)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(proposed_loan_amount=Decimal("5000000")), lb_db)

    assert result.status == "out_of_box"
    assert any("exceeds maximum" in r for r in result.fail_reasons)


def test_ltc_exceeds_limit(lb_db):
    key = _program_key("ltc")
    # ltc≈0.851 → fails 0.80 max; ltv≈0.645 → passes 0.90 max
    _insert_program(lb_db, key, max_ltc=0.80, max_ltv=0.90)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(), lb_db)

    assert result.status == "out_of_box"
    assert any("LTC" in r for r in result.fail_reasons)


def test_ltv_exceeds_limit(lb_db):
    key = _program_key("ltv")
    # ltv≈0.645 → fails 0.50 max; ltc≈0.851 → passes 0.90 max
    _insert_program(lb_db, key, max_ltc=0.90, max_ltv=0.50)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(), lb_db)

    assert result.status == "out_of_box"
    assert any("LTV" in r for r in result.fail_reasons)


def test_state_not_in_any_program(lb_db):
    key = _program_key("state")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")  # FL only

    result = evaluate(_clean_deal(state="NY"), lb_db)

    assert result.status == "out_of_box"
    assert any("geography" in r for r in result.fail_reasons)


def test_experience_below_minimum(lb_db):
    key = _program_key("expfail")
    _insert_program(lb_db, key, max_ltc=0.86, min_borrower_prior_loans=3)
    _insert_geo(lb_db, key, "FL")

    result = evaluate(_clean_deal(borrower_prior_loans=1), lb_db)

    assert result.status == "out_of_box"
    assert any("prior loans" in r for r in result.fail_reasons)


# ---------------------------------------------------------------------------
# Geography edge cases
# ---------------------------------------------------------------------------

def test_county_level_exclusion_blocks_within_permitted_state(lb_db):
    key = _program_key("countyexcl")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")                              # state-wide allowed
    _insert_geo(lb_db, key, "FL", "Pasco", is_excluded=True)  # Pasco excluded

    blocked = evaluate(_clean_deal(state="FL", county="Pasco"), lb_db)
    allowed = evaluate(_clean_deal(state="FL", county="Hillsborough"), lb_db)

    assert blocked.status == "out_of_box"
    assert allowed.status == "in_box"


def test_county_level_allow_without_state_wide_row(lb_db):
    key = _program_key("countyonly")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL", "Hillsborough")  # county-level only, no state-wide row

    in_county = evaluate(_clean_deal(state="FL", county="Hillsborough"), lb_db)
    out_of_county = evaluate(_clean_deal(state="FL", county="Pinellas"), lb_db)

    assert in_county.status == "in_box"
    assert out_of_county.status == "out_of_box"


# ---------------------------------------------------------------------------
# Multi-program — first match wins
# ---------------------------------------------------------------------------

def test_second_program_matches_when_first_fails(lb_db):
    key_strict = _program_key("strict")
    key_loose = _program_key("loose")

    # Strict: max LTC 0.70 — ltc≈0.851 will fail.
    _insert_program(lb_db, key_strict, name="Strict", max_ltc=0.70, max_ltv=0.90)
    _insert_geo(lb_db, key_strict, "FL")

    # Loose: max LTC 0.90 — will pass.
    _insert_program(lb_db, key_loose, name="Loose", max_ltc=0.90, max_ltv=0.90)
    _insert_geo(lb_db, key_loose, "FL")

    result = evaluate(_clean_deal(), lb_db)

    assert result.status == "in_box"
    assert result.matched_program == key_loose


# ---------------------------------------------------------------------------
# evaluate_batch
# ---------------------------------------------------------------------------

def test_evaluate_batch_returns_result_per_deal(lb_db):
    key = _program_key("batch")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")

    deals = [
        ("deal_a", _clean_deal()),
        ("deal_b", _clean_deal(property_type="commercial")),
        ("deal_c", _clean_deal(arv=None)),
    ]

    results = evaluate_batch(deals, lb_db)

    assert len(results) == 3
    ref_map = {ref: res for ref, res in results}

    assert ref_map["deal_a"].status == "in_box"
    assert ref_map["deal_b"].status == "out_of_box"
    assert ref_map["deal_c"].status == "uncertain"


def test_evaluate_batch_preserves_ref_order(lb_db):
    key = _program_key("order")
    _insert_program(lb_db, key, max_ltc=0.86)
    _insert_geo(lb_db, key, "FL")

    refs = [f"ref_{i}" for i in range(5)]
    deals = [(r, _clean_deal()) for r in refs]

    results = evaluate_batch(deals, lb_db)

    assert [ref for ref, _ in results] == refs
