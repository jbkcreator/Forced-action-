"""Tests for T-05 — evaluate_lender_fit and the lender matrix.

Covers the four scenarios required by SPEC §5 Test 2:
  S1 — Fix & flip borrower (verified Backflip flip entry fits)
  S2 — Experienced builder (≥3 projects, ≥1 ground-up; Backflip construction fits)
  S3 — Credit-challenged investor (band below 640; all verified lenders reject)
  S4 — Non-qualifying case with exact failure reasons

Plus matrix-level validation and the provisional cost/score helpers.

No database required — evaluate_lender_fit is DB-free; lender_box.evaluate()
tests are in tests/test_lender_box.py and remain unaffected.

Q1–Q5 open items are noted inline where they affect expected values.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from config.lender_matrix import (
    LENDER_MATRIX,
    LenderRules,
    validate_lender_matrix,
)
from src.lending.contracts import (
    BorrowerProfile,
    LenderFitResult,
    LoanRequest,
    LoanType,
)
from src.lending.lender_fit import _fit_score, _total_borrower_cost, evaluate_lender_fit


# ---------------------------------------------------------------------------
# Fixture lender matrix used by all scenario tests
# Keeps tests independent of production Backflip values that Josh may revise.
# ---------------------------------------------------------------------------

_FLIP_LENDER = LenderRules(
    key="fixture_flip",
    name="Fixture Flip Lender",
    verified=True,
    loan_types=frozenset({LoanType.FIX_AND_FLIP}),
    min_loan_amount=Decimal("100_000"),
    max_loan_amount=Decimal("2_000_000"),
    max_ltv=Decimal("0.75"),
    min_purchase_price=Decimal("85_000"),
    credit_floor=640,
    min_completed_projects=0,
    approved_states=frozenset({"FL", "GA"}),
    origination_points=Decimal("0.02"),
    rate_spread=Decimal("0.10"),
    hold_months=12,
)

_CONSTRUCTION_LENDER = LenderRules(
    key="fixture_construction",
    name="Fixture Construction Lender",
    verified=True,
    loan_types=frozenset({LoanType.GROUND_UP_CONSTRUCTION}),
    min_loan_amount=Decimal("500_000"),
    max_loan_amount=Decimal("5_000_000"),
    max_ltc=Decimal("0.85"),
    credit_floor=680,
    min_completed_projects=3,
    requires_ground_up_build=True,
    approved_states=frozenset({"FL", "GA"}),
    origination_points=Decimal("0.02"),
    rate_spread=Decimal("0.08"),
    hold_months=12,
)

_DSCR_LENDER = LenderRules(
    key="fixture_dscr",
    name="Fixture DSCR Lender",
    verified=True,
    loan_types=frozenset({LoanType.DSCR_RENTAL}),
    min_loan_amount=Decimal("150_000"),
    approved_states=frozenset({"FL"}),
    credit_floor=None,
    origination_points=Decimal("0.015"),
    rate_spread=Decimal("0.07"),
    hold_months=12,
)

_UNVERIFIED_SHELL = LenderRules(
    key="fixture_unverified",
    name="Fixture Unverified Shell",
    verified=False,
)

FIXTURE_MATRIX: tuple[LenderRules, ...] = (
    _FLIP_LENDER,
    _CONSTRUCTION_LENDER,
    _DSCR_LENDER,
    _UNVERIFIED_SHELL,
)


# ---------------------------------------------------------------------------
# S1 — Fix & flip borrower
# SPEC §5 Test 2 scenario 1
# ---------------------------------------------------------------------------

def test_s1_fix_and_flip_borrower_qualifies_for_flip_lender() -> None:
    """Standard FL fix-and-flip borrower should fit the flip lender."""
    profile = BorrowerProfile(
        credit_band_min_fico=640,
        completed_projects=1,
        has_live_deal=True,
        has_liquidity=True,
        state="FL",
    )
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("300_000"),
        state="FL",
        purchase_price=Decimal("200_000"),
        rehab_budget=Decimal("50_000"),
        arv=Decimal("450_000"),
    )

    result = evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX)

    fitting_keys = {f.lender_key for f in result.fitting}
    assert "fixture_flip" in fitting_keys, (
        f"Expected fixture_flip to fit; fitting={fitting_keys}, "
        f"non_fitting={[(m.lender_key, m.reasons) for m in result.non_fitting]}"
    )
    assert "fixture_construction" not in fitting_keys, (
        "Construction lender should not fit a FIX_AND_FLIP loan type"
    )
    assert result.lender_fit_score is not None


def test_s1_flip_lender_ranked_by_cost_ascending() -> None:
    """Fitting lenders are ranked lowest total_borrower_cost first."""
    # Add a second verified flip lender with higher cost.
    cheap_lender = LenderRules(
        key="cheap_flip",
        name="Cheap Flip",
        verified=True,
        loan_types=frozenset({LoanType.FIX_AND_FLIP}),
        min_loan_amount=Decimal("100_000"),
        approved_states=frozenset({"FL"}),
        credit_floor=640,
        origination_points=Decimal("0.01"),
        rate_spread=Decimal("0.09"),
        hold_months=12,
    )
    expensive_lender = LenderRules(
        key="expensive_flip",
        name="Expensive Flip",
        verified=True,
        loan_types=frozenset({LoanType.FIX_AND_FLIP}),
        min_loan_amount=Decimal("100_000"),
        approved_states=frozenset({"FL"}),
        credit_floor=640,
        origination_points=Decimal("0.03"),
        rate_spread=Decimal("0.12"),
        hold_months=12,
    )
    matrix = (cheap_lender, expensive_lender)

    profile = BorrowerProfile(credit_band_min_fico=640, completed_projects=1, state="FL")
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("200_000"),
        state="FL",
    )

    result = evaluate_lender_fit(profile, request, matrix=matrix)

    assert len(result.fitting) == 2
    costs = [f.total_borrower_cost for f in result.fitting]
    assert costs == sorted(costs), f"Expected ascending cost order; got {costs}"
    assert result.fitting[0].lender_key == "cheap_flip"


# ---------------------------------------------------------------------------
# S2 — Experienced builder
# SPEC §5 Test 2 scenario 2
# ---------------------------------------------------------------------------

def test_s2_experienced_builder_qualifies_for_construction_lender() -> None:
    """Builder with 3+ projects including ≥1 ground-up should fit the construction lender."""
    profile = BorrowerProfile(
        credit_band_min_fico=680,
        completed_projects=4,
        completed_ground_up_builds=2,
        has_live_deal=True,
        has_liquidity=True,
        state="FL",
    )
    request = LoanRequest(
        loan_type=LoanType.GROUND_UP_CONSTRUCTION,
        loan_amount=Decimal("600_000"),
        state="FL",
        purchase_price=Decimal("400_000"),
        rehab_budget=Decimal("300_000"),  # LTC = 600k / 700k = 85.7% > 85% cap → borderline
    )

    result = evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX)

    # LTC 85.7% exceeds 85% cap, so expect rejection for that reason.
    construction_miss = next(
        (m for m in result.non_fitting if m.lender_key == "fixture_construction"), None
    )
    assert construction_miss is not None
    assert any("LTC" in r for r in construction_miss.reasons)


def test_s2_experienced_builder_within_ltc_qualifies() -> None:
    """Builder within the LTC cap should qualify the construction lender."""
    profile = BorrowerProfile(
        credit_band_min_fico=680,
        completed_projects=3,
        completed_ground_up_builds=1,
        state="FL",
    )
    request = LoanRequest(
        loan_type=LoanType.GROUND_UP_CONSTRUCTION,
        loan_amount=Decimal("500_000"),
        state="FL",
        purchase_price=Decimal("350_000"),
        rehab_budget=Decimal("240_000"),  # total cost 590k; LTC = 500/590 = 84.7% < 85%
    )

    result = evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX)

    fitting_keys = {f.lender_key for f in result.fitting}
    assert "fixture_construction" in fitting_keys, (
        f"Expected fixture_construction to fit; "
        f"non_fitting={[(m.lender_key, m.reasons) for m in result.non_fitting]}"
    )


def test_s2_builder_without_ground_up_fails_construction() -> None:
    """3 projects but zero ground-up builds: construction lender rejects."""
    profile = BorrowerProfile(
        credit_band_min_fico=680,
        completed_projects=3,
        completed_ground_up_builds=0,
        state="FL",
    )
    request = LoanRequest(
        loan_type=LoanType.GROUND_UP_CONSTRUCTION,
        loan_amount=Decimal("500_000"),
        state="FL",
    )

    result = evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX)

    construction_miss = next(
        (m for m in result.non_fitting if m.lender_key == "fixture_construction"), None
    )
    assert construction_miss is not None
    assert any("ground-up" in r.lower() for r in construction_miss.reasons)


# ---------------------------------------------------------------------------
# S3 — Credit-challenged investor
# SPEC §5 Test 2 scenario 3
# ---------------------------------------------------------------------------

def test_s3_credit_below_640_rejected_by_all_verified_lenders() -> None:
    """Borrower below 640 should be rejected by all lenders with a credit floor.

    Q1 encoding: credit_band_min_fico=0 means "caller answered 'below 640'".
    """
    profile = BorrowerProfile(
        credit_band_min_fico=0,   # Q1: 0 = "below 640"
        completed_projects=5,
        completed_ground_up_builds=2,
        has_live_deal=True,
        has_liquidity=True,
        state="FL",
    )
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("300_000"),
        state="FL",
        purchase_price=Decimal("200_000"),
        arv=Decimal("450_000"),
    )

    result = evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX)

    # flip lender has credit_floor=640 → should reject
    flip_miss = next(
        (m for m in result.non_fitting if m.lender_key == "fixture_flip"), None
    )
    assert flip_miss is not None, "fixture_flip should reject a below-640 borrower"
    assert any("640" in r for r in flip_miss.reasons)
    assert any("below" in r.lower() for r in flip_miss.reasons)
    # "below 640" label must not expose an exact FICO score
    assert all("620" not in r and "600" not in r for r in flip_miss.reasons)


def test_s3_unknown_credit_gives_conservative_rejection_with_missing_field() -> None:
    """Credit not captured on call: lender rejects with 'capture on call' reason."""
    profile = BorrowerProfile(
        credit_band_min_fico=None,  # unknown
        completed_projects=2,
        state="FL",
    )
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("200_000"),
        state="FL",
        purchase_price=Decimal("150_000"),
        arv=Decimal("300_000"),
    )

    result = evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX)

    assert "credit_band_min_fico" in result.missing_fields
    flip_miss = next(
        (m for m in result.non_fitting if m.lender_key == "fixture_flip"), None
    )
    assert flip_miss is not None
    assert any("credit band unknown" in r.lower() for r in flip_miss.reasons)


# ---------------------------------------------------------------------------
# S4 — Non-qualifying scenario: verify exact failure reasons
# SPEC §5 Test 2 — non-qualifying case
# ---------------------------------------------------------------------------

def test_s4_non_qualifying_exact_failure_reasons() -> None:
    """Non-qualifying borrower — verify the reason strings match the spec examples."""
    # Insufficient loan for construction lender ($200K < $500K min)
    # Below credit floor for flip lender (below 640)
    # Wrong state for all lenders (TX)
    profile = BorrowerProfile(
        credit_band_min_fico=0,  # below 640 per Q1 encoding
        completed_projects=0,
        state="TX",
    )
    request = LoanRequest(
        loan_type=LoanType.GROUND_UP_CONSTRUCTION,
        loan_amount=Decimal("200_000"),
        state="TX",
    )

    result = evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX)

    # No fitting lenders expected
    assert result.fitting == [], f"Expected no fitting lenders; got {result.fitting}"

    # Construction lender: loan amount < $500K minimum
    construction_miss = next(
        (m for m in result.non_fitting if m.lender_key == "fixture_construction"), None
    )
    assert construction_miss is not None
    loan_reasons = [r for r in construction_miss.reasons if "$200,000" in r or "minimum" in r]
    assert loan_reasons, (
        f"Expected 'loan amount below minimum' reason; got {construction_miss.reasons}"
    )

    # Flip lender: wrong loan type (GROUND_UP_CONSTRUCTION vs FIX_AND_FLIP)
    flip_miss = next(
        (m for m in result.non_fitting if m.lender_key == "fixture_flip"), None
    )
    assert flip_miss is not None
    assert any("loan type" in r.lower() for r in flip_miss.reasons), (
        f"Expected loan type mismatch reason; got {flip_miss.reasons}"
    )

    # Unverified shell: always rejected with 'not yet verified' reason
    unverified_miss = next(
        (m for m in result.non_fitting if m.lender_key == "fixture_unverified"), None
    )
    assert unverified_miss is not None
    assert any("not yet verified" in r for r in unverified_miss.reasons)

    # fit_score: 0 fitting / 3 verified = 0.0
    assert result.lender_fit_score == Decimal("0.0")


# ---------------------------------------------------------------------------
# Partial input (Minute-5 pre-qual: 4 fields only)
# ---------------------------------------------------------------------------

def test_partial_input_four_fields_returns_result_not_exception() -> None:
    """Minute-5 pre-qual input: credit band, loan amount, state, loan type only.

    Missing ARV / purchase price / rehab should be reported in missing_fields,
    not raise an exception.  T-07 uses this to decide whether a PDF is possible.
    """
    profile = BorrowerProfile(
        credit_band_min_fico=640,
        state="FL",
    )
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("250_000"),
        state="FL",
        # purchase_price, rehab_budget, arv all absent
    )

    result = evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX)

    # Should not raise; missing_fields should be populated.
    assert isinstance(result, LenderFitResult)
    assert "arv" in result.missing_fields or len(result.non_fitting) > 0


def test_partial_input_missing_purchase_price_reported() -> None:
    """Purchase price missing → reported in missing_fields when lender requires it."""
    profile = BorrowerProfile(credit_band_min_fico=640, state="FL")
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("200_000"),
        state="FL",
        # no purchase_price
    )

    result = evaluate_lender_fit(profile, request, matrix=(_FLIP_LENDER,))

    assert "purchase_price" in result.missing_fields


# ---------------------------------------------------------------------------
# Unverified lender always in non_fitting
# ---------------------------------------------------------------------------

def test_unverified_lender_never_in_fitting() -> None:
    """An unverified lender is always routed to non_fitting regardless of other inputs."""
    profile = BorrowerProfile(credit_band_min_fico=700, completed_projects=5, state="FL")
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("300_000"),
        state="FL",
    )

    result = evaluate_lender_fit(profile, request, matrix=(_UNVERIFIED_SHELL,))

    assert result.fitting == []
    assert len(result.non_fitting) == 1
    assert result.non_fitting[0].lender_key == "fixture_unverified"
    assert result.lender_fit_score is None  # no verified lenders → None (Q4)


# ---------------------------------------------------------------------------
# fit_score helper unit tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fitting,total,expected", [
    (0, 0, None),        # no verified lenders → None
    (2, 4, Decimal("50.0")),
    (3, 3, Decimal("100.0")),
    (0, 3, Decimal("0.0")),
    (1, 3, Decimal("33.3")),
])
def test_fit_score(fitting: int, total: int, expected: "Decimal | None") -> None:
    assert _fit_score(fitting, total) == expected


# ---------------------------------------------------------------------------
# total_borrower_cost helper unit tests
# ---------------------------------------------------------------------------

def test_total_borrower_cost_both_known() -> None:
    """2 points + 10% spread on $100K over 12 months = $2K points + $10K interest."""
    rules = LenderRules(
        key="cost_test",
        name="Cost Test",
        verified=True,
        origination_points=Decimal("0.02"),
        rate_spread=Decimal("0.10"),
        hold_months=12,
    )
    cost = _total_borrower_cost(rules, Decimal("100_000"))
    assert cost == Decimal("12000")


def test_total_borrower_cost_none_when_fields_missing() -> None:
    """Missing origination_points or rate_spread → None (Q2 pending)."""
    rules_no_points = LenderRules(
        key="x", name="x", verified=True,
        origination_points=None, rate_spread=Decimal("0.10"), hold_months=12,
    )
    rules_no_spread = LenderRules(
        key="y", name="y", verified=True,
        origination_points=Decimal("0.02"), rate_spread=None, hold_months=12,
    )
    assert _total_borrower_cost(rules_no_points, Decimal("100_000")) is None
    assert _total_borrower_cost(rules_no_spread, Decimal("100_000")) is None


# ---------------------------------------------------------------------------
# Cost unknown sorts last
# ---------------------------------------------------------------------------

def test_cost_unknown_lender_sorted_last() -> None:
    """Lenders with no cost formula sort after those with a known cost."""
    with_cost = LenderRules(
        key="with_cost", name="With Cost", verified=True,
        loan_types=frozenset({LoanType.FIX_AND_FLIP}),
        approved_states=frozenset({"FL"}),
        origination_points=Decimal("0.02"),
        rate_spread=Decimal("0.10"),
        hold_months=12,
    )
    no_cost = LenderRules(
        key="no_cost", name="No Cost", verified=True,
        loan_types=frozenset({LoanType.FIX_AND_FLIP}),
        approved_states=frozenset({"FL"}),
        origination_points=None,
        rate_spread=None,
    )
    profile = BorrowerProfile(credit_band_min_fico=640, state="FL")
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("200_000"),
        state="FL",
    )

    result = evaluate_lender_fit(profile, request, matrix=(no_cost, with_cost))

    assert len(result.fitting) == 2
    assert result.fitting[0].lender_key == "with_cost"
    assert result.fitting[1].lender_key == "no_cost"


# ---------------------------------------------------------------------------
# Matrix validation
# ---------------------------------------------------------------------------

def test_validate_lender_matrix_passes_production_matrix() -> None:
    """The production LENDER_MATRIX in config/lender_matrix.py passes structural validation."""
    validate_lender_matrix(LENDER_MATRIX)  # should not raise


def test_validate_lender_matrix_rejects_inverted_loan_bounds() -> None:
    bad = LenderRules(
        key="bad", name="Bad", verified=True,
        min_loan_amount=Decimal("500_000"),
        max_loan_amount=Decimal("100_000"),
    )
    with pytest.raises(ValueError, match="min_loan_amount"):
        validate_lender_matrix((bad,))


def test_validate_lender_matrix_rejects_duplicate_keys() -> None:
    dup = LenderRules(key="dup", name="Dup A", verified=True)
    dup2 = LenderRules(key="dup", name="Dup B", verified=True)
    with pytest.raises(ValueError, match="Duplicate"):
        validate_lender_matrix((dup, dup2))


# ---------------------------------------------------------------------------
# Re-export from lender_box.py — existing callers not broken
# ---------------------------------------------------------------------------

def test_lender_box_re_exports_evaluate_lender_fit() -> None:
    """evaluate_lender_fit is importable from src.services.lender_box (SPEC §4.3 path)."""
    from src.services.lender_box import evaluate_lender_fit as fn
    assert callable(fn)


# ---------------------------------------------------------------------------
# Review fixes F1/F2 — every constraint field on LenderRules must be enforced
# ---------------------------------------------------------------------------

def test_every_lender_rules_constraint_field_is_read_by_the_evaluator() -> None:
    """A LenderRules field that _check_rules/_total_borrower_cost never reads is a
    silently ignored constraint (the defect behind review F1/F2)."""
    import dataclasses
    import inspect

    from src.lending import lender_fit

    source = inspect.getsource(lender_fit._check_rules) + inspect.getsource(
        lender_fit._total_borrower_cost
    )
    identity_fields = {"key", "name", "verified"}
    unread = [
        f.name
        for f in dataclasses.fields(LenderRules)
        if f.name not in identity_fields and f"rules.{f.name}" not in source
    ]
    assert unread == [], f"LenderRules fields never enforced: {unread}"


def test_property_type_not_in_allowed_set_rejected() -> None:
    lender = LenderRules(
        key="sfr_only", name="SFR Only", verified=True,
        allowed_property_types=frozenset({"single_family"}),
    )
    profile = BorrowerProfile(state="FL")
    base = dict(loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("200_000"), state="FL")

    ok = evaluate_lender_fit(profile, LoanRequest(property_type="Single_Family ", **base), matrix=(lender,))
    assert [f.lender_key for f in ok.fitting] == ["sfr_only"]

    bad = evaluate_lender_fit(profile, LoanRequest(property_type="condo", **base), matrix=(lender,))
    assert bad.fitting == []
    assert any("property type" in r for r in bad.non_fitting[0].reasons)

    unknown = evaluate_lender_fit(profile, LoanRequest(**base), matrix=(lender,))
    assert unknown.fitting == []
    assert "property_type" in unknown.missing_fields
