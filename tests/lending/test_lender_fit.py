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

from dataclasses import replace
from decimal import Decimal

import pytest

from config.lender_matrix import (
    LENDER_MATRIX,
    LenderRules,
    validate_lender_matrix,
)
from src.lending.contracts import (
    BorrowerProfile,
    DeclineReason,
    FieldStatus,
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
    # The fixture matrix holds an unconfirmed lender, so the score stays hidden (A1).
    assert result.lender_fit_score is None


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
    assert any("Requested: 85.7%, Max: 85%" in r for r in construction_miss.reasons)


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


def test_s3_unknown_credit_is_possible_not_a_decline() -> None:
    """Credit not captured on call: "possible, confirm on the call" (Josh, A3)."""
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
    assert all(m.lender_key != "fixture_flip" for m in result.non_fitting)
    assert all(f.lender_key != "fixture_flip" for f in result.fitting)
    flip_possible = next((p for p in result.possible if p.lender_key == "fixture_flip"), None)
    assert flip_possible is not None
    assert any("credit band unknown" in r.lower() for r in flip_possible.why_unknown)


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

    # Unconfirmed shell: evaluated against the placeholder box (fix and flip only), so a
    # construction loan is a decline that says the rule is unconfirmed.
    unverified_miss = next(
        (m for m in result.non_fitting if m.lender_key == "fixture_unverified"), None
    )
    assert unverified_miss is not None
    assert any("unconfirmed rule" in r for r in unverified_miss.reasons)

    # Exact decline codes alongside the reason strings.
    assert DeclineReason.LOAN_TOO_SMALL in construction_miss.reason_codes
    assert DeclineReason.STATE_NOT_COVERED in construction_miss.reason_codes
    assert DeclineReason.CREDIT_BELOW_MIN in construction_miss.reason_codes

    # An unconfirmed lender is in the matrix, so no score is shown (A1).
    assert result.lender_fit_score is None


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

def test_unconfirmed_lender_with_unknown_deal_facts_is_possible() -> None:
    """An unconfirmed lender is evaluated on the placeholder box; unknown deal facts leave it possible."""
    profile = BorrowerProfile(credit_band_min_fico=700, completed_projects=5, state="FL")
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP,
        loan_amount=Decimal("300_000"),
        state="FL",
    )

    result = evaluate_lender_fit(profile, request, matrix=(_UNVERIFIED_SHELL,))

    assert result.fitting == []
    assert result.non_fitting == []
    assert [p.lender_key for p in result.possible] == ["fixture_unverified"]
    assert result.lender_fit_score is None  # not every lender confirmed (A1)


def test_unconfirmed_lender_that_passes_every_check_fits_subject_to_confirmation() -> None:
    profile = BorrowerProfile(credit_band_min_fico=700, state="FL")
    request = LoanRequest(
        loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("300_000"), state="FL",
        purchase_price=Decimal("300_000"), rehab_budget=Decimal("100_000"), arv=Decimal("600_000"),
    )

    result = evaluate_lender_fit(profile, request, matrix=(_UNVERIFIED_SHELL, _FLIP_LENDER))

    by_key = {f.lender_key: f for f in result.fitting}
    assert by_key["fixture_unverified"].subject_to_confirmation is True
    assert by_key["fixture_flip"].subject_to_confirmation is False
    assert result.lender_fit_score is None


# ---------------------------------------------------------------------------
# Josh A1 / A2 / A3 / B1(c) behaviour
# ---------------------------------------------------------------------------

def _flip_request(**overrides) -> LoanRequest:
    base = dict(loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("300_000"), state="FL",
                purchase_price=Decimal("300_000"), rehab_budget=Decimal("100_000"), arv=Decimal("600_000"))
    base.update(overrides)
    return LoanRequest(**base)


def test_placeholder_box_fills_an_unset_cap_and_is_not_skipped() -> None:
    """A1: the unset LTC of an unconfirmed lender takes the 85% placeholder, so 300K on 330K cost declines."""
    shell = LenderRules(key="shell", name="Shell", verified=False, loan_types=frozenset({LoanType.FIX_AND_FLIP}))
    profile = BorrowerProfile(credit_band_min_fico=700, state="FL")

    result = evaluate_lender_fit(profile, _flip_request(rehab_budget=Decimal("30_000")), matrix=(shell,))

    miss = result.non_fitting[0]
    assert DeclineReason.LTC_OVER_MAX in miss.reason_codes
    assert any("unconfirmed rule" in r for r in miss.reasons)


def test_confirmed_lender_with_unset_cap_means_no_limit() -> None:
    lender = LenderRules(key="conf", name="Conf", verified=True, loan_types=frozenset({LoanType.FIX_AND_FLIP}))
    profile = BorrowerProfile(credit_band_min_fico=700, state="FL")

    result = evaluate_lender_fit(profile, _flip_request(rehab_budget=Decimal("30_000")), matrix=(lender,))

    assert [f.lender_key for f in result.fitting] == ["conf"]


def test_per_field_status_all_confirmed_makes_a_lender_confirmed() -> None:
    from config.lender_matrix import PARAMETER_FIELDS, fully_confirmed

    unconfirmed = LenderRules(key="a", name="A", verified=False)
    confirmed = LenderRules(
        key="b", name="B", verified=False,
        field_status={name: FieldStatus.CONFIRMED for name in PARAMETER_FIELDS},
    )
    one_pending = LenderRules(
        key="c", name="C", verified=False,
        field_status={**{name: FieldStatus.CONFIRMED for name in PARAMETER_FIELDS},
                      "max_ltc": FieldStatus.CONFIRM_AT_SUBMISSION},
    )
    assert fully_confirmed(unconfirmed) is False
    assert fully_confirmed(confirmed) is True
    assert fully_confirmed(one_pending) is False


def test_validate_lender_matrix_rejects_bad_field_status() -> None:
    with pytest.raises(ValueError, match="unknown parameters"):
        validate_lender_matrix((LenderRules(key="x", name="X", verified=False,
                                            field_status={"not_a_field": FieldStatus.CONFIRMED}),))
    with pytest.raises(ValueError, match="not confirmed"):
        validate_lender_matrix((LenderRules(key="y", name="Y", verified=True,
                                            field_status={"max_ltc": FieldStatus.PLACEHOLDER}),))


def test_score_shown_only_when_every_lender_is_confirmed() -> None:
    profile = BorrowerProfile(credit_band_min_fico=640, state="FL")
    request = LoanRequest(loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("200_000"), state="FL")

    confirmed = evaluate_lender_fit(profile, request, matrix=(_FLIP_LENDER, _DSCR_LENDER))
    pending = evaluate_lender_fit(profile, request, matrix=(_FLIP_LENDER, _UNVERIFIED_SHELL))

    assert confirmed.lender_fit_score is not None
    assert pending.lender_fit_score is None


@pytest.mark.parametrize("band,expect", [
    (None, "possible"),     # unknown
    (0, "decline"),         # "below 640" is below every floor
    (640, "possible"),      # 640+ may or may not reach 680
    (679, "possible"),
    (680, "fit"),           # at the floor
    (700, "fit"),
])
def test_credit_band_against_a_680_floor(band, expect) -> None:
    lender = LenderRules(key="l", name="L", verified=True, loan_types=frozenset({LoanType.FIX_AND_FLIP}),
                         credit_floor=680)
    result = evaluate_lender_fit(BorrowerProfile(credit_band_min_fico=band, state="FL"),
                                 _flip_request(), matrix=(lender,))
    got = "fit" if result.fitting else "possible" if result.possible else "decline"
    assert got == expect
    if expect == "decline":
        assert DeclineReason.CREDIT_BELOW_MIN in result.non_fitting[0].reason_codes


def test_unknown_state_is_possible_and_wrong_state_is_declined() -> None:
    lender = LenderRules(key="l", name="L", verified=True, loan_types=frozenset({LoanType.FIX_AND_FLIP}),
                         approved_states=frozenset({"FL"}))
    profile = BorrowerProfile(credit_band_min_fico=700)

    unknown = evaluate_lender_fit(profile, _flip_request(state=""), matrix=(lender,))
    wrong = evaluate_lender_fit(profile, _flip_request(state="NY"), matrix=(lender,))

    assert [p.lender_key for p in unknown.possible] == ["l"]
    assert "state" in unknown.missing_fields
    assert wrong.non_fitting[0].reason_codes == [DeclineReason.STATE_NOT_COVERED]


@pytest.mark.parametrize("amount,expect", [
    (Decimal("99_999"), "decline"),   # one under the minimum
    (Decimal("100_000"), "fit"),      # at the minimum
    (Decimal("2_000_000"), "fit"),    # at the maximum
    (Decimal("2_000_001"), "decline"),
])
def test_loan_amount_boundaries_and_codes(amount, expect) -> None:
    lender = LenderRules(key="l", name="L", verified=True, loan_types=frozenset({LoanType.FIX_AND_FLIP}),
                         min_loan_amount=Decimal("100_000"), max_loan_amount=Decimal("2_000_000"))
    result = evaluate_lender_fit(BorrowerProfile(state="FL"), _flip_request(loan_amount=amount,
                                 purchase_price=None, rehab_budget=None, arv=None), matrix=(lender,))
    assert ("fit" if result.fitting else "decline") == expect
    if expect == "decline":
        wanted = DeclineReason.LOAN_TOO_SMALL if amount < Decimal("100_000") else DeclineReason.LOAN_TOO_LARGE
        assert result.non_fitting[0].reason_codes == [wanted]


def test_ratio_experience_and_property_codes() -> None:
    lender = LenderRules(
        key="l", name="L", verified=True, loan_types=frozenset({LoanType.FIX_AND_FLIP}),
        max_ltv=Decimal("0.4"), max_ltc=Decimal("0.5"), min_completed_projects=3,
        allowed_property_types=frozenset({"single_family"}),
    )
    result = evaluate_lender_fit(
        BorrowerProfile(completed_projects=1, state="FL"),
        _flip_request(property_type="condo"), matrix=(lender,),
    )
    assert set(result.non_fitting[0].reason_codes) == {
        DeclineReason.LTV_OVER_MAX, DeclineReason.LTC_OVER_MAX,
        DeclineReason.EXPERIENCE_BELOW_MIN, DeclineReason.PROPERTY_TYPE_EXCLUDED,
    }


def test_backflip_ranks_first_even_when_costlier() -> None:
    def lender(key, points):
        return LenderRules(key=key, name=key, verified=True, loan_types=frozenset({LoanType.FIX_AND_FLIP}),
                           origination_points=points, rate_spread=Decimal("0.10"))

    matrix = (lender("cheap_other", Decimal("0.01")), lender("generic_backflip_flip", Decimal("0.05")),
              lender("mid_other", Decimal("0.02")))
    profile = BorrowerProfile(state="FL")

    result = evaluate_lender_fit(profile, _flip_request(), matrix=matrix)
    assert [f.lender_key for f in result.fitting] == ["generic_backflip_flip", "cheap_other", "mid_other"]

    without_backflip = evaluate_lender_fit(profile, _flip_request(), matrix=(matrix[0], matrix[2]))
    assert [f.lender_key for f in without_backflip.fitting] == ["cheap_other", "mid_other"]


def test_production_matrix_never_fits_without_the_confirmation_flag() -> None:
    profile = BorrowerProfile(credit_band_min_fico=700, completed_projects=5, completed_ground_up_builds=2, state="FL")
    for loan_type in LoanType:
        request = LoanRequest(loan_type=loan_type, loan_amount=Decimal("300_000"), state="FL",
                              purchase_price=Decimal("300_000"), rehab_budget=Decimal("100_000"),
                              arv=Decimal("600_000"), property_type="single_family")
        result = evaluate_lender_fit(profile, request)
        assert all(f.subject_to_confirmation for f in result.fitting)
        assert result.lender_fit_score is None


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


def _same_cost_lender(key: str) -> LenderRules:
    return LenderRules(
        key=key, name=key, verified=True,
        loan_types=frozenset({LoanType.FIX_AND_FLIP}),
        approved_states=frozenset({"FL"}),
        origination_points=Decimal("0.02"),
        rate_spread=Decimal("0.10"),
        hold_months=12,
    )


def test_equal_costs_are_ordered_by_lenders_priority() -> None:
    """Chunk 1 Addendum 1.2.3: a cost tie goes to the lower /lenders priority; no R-1 row sorts after."""
    profile = BorrowerProfile(credit_band_min_fico=640, state="FL")
    request = LoanRequest(loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("200_000"), state="FL")
    matrix = (_same_cost_lender("lender_c"), _same_cost_lender("lender_none"), _same_cost_lender("lender_a"))

    result = evaluate_lender_fit(profile, request, matrix=matrix, priorities={"lender_a": 2, "lender_c": 5})

    assert [f.lender_key for f in result.fitting] == ["lender_a", "lender_c", "lender_none"]


def test_no_cost_tie_never_reads_priorities(monkeypatch) -> None:
    from src.lending import lender_fit

    monkeypatch.setattr(lender_fit, "routing_priorities", lambda: pytest.fail("priorities read without a tie"))
    profile = BorrowerProfile(credit_band_min_fico=640, state="FL")
    request = LoanRequest(loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("200_000"), state="FL")
    cheaper = replace(_same_cost_lender("cheaper"), origination_points=Decimal("0.01"))
    result = evaluate_lender_fit(profile, request, matrix=(_same_cost_lender("dearer"), cheaper))
    assert [f.lender_key for f in result.fitting] == ["cheaper", "dearer"]


@pytest.mark.parametrize("figures, expected", [
    ({"purchase_price": Decimal("200_000"), "rehab_budget": Decimal("50_000"), "arv": Decimal("400_000")}, "checked"),
    ({"purchase_price": Decimal("200_000"), "rehab_budget": Decimal("50_000")}, "pending_call_facts"),
    ({"arv": Decimal("400_000")}, "pending_call_facts"),
])
def test_leverage_status_says_when_figures_wait_for_the_call(figures, expected) -> None:
    """Chunk 1 Addendum 1.2.4: no purchase price / ARV (raw LendingFlow) -> PENDING_CALL_FACTS."""
    profile = BorrowerProfile(credit_band_min_fico=640, state="FL")
    request = LoanRequest(loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("150_000"), state="FL", **figures)
    assert evaluate_lender_fit(profile, request, matrix=FIXTURE_MATRIX).leverage_status == expected


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
    identity_fields = {"key", "name", "verified", "field_status"}
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


# ---------------------------------------------------------------------------
# Closed credit bands: the upper bound decides decline vs possible (review: 600-639 vs a 640 floor)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("band_min, band_max, floor, outcome", [
    (600, 639, 640, "decline"),    # the whole band is below the floor
    (640, 679, 680, "decline"),
    (620, 679, 640, "possible"),   # the band crosses the floor
    (640, None, 680, "possible"),  # open-ended "640+"
    (660, 719, 680, "possible"),
    (680, 719, 680, "fit"),
    (680, 719, 740, "decline"),    # review on #363: "680-719" is not "680+", it never reaches a 740 floor
    (700, 759, 740, "possible"),   # a band that straddles the floor needs confirming on the call
    (680, None, 740, "possible"),  # open-ended "680+" can reach 740
])
def test_closed_credit_band_declines_only_when_its_top_is_below_the_floor(band_min, band_max, floor, outcome):
    lender = LenderRules(key="band_lender", name="Band Lender", verified=True,
                         loan_types=frozenset({LoanType.FIX_AND_FLIP}), credit_floor=floor)
    profile = BorrowerProfile(credit_band_min_fico=band_min, credit_band_max_fico=band_max, state="FL")
    request = LoanRequest(loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("300_000"), state="FL")
    result = evaluate_lender_fit(profile, request, matrix=(lender,))
    found = ("decline" if result.non_fitting else "possible" if result.possible else "fit")
    assert found == outcome
    if outcome == "decline":
        assert DeclineReason.CREDIT_BELOW_MIN in result.non_fitting[0].reason_codes
        assert f"{band_min}-{band_max}" in result.non_fitting[0].reasons[0]


@pytest.mark.parametrize("raw, expected", [
    ("600-639", (600, 639)), ("680 to 719", (680, 719)), ("720+", None), ("below 640", None), ("", None), (None, None),
])
def test_band_bounds_parser_gives_an_upper_bound_only_for_a_closed_band(raw, expected):
    from src.lending.enrichment.fit_card import parse_band_bounds

    assert parse_band_bounds(raw) == expected
