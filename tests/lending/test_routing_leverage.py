"""Chunk 1 Addendum 1.2.1 / 1.2.4: the router's leverage check and why the primary was bypassed (no database)."""
from __future__ import annotations

from src.lending.contracts import LoanType
from src.lending.routing.rules import (
    FileProfile,
    LenderRow,
    ProductBox,
    lender_passes_box,
    outside_footprint,
    primary_bypass_reason,
)

GU = LoanType.GROUND_UP_CONSTRUCTION.value


def _box(**overrides) -> ProductBox:
    fields = dict(loan_types=frozenset({GU}), min_loan_amount=None, max_loan_amount=None, approved_states=None,
                  credit_floor=None, max_ltv=0.70, max_ltc=0.85, min_completed_projects=0,
                  requires_ground_up_build=False)
    return ProductBox(**{**fields, **overrides})


def _lender(*boxes: ProductBox) -> LenderRow:
    return LenderRow(id=1, name="Backflip", active=True, is_primary=True, agreement_status="signed", priority=1,
                     monthly_cap=15, construction_min_amount=500_000, takes_ground_up=True, boxes=boxes)


def _file(**figures) -> FileProfile:
    return FileProfile(loan_type=GU, loan_amount=700_000, state="FL", **figures)


def test_a_100_percent_ltc_file_fails_an_85_percent_box():
    """Josh's Test 1 (Oct 22, Q21): the file fails the box on its own numbers."""
    lender = _lender(_box())
    assert not lender_passes_box(lender, _file(purchase_price=500_000, rehab_budget=200_000))
    assert primary_bypass_reason(lender, _file(purchase_price=500_000, rehab_budget=200_000), at_cap=False) \
        == "LEVERAGE_EXCEEDED"


def test_an_ltv_over_the_arv_cap_fails_and_one_under_passes():
    lender = _lender(_box())
    assert not lender_passes_box(lender, _file(arv=900_000))  # 77.8% > 70%
    assert lender_passes_box(lender, _file(arv=1_100_000))  # 63.6%


def test_missing_figures_skip_the_leverage_check():
    """A LendingFlow lead has no price, rehab or ARV: leverage waits for the call (PENDING_CALL_FACTS)."""
    assert lender_passes_box(_lender(_box()), _file())
    assert lender_passes_box(_lender(_box()), _file(purchase_price=500_000))  # rehab unknown -> no LTC check


def test_the_bypass_reason_names_what_failed():
    lender = _lender(_box(approved_states=frozenset({"GA"})))
    assert primary_bypass_reason(lender, _file(), at_cap=True) == "SEAT_CAP"
    assert primary_bypass_reason(lender, _file(), at_cap=False) == "OUT_OF_FOOTPRINT"
    assert primary_bypass_reason(_lender(_box(max_loan_amount=600_000)), _file(), at_cap=False) == "BOX_FAIL"


def test_a_state_no_signed_lender_covers_is_outside_the_footprint():
    lenders = [_lender(_box(approved_states=frozenset({"FL", "GA"})))]
    assert outside_footprint(lenders, "TX") is True
    assert outside_footprint(lenders, "FL") is False


def test_a_box_without_states_covers_every_state():
    assert outside_footprint([_lender(_box(approved_states=frozenset({"FL"})), _box())], "TX") is False


def test_a_file_without_a_state_or_no_lenders_is_never_outside_the_footprint():
    assert outside_footprint([_lender(_box(approved_states=frozenset({"FL"})))], "") is False
    assert outside_footprint([], "TX") is False
