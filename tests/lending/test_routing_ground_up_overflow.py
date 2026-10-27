"""R-1: a ground-up file at or above the primary's minimum that the primary cannot take cascades to the
next ground-up lender instead of stalling in the hold queue.

These call the router's rule function directly on in-memory lenders, with the seat count stubbed, so they
need no database (the rule tests in test_routing.py need a disposable Postgres).
"""
from __future__ import annotations

from typing import Optional

import pytest

from config.lending_routing import OVERFLOW_PRIMARY_AT_CAP, OVERFLOW_PRIMARY_BOX_FAIL
from src.lending.contracts import LoanType
from src.lending.routing import router
from src.lending.routing.rules import FileProfile, LenderRow, ProductBox

GROUND_UP = LoanType.GROUND_UP_CONSTRUCTION.value
FLIP = LoanType.FIX_AND_FLIP.value


def _box(*loan_types: str, states: Optional[list[str]] = None) -> ProductBox:
    return ProductBox(
        loan_types=frozenset(loan_types), min_loan_amount=None, max_loan_amount=None,
        approved_states=frozenset(states) if states else None, credit_floor=None,
        max_ltv=None, max_ltc=None, min_completed_projects=0, requires_ground_up_build=False,
    )


def _lender(lender_id: int, name: str, *, priority: int, boxes: list[ProductBox], is_primary: bool = False,
            construction_min: Optional[int] = None, takes_ground_up: bool = False) -> LenderRow:
    return LenderRow(
        id=lender_id, name=name, active=True, is_primary=is_primary, agreement_status="signed",
        priority=priority, monthly_cap=None, construction_min_amount=construction_min,
        takes_ground_up=takes_ground_up, boxes=tuple(boxes),
    )


PRIMARY = _lender(1, "GU Primary", priority=1, is_primary=True, construction_min=500_000,
                  boxes=[_box(GROUND_UP, FLIP, states=["FL"])])
GROUND_UP_NEXT = _lender(2, "GU Next", priority=2, takes_ground_up=True, boxes=[_box(GROUND_UP)])
FLIP_NEXT = _lender(3, "Flip Next", priority=3, boxes=[_box(FLIP)])


@pytest.fixture
def choose(monkeypatch):
    """Run the router's rules with the primary's seat cap set open or full."""
    def run(profile: FileProfile, lenders: list[LenderRow], *, at_cap: bool = False, tried: tuple[int, ...] = ()):
        monkeypatch.setattr(router, "seat_at_cap", lambda lender, db: at_cap)
        return router._choose(profile, lenders, list(tried), db=None)
    return run


def _ground_up(amount: int, state: str = "FL") -> FileProfile:
    return FileProfile(loan_type=GROUND_UP, loan_amount=amount, state=state)


def test_at_the_primarys_cap_a_large_ground_up_file_goes_to_the_next_ground_up_lender(choose):
    assert choose(_ground_up(600_000), [PRIMARY, GROUND_UP_NEXT], at_cap=True) == (
        GROUND_UP_NEXT, OVERFLOW_PRIMARY_AT_CAP)


def test_outside_the_primarys_box_a_large_ground_up_file_goes_to_the_next_ground_up_lender(choose):
    assert choose(_ground_up(750_000, state="GA"), [PRIMARY, GROUND_UP_NEXT]) == (
        GROUND_UP_NEXT, OVERFLOW_PRIMARY_BOX_FAIL)


def test_the_box_is_reported_over_the_cap_when_both_stop_the_primary(choose):
    assert choose(_ground_up(750_000, state="GA"), [PRIMARY, GROUND_UP_NEXT], at_cap=True) == (
        GROUND_UP_NEXT, OVERFLOW_PRIMARY_BOX_FAIL)


def test_a_large_ground_up_file_the_primary_can_take_still_goes_to_the_primary(choose):
    assert choose(_ground_up(750_000), [PRIMARY, GROUND_UP_NEXT]) == (PRIMARY, None)


def test_the_minimum_is_inclusive_so_exactly_500000_at_cap_also_cascades(choose):
    assert choose(_ground_up(500_000), [PRIMARY, GROUND_UP_NEXT], at_cap=True) == (
        GROUND_UP_NEXT, OVERFLOW_PRIMARY_AT_CAP)


def test_with_no_ground_up_lender_left_the_file_is_held(choose):
    assert choose(_ground_up(750_000), [PRIMARY, FLIP_NEXT], at_cap=True) == (None, None)


def test_below_the_minimum_the_waterfall_is_unchanged_and_records_no_bypass(choose):
    assert choose(_ground_up(450_000), [PRIMARY, GROUND_UP_NEXT]) == (GROUND_UP_NEXT, None)


def test_after_the_primary_declines_the_waterfall_is_unchanged_and_records_no_bypass(choose):
    assert choose(_ground_up(750_000), [PRIMARY, GROUND_UP_NEXT], tried=(PRIMARY.id,)) == (GROUND_UP_NEXT, None)


def test_a_flip_file_at_the_primarys_cap_is_unchanged_and_records_no_bypass(choose):
    flip = FileProfile(loan_type=FLIP, loan_amount=300_000, state="FL")
    assert choose(flip, [PRIMARY, GROUND_UP_NEXT, FLIP_NEXT], at_cap=True) == (FLIP_NEXT, None)
