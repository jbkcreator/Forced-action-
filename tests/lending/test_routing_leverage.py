"""R-1: the router enforces each box's LTV (loan / ARV) and LTC (loan / (purchase + rehab)) caps when the file
carries the figures, and leaves them to the call when it does not.

Pure: in-memory boxes and lenders, seat count stubbed, no database.
"""
from __future__ import annotations

from typing import Optional

import pytest

from config.lending_routing import OVERFLOW_PRIMARY_BOX_FAIL
from src.lending.contracts import LoanType
from src.lending.routing import router
from src.lending.routing.rules import (
    FileProfile,
    LenderRow,
    ProductBox,
    _box_matches,
    lender_passes_box,
    missing_leverage_facts,
)

FLIP = LoanType.FIX_AND_FLIP.value
GROUND_UP = LoanType.GROUND_UP_CONSTRUCTION.value


def _box(*loan_types: str, max_ltv: Optional[float] = None, max_ltc: Optional[float] = None) -> ProductBox:
    return ProductBox(
        loan_types=frozenset(loan_types), min_loan_amount=None, max_loan_amount=None, approved_states=None,
        credit_floor=None, max_ltv=max_ltv, max_ltc=max_ltc, min_completed_projects=0,
        requires_ground_up_build=False,
    )


def _lender(lender_id: int, name: str, *, priority: int, boxes: list[ProductBox], is_primary: bool = False,
            construction_min: Optional[int] = None, takes_ground_up: bool = False) -> LenderRow:
    return LenderRow(
        id=lender_id, name=name, active=True, is_primary=is_primary, agreement_status="signed",
        priority=priority, monthly_cap=None, construction_min_amount=construction_min,
        takes_ground_up=takes_ground_up, boxes=tuple(boxes),
    )


def _flip(loan: int, *, purchase: Optional[int] = None, rehab: Optional[int] = None,
          arv: Optional[int] = None) -> FileProfile:
    return FileProfile(loan_type=FLIP, loan_amount=loan, state="FL",
                       purchase_price=purchase, rehab_budget=rehab, arv=arv)


LTC_85 = _box(FLIP, max_ltc=0.85)
LTV_75 = _box(FLIP, max_ltv=0.75)


# ── LTC ──────────────────────────────────────────────────────────────────────

def test_a_100_percent_ltc_file_fails_an_85_percent_ltc_box_with_a_structured_reason():
    """Josh's Test 1: the file failed the box on its own numbers (LTC 100% over 85%)."""
    matches, reasons = _box_matches(LTC_85, _flip(300_000, purchase=250_000, rehab=50_000))
    assert not matches
    assert reasons == ["LTC_LIMIT_EXCEEDED (Requested: 100.0%, Max: 85%)"]


def test_ltc_under_the_cap_passes():
    assert _box_matches(LTC_85, _flip(240_000, purchase=250_000, rehab=50_000)) == (True, [])  # 80%


def test_ltc_exactly_at_the_cap_passes():
    assert _box_matches(LTC_85, _flip(255_000, purchase=250_000, rehab=50_000)) == (True, [])  # 85%


def test_a_zero_rehab_budget_is_a_real_figure_and_ltc_is_on_the_purchase_alone():
    assert _box_matches(LTC_85, _flip(240_000, purchase=250_000, rehab=0))[0] is False  # 96%


@pytest.mark.parametrize("purchase, rehab", [(None, 50_000), (250_000, None), (None, None)])
def test_ltc_is_not_applied_when_purchase_or_rehab_is_missing(purchase, rehab):
    assert _box_matches(LTC_85, _flip(300_000, purchase=purchase, rehab=rehab)) == (True, [])


# ── LTV ──────────────────────────────────────────────────────────────────────

def test_ltv_over_the_cap_fails_with_a_structured_reason():
    matches, reasons = _box_matches(LTV_75, _flip(320_000, arv=400_000))
    assert not matches
    assert reasons == ["LTV_LIMIT_EXCEEDED (Requested: 80.0%, Max: 75%)"]


def test_ltv_at_the_cap_passes_and_a_missing_arv_is_not_applied():
    assert _box_matches(LTV_75, _flip(300_000, arv=400_000)) == (True, [])
    assert _box_matches(LTV_75, _flip(900_000)) == (True, [])


def test_a_box_with_no_leverage_caps_ignores_the_figures():
    assert _box_matches(_box(FLIP), _flip(500_000, purchase=100_000, rehab=0, arv=100_000)) == (True, [])


def test_both_caps_breached_reports_both():
    box = _box(FLIP, max_ltv=0.75, max_ltc=0.85)
    _, reasons = _box_matches(box, _flip(300_000, purchase=250_000, rehab=50_000, arv=350_000))
    assert [r.split(" ")[0] for r in reasons] == ["LTV_LIMIT_EXCEEDED", "LTC_LIMIT_EXCEEDED"]


# ── Lender level ─────────────────────────────────────────────────────────────

def test_a_lender_passes_when_any_one_of_its_boxes_takes_the_leverage():
    lender = _lender(1, "Two Boxes", priority=1, boxes=[LTC_85, _box(FLIP, max_ltc=1.0)])
    assert lender_passes_box(lender, _flip(300_000, purchase=250_000, rehab=50_000))


def test_a_skipped_lender_logs_the_structured_reason(caplog):
    lender = _lender(7, "Tight", priority=1, boxes=[LTC_85])
    with caplog.at_level("INFO", logger="src.lending.routing.rules"):
        assert not lender_passes_box(lender, _flip(300_000, purchase=250_000, rehab=50_000))
    assert "lender_id=7 skipped: LTC_LIMIT_EXCEEDED (Requested: 100.0%, Max: 85%)" in caplog.text


def test_missing_leverage_facts_names_each_absent_figure():
    assert missing_leverage_facts(_flip(300_000)) == ["purchase_price", "rehab_budget", "arv"]
    assert missing_leverage_facts(_flip(300_000, purchase=250_000, rehab=0, arv=400_000)) == []


# ── Routing ──────────────────────────────────────────────────────────────────

@pytest.fixture
def choose(monkeypatch):
    def run(profile: FileProfile, lenders: list[LenderRow]):
        monkeypatch.setattr(router, "seat_at_cap", lambda lender, db: False)
        return router._choose(profile, lenders, [], db=None)
    return run


def test_the_waterfall_skips_a_lender_whose_ltc_cap_the_file_breaches(choose):
    primary = _lender(1, "Primary", priority=1, is_primary=True, boxes=[LTC_85])
    roomier = _lender(2, "Roomier", priority=2, boxes=[_box(FLIP, max_ltc=1.0)])
    chosen, _ = choose(_flip(300_000, purchase=250_000, rehab=50_000), [primary, roomier])
    assert chosen is roomier


def test_a_file_without_figures_still_routes_to_the_first_lender(choose):
    primary = _lender(1, "Primary", priority=1, is_primary=True, boxes=[LTC_85])
    chosen, _ = choose(_flip(300_000), [primary])
    assert chosen is primary


def test_large_ground_up_over_the_primarys_ltc_cap_cascades_as_a_box_fail(choose):
    primary = _lender(1, "Primary", priority=1, is_primary=True, construction_min=500_000,
                      boxes=[_box(GROUND_UP, max_ltc=0.85)])
    next_ground_up = _lender(2, "Next GU", priority=2, takes_ground_up=True,
                             boxes=[_box(GROUND_UP, max_ltc=0.90)])
    profile = FileProfile(loan_type=GROUND_UP, loan_amount=600_000, state="FL",
                          purchase_price=200_000, rehab_budget=470_000)  # LTC 89.6%
    assert choose(profile, [primary, next_ground_up]) == (next_ground_up, OVERFLOW_PRIMARY_BOX_FAIL)
