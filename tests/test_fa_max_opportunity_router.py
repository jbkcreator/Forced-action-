"""WP-T2-11 — GYR Opportunity Router: unit tests.

Pure classify() tests — no DB, no network. Guards ADR 0001 against regression
to the squared expected-revenue formula.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from src.services.dial_list.config import DEFAULT_CONFIG
from src.services.dial_list.models import DialCandidate
from src.services.dial_list.rank import score_candidate
from src.services.opportunity_router.classify import classify
from src.services.opportunity_router.models import (
    GyrColor,
    RouterConfig,
    RouterContext,
    YellowReason,
    RedReason,
)

_TODAY = date(2026, 9, 21)
_CONFIG = RouterConfig(green_min_expected_revenue_cents=15_000)  # $150


def _ctx(**overrides) -> RouterContext:
    defaults = dict(
        opportunity_id="00000000-0000-0000-0000-000000000001",
        person_id="00000000-0000-0000-0000-000000000002",
        outcome="open",
        opportunity_type="acquisition",
        loan_amount_cents=300_000_00,  # $300k
        maturity_months=12,
        expected_need_date=None,
        lender_box_status="in_box",
        lender_box_fail_reasons=[],
        lender_box_uncertain_flags=[],
        arv=Decimal("350000"),
        arv_confidence="high",
        dial_candidate_assessed_value=Decimal("300000"),
        dial_candidate_last_sale_price=None,
        dial_candidate_arv=Decimal("350000"),
        dial_candidate_max_ltc=Decimal("0.75"),
        dial_candidate_loan_override=None,
        dial_candidate_loan_override_confidence=None,
        dial_candidate_triggers=["financing_intent"],
        dial_candidate_intent_tier="high",
        dial_candidate_is_builder=False,
        dial_candidate_urgency_date=None,
        suppressed=False,
        has_interaction=True,
        as_of=_TODAY,
    )
    defaults.update(overrides)
    return RouterContext(**defaults)


# ──────────────────────────────────────────────────────────────────────────────
# Green path
# ──────────────────────────────────────────────────────────────────────────────

def test_green_in_box_above_floor():
    decision = classify(_ctx(), _CONFIG)
    assert decision.color == GyrColor.GREEN
    assert decision.queue == "MONEY"
    assert decision.reason_codes == []


def test_green_ranked_above_smaller_green():
    big = classify(_ctx(dial_candidate_arv=Decimal("900000")), _CONFIG)
    small = classify(_ctx(dial_candidate_arv=Decimal("200000")), _CONFIG)
    assert big.color == GyrColor.GREEN
    assert small.color == GyrColor.GREEN
    assert big.expected_revenue_cents > small.expected_revenue_cents


# ──────────────────────────────────────────────────────────────────────────────
# Yellow paths
# ──────────────────────────────────────────────────────────────────────────────

def test_yellow_below_revenue_floor():
    # in_box but ARV so small that expected_revenue < floor
    decision = classify(
        _ctx(
            dial_candidate_arv=Decimal("10000"),
            dial_candidate_assessed_value=Decimal("10000"),
            dial_candidate_max_ltc=None,
        ),
        _CONFIG,
    )
    assert decision.color == GyrColor.YELLOW
    assert decision.queue == "MONEY"
    assert YellowReason.BELOW_REVENUE_FLOOR.value in decision.reason_codes


def test_yellow_missing_arv():
    decision = classify(_ctx(arv=None, arv_confidence=None), _CONFIG)
    assert decision.color == GyrColor.YELLOW
    assert YellowReason.MISSING_ARV.value in decision.reason_codes


def test_yellow_low_arv_confidence():
    decision = classify(_ctx(arv_confidence="low"), _CONFIG)
    assert decision.color == GyrColor.YELLOW
    assert YellowReason.LOW_ARV_CONFIDENCE.value in decision.reason_codes


def test_yellow_borrower_not_contacted():
    decision = classify(_ctx(has_interaction=False), _CONFIG)
    assert decision.color == GyrColor.YELLOW
    assert YellowReason.BORROWER_NOT_CONTACTED.value in decision.reason_codes


def test_yellow_uncertain_lender_box():
    decision = classify(
        _ctx(lender_box_status="uncertain", lender_box_uncertain_flags=["program_mismatch"]),
        _CONFIG,
    )
    assert decision.color == GyrColor.YELLOW
    assert decision.queue == "MONEY"


# ──────────────────────────────────────────────────────────────────────────────
# Red paths
# ──────────────────────────────────────────────────────────────────────────────

def test_red_terminal_funded():
    decision = classify(_ctx(outcome="funded"), _CONFIG)
    assert decision.color == GyrColor.RED
    assert decision.queue is None  # audit-only, not posted to EXCEPTIONS
    assert RedReason.ALREADY_FUNDED.value in decision.reason_codes


def test_red_terminal_dead():
    decision = classify(_ctx(outcome="dead"), _CONFIG)
    assert decision.color == GyrColor.RED
    assert decision.queue is None


def test_red_terminal_recycled():
    decision = classify(_ctx(outcome="recycled"), _CONFIG)
    assert decision.color == GyrColor.RED
    assert decision.queue is None


def test_red_suppressed():
    decision = classify(_ctx(suppressed=True), _CONFIG)
    assert decision.color == GyrColor.RED
    assert decision.queue == "EXCEPTIONS"
    assert RedReason.BORROWER_SUPPRESSED.value in decision.reason_codes


def test_red_out_of_box():
    decision = classify(
        _ctx(
            lender_box_status="out_of_box",
            lender_box_fail_reasons=["state_not_fl", "ltv_exceeds_max"],
        ),
        _CONFIG,
    )
    assert decision.color == GyrColor.RED
    assert decision.queue == "EXCEPTIONS"
    assert RedReason.OUT_OF_BOX.value in decision.reason_codes
    assert "state_not_fl" in decision.reason_codes
    assert decision.disqualifying_rule == "state_not_fl"


# ──────────────────────────────────────────────────────────────────────────────
# ADR 0001 guard — expected revenue matches dial-list score_candidate exactly
# ──────────────────────────────────────────────────────────────────────────────

def test_expected_revenue_matches_dial_list_scorer():
    """Router and dial-list must always agree on the dollar value (ADR 0001).

    If this test fails, the router may have drifted to the squared formula.
    """
    ctx = _ctx()
    decision = classify(ctx, _CONFIG)

    candidate = DialCandidate(
        property_id=0,
        opportunity_id=ctx.opportunity_id,
        triggers=["financing_intent"],
        intent_tier="high",
        arv=ctx.dial_candidate_arv,
        max_ltc=ctx.dial_candidate_max_ltc,
        assessed_value_mkt=ctx.dial_candidate_assessed_value,
        is_builder=False,
    )
    dial_revenue = score_candidate(candidate, _TODAY, DEFAULT_CONFIG)
    dial_cents = int(dial_revenue * 100)

    assert decision.expected_revenue_cents == dial_cents, (
        f"Router ({decision.expected_revenue_cents}¢) disagrees with dial-list "
        f"scorer ({dial_cents}¢) — check ADR 0001 / classify.py"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Priority: terminal beats suppressed beats out-of-box
# ──────────────────────────────────────────────────────────────────────────────

def test_terminal_beats_suppressed():
    decision = classify(_ctx(outcome="funded", suppressed=True), _CONFIG)
    assert decision.color == GyrColor.RED
    assert decision.queue is None  # terminal wins: no EXCEPTIONS post


def test_suppressed_beats_out_of_box():
    decision = classify(
        _ctx(suppressed=True, lender_box_status="out_of_box"),
        _CONFIG,
    )
    assert decision.color == GyrColor.RED
    assert RedReason.BORROWER_SUPPRESSED.value in decision.reason_codes
