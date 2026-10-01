"""Deterministic 1-10 lead score, provenance labels, caller questions and queue order."""
from __future__ import annotations

import random
from dataclasses import replace
from datetime import date
from decimal import Decimal

from src.lending.lead_scoring import LeadSignals, RankedLead, Signal, order_queue, score_lead

TODAY = date(2026, 10, 5)
SOON = date(2026, 11, 20)

TOP = LeadSignals(
    maturity_date=Signal.known(SOON),
    entity_status=Signal.known("ACTIVE"),
    equity_pct=Signal.known(Decimal("40")),
    decision_maker_confirmed=Signal.known(True),
)


def _score(signals):
    return score_lead(signals, today=TODAY)


def test_all_four_core_signals_known_and_met_is_rank_ten():
    result = _score(TOP)
    assert result.rank == 10
    assert len(result.signals_met) == 4


def test_nothing_known_is_rank_one():
    result = _score(LeadSignals())
    assert result.rank == 1
    assert set(result.labels.values()) == {"missing"}


def test_estimated_maturity_earns_no_rank_and_becomes_a_question():
    result = _score(replace(TOP, maturity_date=Signal.estimated(SOON)))
    assert result.rank == 7
    assert "maturity_within_window" not in result.signals_met
    assert result.caller_questions == ("Is the loan coming due around November 2026?",)
    assert result.labels["maturity_date"] == "estimated"


def test_estimated_maturity_outside_the_window_asks_nothing():
    result = _score(LeadSignals(maturity_date=Signal.estimated(date(2027, 6, 1))))
    assert result.caller_questions == ()


def test_known_maturity_outside_the_window_does_not_count():
    assert _score(replace(TOP, maturity_date=Signal.known(date(2027, 6, 1)))).rank == 7


def test_estimated_equity_does_not_count():
    assert _score(replace(TOP, equity_pct=Signal.estimated(Decimal("60")))).rank == 7


def test_equity_must_be_above_the_threshold():
    assert _score(replace(TOP, equity_pct=Signal.known(Decimal("35")))).rank == 7


def test_inactive_entity_does_not_count():
    assert _score(replace(TOP, entity_status=Signal.known("INACTIVE"))).rank == 7


def test_ladder_below_the_top():
    one = LeadSignals(entity_status=Signal.known("ACTIVE"))
    two = replace(one, equity_pct=Signal.known(Decimal("50")))
    assert (_score(one).rank, _score(two).rank) == (3, 5)


def test_repeat_operator_and_large_loan_lift_the_rank():
    base = LeadSignals(entity_status=Signal.known("ACTIVE"))
    lifted = replace(base, entity_property_count=Signal.known(3), loan_amount=Signal.known(Decimal("450000")))
    result = _score(lifted)
    assert result.rank == 5
    assert result.bonuses == ("repeat_operator", "large_loan")


def test_recent_permits_also_mark_a_repeat_operator():
    result = _score(LeadSignals(entity_recent_permit_count=Signal.known(2)))
    assert result.bonuses == ("repeat_operator",)


def test_bonuses_never_reach_rank_ten():
    three_met = replace(TOP, decision_maker_confirmed=Signal.missing(),
                        entity_property_count=Signal.known(5), loan_amount=Signal.known(Decimal("900000")))
    assert _score(three_met).rank == 9


def test_single_property_small_loan_earns_no_bonus():
    result = _score(LeadSignals(entity_property_count=Signal.known(1), loan_amount=Signal.known(Decimal("150000"))))
    assert result.bonuses == ()


def test_every_input_has_a_label():
    labels = _score(LeadSignals()).labels
    assert set(labels) == set(LeadSignals.__dataclass_fields__)


def _leads(n):
    return [RankedLead(item=f"lead-{i}", rank=10 - (i % 10)) for i in range(n)]


def test_queue_is_sorted_by_rank_without_exploration():
    queue = order_queue(_leads(10), rng=random.Random(1), exploration_share=0)
    ranks = {f"lead-{i}": 10 - (i % 10) for i in range(10)}
    assert [ranks[item] for item in queue] == sorted(ranks.values(), reverse=True)


def test_exploration_slice_comes_from_the_lower_half_and_keeps_every_lead():
    leads = _leads(40)
    queue = order_queue(leads, rng=random.Random(7), exploration_share=0.1)
    assert sorted(queue) == sorted(lead.item for lead in leads)
    ranked = sorted(leads, key=lambda lead: -lead.rank)
    lower_half = {lead.item for lead in ranked[20:]}
    top_block = queue[:10]
    assert any(item in lower_half for item in top_block)


def test_queue_order_is_reproducible_with_the_same_seed():
    leads = _leads(30)
    assert order_queue(leads, rng=random.Random(3)) == order_queue(leads, rng=random.Random(3))


def test_tiebreak_orders_equal_ranks():
    leads = [RankedLead("small", 5, Decimal("100")), RankedLead("big", 5, Decimal("900"))]
    assert order_queue(leads, rng=random.Random(1), exploration_share=0) == ["big", "small"]
