"""WP-9 Dial List — pure ranking tests. Fixture in, object out. No DB/IO."""
from datetime import date
from decimal import Decimal

import pytest

from src.services.dial_list import (
    DialCandidate,
    DialListConfig,
    rank_dial_list,
)

AS_OF = date(2026, 9, 16)


def _cand(**kw):
    base = dict(property_id=1, triggers=["financing_intent"])
    base.update(kw)
    return DialCandidate(**base)


# 1 — dollar ordering: big builder construction loan outranks small flip
def test_big_construction_outranks_small_flip():
    construction = _cand(
        property_id=10, opportunity_id="a", buyer_entity_id=100,
        triggers=["builder"], is_builder=True, intent_tier="medium",
        max_ltc=Decimal("0.80"), arv=Decimal("1125000"),  # loan 900k
    )
    flip = _cand(
        property_id=11, opportunity_id="b", buyer_entity_id=101,
        triggers=["financing_intent"], intent_tier="high",
        max_ltc=Decimal("0.80"), arv=Decimal("250000"),  # loan 200k
    )
    result = rank_dial_list([flip, construction], AS_OF)
    assert [e.property_id for e in result.entries] == [10, 11]
    assert result.entries[0].rank == 1
    assert result.entries[1].rank == 2


# 2 — builder multiplier floats a builder above an equal-base non-builder
def test_builder_multiplier_floats_builder():
    builder = _cand(property_id=1, buyer_entity_id=1, is_builder=True,
                    intent_tier="high", assessed_value_mkt=Decimal("300000"))
    plain = _cand(property_id=2, buyer_entity_id=2, is_builder=False,
                  intent_tier="high", assessed_value_mkt=Decimal("300000"))
    result = rank_dial_list([plain, builder], AS_OF)
    assert result.entries[0].property_id == 1
    assert result.entries[0].expected_revenue > result.entries[1].expected_revenue


# 3 — detector-only candidate (no intent tier) uses the probability floor
def test_detector_only_uses_floor():
    c = _cand(property_id=5, intent_tier=None, triggers=["auction_probate"],
              assessed_value_mkt=Decimal("200000"))
    result = rank_dial_list([c], AS_OF)
    assert result.entries[0].probability == Decimal("0.10")


# 4 — expected loan confidence: max_ltc×ARV high, fallback low
def test_expected_loan_confidence():
    arv_c = _cand(property_id=1, buyer_entity_id=1, intent_tier="high",
                  max_ltc=Decimal("0.75"), arv=Decimal("400000"))
    fallback_c = _cand(property_id=2, buyer_entity_id=2, intent_tier="high",
                       assessed_value_mkt=Decimal("400000"))
    result = rank_dial_list([arv_c, fallback_c], AS_OF)
    by_id = {e.property_id: e for e in result.entries}
    assert by_id[1].expected_loan_confidence == "high"
    assert by_id[1].expected_loan == Decimal("300000")  # 0.75 × 400k
    assert by_id[2].expected_loan_confidence == "low"


# 5 — no value bases: loan 0, still appears, not crashed
def test_no_value_base_still_ranks_at_floor():
    c = _cand(property_id=9, intent_tier="low", triggers=["out_of_state"])
    result = rank_dial_list([c], AS_OF)
    assert len(result.entries) == 1
    assert result.entries[0].expected_loan == Decimal("0")
    assert result.entries[0].expected_revenue == Decimal("0")
    assert result.entries[0].expected_loan_confidence == "low"


# 6 — dedup by resolved borrower: merge triggers, keep highest score
def test_dedup_by_borrower_keeps_winning_property_triggers_only():
    # Two different properties, same borrower. Fix 7: triggers are NOT merged
    # across properties — only the retained property's signals are shown.
    hi = _cand(property_id=1, buyer_entity_id=77, triggers=["cash_purchase"],
               intent_tier="high", assessed_value_mkt=Decimal("500000"))
    lo = _cand(property_id=2, buyer_entity_id=77, triggers=["out_of_state"],
               intent_tier="low", assessed_value_mkt=Decimal("100000"))
    result = rank_dial_list([hi, lo], AS_OF)
    assert len(result.entries) == 1
    entry = result.entries[0]
    assert entry.buyer_entity_id == 77
    assert entry.property_id == 1  # higher-scoring candidate kept
    assert "cash_purchase" in entry.triggers   # property 1's own trigger
    assert "out_of_state" not in entry.triggers  # property 2's trigger NOT merged


# 7 — unresolved borrowers dedup by property, distinct properties both appear
def test_unresolved_dedup_by_property():
    a = _cand(property_id=1, buyer_entity_id=None, intent_tier="high",
              assessed_value_mkt=Decimal("300000"))
    b = _cand(property_id=2, buyer_entity_id=None, intent_tier="high",
              assessed_value_mkt=Decimal("300000"))
    dup = _cand(property_id=1, buyer_entity_id=None, triggers=["out_of_state"],
                intent_tier="low", assessed_value_mkt=Decimal("50000"))
    result = rank_dial_list([a, b, dup], AS_OF)
    assert len(result.entries) == 2
    assert {e.property_id for e in result.entries} == {1, 2}
    prop1 = next(e for e in result.entries if e.property_id == 1)
    assert set(prop1.triggers) == {"financing_intent", "out_of_state"}
    assert all(e.borrower_resolved is False for e in result.entries)


# 8 — top-N cut and rank assignment
def test_top_n_cut_and_ranks():
    cands = [
        _cand(property_id=i, buyer_entity_id=i, intent_tier="high",
              assessed_value_mkt=Decimal(str(100000 * i)))
        for i in range(1, 6)
    ]
    result = rank_dial_list(cands, AS_OF, DialListConfig(list_size=2))
    assert len(result.entries) == 2
    assert [e.rank for e in result.entries] == [1, 2]
    assert result.candidate_count == 5
    # highest assessed value ranks first
    assert result.entries[0].property_id == 5


# 9 — determinism incl. stable tiebreak on equal scores
def test_determinism_and_stable_tiebreak():
    c1 = _cand(property_id=1, opportunity_id="z", buyer_entity_id=1,
               intent_tier="high", assessed_value_mkt=Decimal("300000"))
    c2 = _cand(property_id=2, opportunity_id="a", buyer_entity_id=2,
               intent_tier="high", assessed_value_mkt=Decimal("300000"))
    r1 = rank_dial_list([c1, c2], AS_OF)
    r2 = rank_dial_list([c2, c1], AS_OF)
    assert [e.property_id for e in r1.entries] == [e.property_id for e in r2.entries]
    # equal score → tiebreak by opportunity_id ("a" before "z")
    assert r1.entries[0].opportunity_id == "a"


# 10 — reason + talking points render fired triggers and relationship facts
def test_reason_and_talking_points():
    c = _cand(property_id=1, buyer_entity_id=1, triggers=["cash_purchase"],
              intent_tier="high", assessed_value_mkt=Decimal("300000"),
              properties_owned=4, last_deal_months_ago=8)
    entry = rank_dial_list([c], AS_OF).entries[0]
    assert "leverage" in entry.reason.lower()
    assert "Owns 4 properties" in entry.talking_points
    assert "Last sale 8 months ago" in entry.talking_points


# 11 — config overrides change output
def test_config_overrides():
    c = _cand(property_id=1, buyer_entity_id=1, is_builder=True,
              intent_tier="high", assessed_value_mkt=Decimal("300000"))
    base = rank_dial_list([c], AS_OF).entries[0].expected_revenue
    boosted = rank_dial_list(
        [c], AS_OF, DialListConfig(builder_multiplier=Decimal("3.0"))
    ).entries[0].expected_revenue
    assert boosted > base


# 12 — urgency: near urgency_date outranks far/absent, all else equal
def test_urgency_date_proximity():
    near = _cand(property_id=1, buyer_entity_id=1, intent_tier="high",
                 assessed_value_mkt=Decimal("300000"), urgency_date=AS_OF)
    none = _cand(property_id=2, buyer_entity_id=2, intent_tier="high",
                 assessed_value_mkt=Decimal("300000"), urgency_date=None)
    result = rank_dial_list([none, near], AS_OF)
    assert result.entries[0].property_id == 1
    assert result.entries[0].urgency > result.entries[1].urgency


# 13 — formula pinned numerically: loan enters once (via commission), not squared
def test_expected_revenue_formula_no_double_count():
    c = _cand(property_id=1, buyer_entity_id=1, intent_tier="high",
              max_ltc=Decimal("0.80"), arv=Decimal("500000"))  # loan 400k
    entry = rank_dial_list([c], AS_OF).entries[0]
    assert entry.expected_loan == Decimal("400000")
    assert entry.commission == Decimal("6000")  # 0.015 × 400k
    # probability(0.50) × commission(6000) × urgency(1.0) — loan is NOT squared
    assert entry.expected_revenue == Decimal("3000")


# 14 — no loan basis: caveat says the size is unavailable, not merely low-confidence
def test_no_loan_basis_flags_unavailable():
    c = _cand(property_id=1, buyer_entity_id=1, intent_tier="high")
    entry = rank_dial_list([c], AS_OF).entries[0]
    assert "No size estimate (insufficient data)" in entry.talking_points
    assert "low-confidence" not in " ".join(entry.talking_points)


# 15 — fallback loan basis is flagged via the confidence field, no jargon line
def test_fallback_loan_basis_flags_low_confidence():
    c = _cand(property_id=1, buyer_entity_id=1, intent_tier="high",
              assessed_value_mkt=Decimal("300000"))
    entry = rank_dial_list([c], AS_OF).entries[0]
    assert entry.expected_loan_confidence == "low"
    # confidence is surfaced in the digest header, not tagged in every line
    assert not any("fallback" in p for p in entry.talking_points)


# 16 — implausible loan basis is clipped to the cap and flagged low-confidence
def test_expected_loan_capped():
    huge = _cand(property_id=1, buyer_entity_id=1, intent_tier="high",
                 assessed_value_mkt=Decimal("944000000"))  # 0.70× → 660M
    entry = rank_dial_list([huge], AS_OF).entries[0]
    assert entry.expected_loan == Decimal("5000000")  # default max_expected_loan
    assert entry.expected_loan_confidence == "low"


# 17 — misleading relationship facts are suppressed (owns 0, stale sale proxy)
def test_relationship_facts_suppressed_when_empty_or_stale():
    c = _cand(property_id=1, buyer_entity_id=1, triggers=["cash_purchase"],
              intent_tier="high", assessed_value_mkt=Decimal("300000"),
              properties_owned=0, last_deal_months_ago=175)
    tp = rank_dial_list([c], AS_OF).entries[0].talking_points
    assert not any("Owns" in p for p in tp)        # owns 0 → hidden
    assert not any("Last sale" in p for p in tp)   # 175mo → too stale, hidden


def test_invalid_config_rejected():
    with pytest.raises(Exception):
        DialListConfig(list_size=0)
