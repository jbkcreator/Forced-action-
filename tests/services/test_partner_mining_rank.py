"""
Tests for rank.rank_partners.
Seam: pure function — list of PartnerRow dicts → ordered list with rank +
      status assigned (top-25 = 'active', rest = 'identified').
"""
from datetime import date

import pytest

from src.services.partner_mining.rank import rank_partners, PartnerRow


def _row(name: str, partner_class: str, count: int,
         last_observed: date = date(2024, 1, 1),
         cash_volume: float = 0.0) -> PartnerRow:
    return PartnerRow(
        buyer_entity_id=hash(name),
        canonical_name=name,
        partner_class=partner_class,
        observed_transaction_count=count,
        last_observed_at=last_observed,
        total_cash_volume=cash_volume,
    )


# ---------------------------------------------------------------------------
# basic ordering
# ---------------------------------------------------------------------------

def test_higher_count_ranks_first():
    rows = [
        _row("LOW VOLUME LLC", "lender", count=5),
        _row("HIGH VOLUME LLC", "lender", count=20),
    ]
    ranked = rank_partners(rows)
    assert ranked[0].canonical_name == "HIGH VOLUME LLC"
    assert ranked[0].rank == 1
    assert ranked[1].rank == 2


def test_rank_1_is_top():
    rows = [_row("ONLY ONE LLC", "lender", count=10)]
    ranked = rank_partners(rows)
    assert ranked[0].rank == 1


# ---------------------------------------------------------------------------
# tie-breaking
# ---------------------------------------------------------------------------

def test_tie_broken_by_last_observed_desc():
    rows = [
        _row("OLDER LLC", "wholesaler", count=10, last_observed=date(2023, 6, 1)),
        _row("NEWER LLC", "wholesaler", count=10, last_observed=date(2024, 1, 1)),
    ]
    ranked = rank_partners(rows)
    assert ranked[0].canonical_name == "NEWER LLC"


def test_second_tiebreak_cash_volume():
    rows = [
        _row("LOW CASH LLC", "lender", count=5, last_observed=date(2024, 1, 1), cash_volume=50_000),
        _row("HIGH CASH LLC", "lender", count=5, last_observed=date(2024, 1, 1), cash_volume=500_000),
    ]
    ranked = rank_partners(rows)
    assert ranked[0].canonical_name == "HIGH CASH LLC"


# ---------------------------------------------------------------------------
# status assignment (top-25 active, rest identified)
# ---------------------------------------------------------------------------

def test_top_25_get_active_status():
    rows = [_row(f"LENDER {i} LLC", "lender", count=100 - i) for i in range(30)]
    ranked = rank_partners(rows)
    active = [r for r in ranked if r.status == "active"]
    identified = [r for r in ranked if r.status == "identified"]
    assert len(active) == 25
    assert len(identified) == 5


def test_fewer_than_25_all_active():
    rows = [_row(f"LENDER {i} LLC", "lender", count=10 - i) for i in range(10)]
    ranked = rank_partners(rows)
    assert all(r.status == "active" for r in ranked)


# ---------------------------------------------------------------------------
# per-class isolation (each class ranked independently)
# ---------------------------------------------------------------------------

def test_each_class_ranked_independently():
    rows = [
        _row("TOP LENDER LLC", "lender", count=50),
        _row("LOW LENDER LLC", "lender", count=5),
        _row("TOP WHOLESALER LLC", "wholesaler", count=30),
    ]
    ranked = rank_partners(rows)
    lenders = [r for r in ranked if r.partner_class == "lender"]
    wholesalers = [r for r in ranked if r.partner_class == "wholesaler"]
    assert lenders[0].rank == 1 and lenders[0].canonical_name == "TOP LENDER LLC"
    assert wholesalers[0].rank == 1 and wholesalers[0].canonical_name == "TOP WHOLESALER LLC"


# ---------------------------------------------------------------------------
# non-destructive: dropout keeps row, rank updated
# ---------------------------------------------------------------------------

def test_result_preserves_all_rows():
    rows = [_row(f"LENDER {i} LLC", "lender", count=i) for i in range(30)]
    ranked = rank_partners(rows)
    assert len(ranked) == 30
