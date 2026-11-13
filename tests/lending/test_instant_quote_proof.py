"""Cora P2.2 done-when proof: the multi-guarantor run and the 4 Instant-Quote-Rules.md cases."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.lending.instant_quote import quote
from src.lending.instant_quote.proof import DOC_CASES, MULTI_GUARANTOR_CASE, render_markdown


def test_multi_guarantor_case_prices_off_the_lowest_fico_with_the_reserve():
    # 720 + 640 -> priced at 640: 85% - 15% = 70% x 250K = 175K; below 660 -> full-term reserve
    result = quote(MULTI_GUARANTOR_CASE.request)

    assert result.loan_amount == Decimal("175000")
    assert result.fee_surcharge_pct == Decimal("0.04")
    assert [c.code for c in result.conditions] == ["INTEREST_RESERVE_FULL_TERM"]


@pytest.mark.parametrize("case", DOC_CASES, ids=lambda c: c.name)
def test_documented_case_returns_the_exact_documented_loan_amount(case):
    if case.request is None or case.expected_loan is None:
        pytest.skip("awaiting claude/Instant-Quote-Rules.md (client Q1)")
    assert quote(case.request).loan_amount == case.expected_loan


def test_proof_table_lists_every_case_and_marks_pending_ones():
    table = render_markdown()

    assert "| Multi-guarantor (720 + 640) |" in table
    assert "175,000" in table
    assert table.count("awaiting Instant-Quote-Rules.md") == len(DOC_CASES)
