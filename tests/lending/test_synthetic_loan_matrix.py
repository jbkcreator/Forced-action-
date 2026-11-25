"""R1 0.10: the synthetic loan matrix, 3 loan types x 7 edge states, run on every PR in CI.

Each fixture (tests/fixtures/synthetic_loans.py) goes through the check that already exists for its edge state.
The expired-document and cross-borrower fixtures carry their data now; the validator that blocks them is R1 1.7
(packaging/packet_validator.py), which adds its tests over ``loans(edge_case=...)``.
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from config.lending_instant_quote import REFINANCE_RULE_PENDING
from packages.agent_core.lane_guard import lane_violation
from src.lending.contracts import LoanFile
from src.lending.instant_quote import Guarantor, quote
from tests.fixtures.synthetic_loans import AS_OF, EDGE_CASES, PRODUCTS, all_loans, loans


def test_matrix_is_complete_and_deterministic():
    first, second = all_loans(), all_loans()
    assert len(first) == 21
    assert {(l.product, l.edge_case) for l in first} == {(p, c) for p in PRODUCTS for c in EDGE_CASES}
    assert first == second
    assert len({l.loan_file.borrower.phone for l in first}) == 21
    assert len({l.loan_file.property.address for l in first}) == 21


@pytest.mark.parametrize("loan", all_loans(), ids=lambda l: l.key)
def test_every_fixture_round_trips_the_strict_contract(loan):
    assert LoanFile.model_validate(loan.loan_file.model_dump()) == loan.loan_file
    assert loan.loan_file.schema_version == 1


@pytest.mark.parametrize("loan", loans(edge_case="incomplete"), ids=lambda l: l.key)
def test_incomplete_fixtures_keep_unknown_inputs_none(loan):
    f = loan.loan_file
    assert (f.loan_amount, f.purchase_price, f.rehab_budget, f.arv, f.payoff_balance) == (None,) * 5
    assert loan.quote_request is None


@pytest.mark.parametrize("loan", loans(edge_case="conflicting_fico"), ids=lambda l: l.key)
def test_quote_prices_off_the_lowest_guarantor_fico(loan, monkeypatch):
    monkeypatch.setattr("src.lending.instant_quote.engine.REFINANCE_QUOTES_ENABLED", True)
    both = quote(loan.quote_request)
    low = quote(replace(loan.quote_request, guarantors=(Guarantor(640, 4, 2),)))
    high = quote(replace(loan.quote_request, guarantors=(Guarantor(760, 4, 2),)))
    assert both.status == "indicative"
    assert (both.loan_amount, both.fee_surcharge_pct, both.conditions) == (
        low.loan_amount, low.fee_surcharge_pct, low.conditions)
    assert (both.loan_amount, both.fee_surcharge_pct) != (high.loan_amount, high.fee_surcharge_pct)


@pytest.mark.parametrize("loan", loans(edge_case="clean"), ids=lambda l: l.key)
def test_clean_fixtures_quote_as_the_engine_allows_today(loan):
    result = quote(loan.quote_request)
    if loan.product == "refinance":
        assert (result.status, result.reason) == ("rejected", REFINANCE_RULE_PENDING)
    else:
        assert result.status == "indicative"


def test_only_backflip_fixtures_trip_the_lane_guard():
    hits = {l.key for l in all_loans()
            if lane_violation({"file": l.loan_file.model_dump(mode="json"), "draft": l.outbound_draft})}
    assert hits == {l.key for l in loans(edge_case="backflip_tagged")}


@pytest.mark.parametrize("loan", loans(edge_case="expired_documents"), ids=lambda l: l.key)
def test_expired_document_fixtures_hold_a_stale_statement_and_an_expired_pricing_sheet(loan):
    docs = {d.kind: d for d in loan.documents}
    assert (AS_OF - docs["bank_statement"].dated).days > 60
    assert docs["pricing_sheet"].expires_on < AS_OF


@pytest.mark.parametrize("loan", loans(edge_case="cross_borrower_document"), ids=lambda l: l.key)
def test_cross_borrower_fixtures_hold_another_persons_statement(loan):
    b = loan.loan_file.borrower
    assert any(d.owner_name != f"{b.first_name} {b.last_name}" for d in loan.documents if d.kind == "bank_statement")


def test_fixtures_outside_the_document_cases_carry_only_current_own_documents():
    for l in all_loans():
        if l.edge_case in ("expired_documents", "cross_borrower_document"):
            continue
        b = l.loan_file.borrower
        for d in l.documents:
            assert d.owner_name == f"{b.first_name} {b.last_name}", l.key
            assert (AS_OF - d.dated).days <= 60 and d.expires_on is None, l.key
