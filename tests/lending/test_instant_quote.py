"""Cora P2.2: instant quote engine (pure). Expected figures are worked by hand from Central Lending's
placeholder box (LTC 85%, ARV cap 70%, min loan $75K) and the soft-approval placeholder shares
(85% of purchase, 100% of rehab)."""
from __future__ import annotations

from decimal import Decimal

from src.lending.instant_quote import Guarantor, QuoteRequest, quote


def _request(**overrides) -> QuoteRequest:
    base = dict(
        state="FL", zip_code="33602", borrower_type="entity",
        guarantors=(Guarantor(fico=720, completed_deals=3, exits=3),),
        purpose="purchase", purchase_price=Decimal("200000"), rehab=Decimal("50000"),
        arv=Decimal("400000"), existing_debt=None,
    )
    base.update(overrides)
    return QuoteRequest(**base)


def test_plain_purchase_is_sized_by_the_ltc_cap():
    # requested 200K x 85% + 50K = 220K; LTC 85% x 250K = 212.5K binds; ARV 70% x 400K = 280K; round down to $1K
    result = quote(_request())

    assert result.status == "indicative"
    assert result.reason is None
    assert result.loan_amount == Decimal("212000")
    assert result.rehab_funding == Decimal("50000")
    assert result.purchase_advance == Decimal("162000")
    assert result.effective_ltc == Decimal("0.85")


def test_quote_is_always_indicative_and_flagged_unconfirmed_while_central_is_unverified():
    result = quote(_request())

    assert result.disclaimer == "Indicative, subject to underwriting"
    assert result.unconfirmed is True


def test_arv_cap_binds_when_arv_is_low():
    # LTC 85% x 250K = 212.5K; ARV 70% x 260K = 182K binds
    result = quote(_request(arv=Decimal("260000")))

    assert result.loan_amount == Decimal("182000")
    assert result.rehab_funding == Decimal("50000")
    assert result.purchase_advance == Decimal("132000")


def test_borrower_score_is_returned_for_information():
    # experience 3 deals = 3 points, FL = 2 points, a $212K loan is under $300K, contract unknown
    assert quote(_request()).borrower_score == 5


def test_loan_below_the_minimum_is_rejected_not_raised():
    # LTC 85% x 60K = 51K < $75K minimum
    result = quote(_request(purchase_price=Decimal("50000"), rehab=Decimal("10000"), arv=Decimal("120000")))

    assert result.status == "rejected"
    assert result.reason == "LOAN_TOO_SMALL"
    assert result.loan_amount is None
