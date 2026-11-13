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


# --- 03 eligibility rejections ---

def test_individual_borrower_on_a_florida_deal_needs_an_llc():
    result = quote(_request(borrower_type="individual"))

    assert result.status == "rejected"
    assert result.reason == "NEEDS_LLC_FORMATION"
    assert result.loan_amount is None


def test_individual_borrower_outside_florida_is_quoted():
    assert quote(_request(state="GA", borrower_type="individual")).status == "indicative"


def test_state_outside_the_lending_footprint_is_rejected():
    result = quote(_request(state="NY"))

    assert result.status == "rejected"
    assert result.reason == "STATE_NOT_ELIGIBLE"


def test_placeholder_credit_floor_does_not_reject():
    # Central's 680 floor is a placeholder (client Q2: conflicts with the 660 reserve rule); quote stays unconfirmed
    result = quote(_request(guarantors=(Guarantor(fico=650),)))

    assert result.status == "indicative"
    assert result.unconfirmed is True


# --- 04 pricing FICO + interest reserve ---

def test_fico_660_and_above_carries_no_interest_reserve():
    assert quote(_request(guarantors=(Guarantor(fico=660),))).conditions == ()


def test_pricing_fico_below_660_requires_a_full_term_interest_reserve_with_no_amount_yet():
    result = quote(_request(guarantors=(Guarantor(fico=659),)))

    assert [c.code for c in result.conditions] == ["INTEREST_RESERVE_FULL_TERM"]
    assert result.conditions[0].amount is None


def test_multi_guarantor_quote_prices_off_the_lowest_fico():
    result = quote(_request(guarantors=(Guarantor(fico=720, completed_deals=5), Guarantor(fico=640))))

    assert result.status == "indicative"
    assert result.loan_amount == Decimal("212000")
    assert [c.code for c in result.conditions] == ["INTEREST_RESERVE_FULL_TERM"]


def test_interest_reserve_amount_once_rate_and_term_are_set(monkeypatch):
    # 212K x 12% x 12/12 months = 25,440
    monkeypatch.setattr("src.lending.instant_quote.engine.INTEREST_RESERVE_RATE", Decimal("0.12"))
    monkeypatch.setattr("src.lending.instant_quote.engine.INTEREST_RESERVE_TERM_MONTHS", 12)

    result = quote(_request(guarantors=(Guarantor(fico=640),)))

    assert result.conditions[0].amount == Decimal("25440")
