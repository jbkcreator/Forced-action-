"""Cora P2.2: instant quote engine (pure). Expected figures are worked by hand from Central Lending's
placeholder box (LTC 85%, ARV cap 70%, min loan $75K), the soft-approval placeholder shares
(85% of purchase, 100% of rehab), and Central's FICO grid from Josh's Nov 2 checklist (700+: no cut, no fee;
640-659: -2.5% LTC, +0.5% fee; 620-639: -5%, +1.25%) and experience cuts (0 deals -5%, 1-2 deals -2.5%).
ZIP 33602 is Hillsborough (not rural)."""
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
    # FICO 720, 3 deals: no cuts. 85% x 250K = 212.5K binds (requested 220K, ARV 280K); round down to $1K
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
    assert quote(_request(state="GA", zip_code="30303", borrower_type="individual")).status == "indicative"


def test_state_outside_the_lending_footprint_is_rejected():
    result = quote(_request(state="NY", zip_code="10001"))

    assert result.status == "rejected"
    assert result.reason == "STATE_NOT_ELIGIBLE"


def test_central_has_no_credit_floor():
    # Central's grid prices down to under 600: FICO 550, 3 deals -> -15%: 70% x 250K = 175K
    result = quote(_request(guarantors=(Guarantor(fico=550, completed_deals=3),)))

    assert result.status == "indicative"
    assert result.loan_amount == Decimal("175000")
    assert result.fee_surcharge_pct == Decimal("0.04")


# --- 04 pricing FICO + interest reserve ---

def test_fico_660_and_above_carries_no_interest_reserve():
    assert quote(_request(guarantors=(Guarantor(fico=660),))).conditions == ()


def test_pricing_fico_below_660_requires_a_full_term_interest_reserve_with_no_amount_yet():
    result = quote(_request(guarantors=(Guarantor(fico=659),)))

    assert [c.code for c in result.conditions] == ["INTEREST_RESERVE_FULL_TERM"]
    assert result.conditions[0].amount is None


def test_multi_guarantor_quote_prices_off_the_lowest_fico():
    # priced at 640 (640-659: -2.5% LTC, +0.5% fee); experience 5 deals: no cut. 82.5% x 250K = 206.25K
    result = quote(_request(guarantors=(Guarantor(fico=720, completed_deals=5), Guarantor(fico=640))))

    assert result.status == "indicative"
    assert result.loan_amount == Decimal("206000")
    assert result.fee_surcharge_pct == Decimal("0.005")
    assert [c.code for c in result.conditions] == ["INTEREST_RESERVE_FULL_TERM"]


def test_interest_reserve_amount_once_rate_and_term_are_set(monkeypatch):
    # FICO 640, 3 deals: 82.5% x 250K = 206.25K -> 206K; 206K x 12% x 12/12 months = 24,720
    monkeypatch.setattr("src.lending.instant_quote.engine.INTEREST_RESERVE_RATE", Decimal("0.12"))
    monkeypatch.setattr("src.lending.instant_quote.engine.INTEREST_RESERVE_TERM_MONTHS", 12)

    result = quote(_request(guarantors=(Guarantor(fico=640, completed_deals=3),)))

    assert result.conditions[0].amount == Decimal("24720")


# --- 05 pricing grid ---

def test_grid_tier_cut_and_fee_are_listed_without_naming_the_tier():
    # FICO 630 (620-639): -5% LTC, +1.25% fee; 3 deals: no experience cut. 80% x 250K = 200K
    result = quote(_request(guarantors=(Guarantor(fico=630, completed_deals=3),)))

    assert [(c.reason, c.pct) for c in result.cuts] == [("PRICING_GRID", Decimal("-0.05"))]
    assert result.fee_surcharge_pct == Decimal("0.0125")
    assert result.loan_amount == Decimal("200000")


def test_700_and_up_prices_at_the_full_ltc_with_no_fee():
    result = quote(_request(guarantors=(Guarantor(fico=780, completed_deals=3),)))

    assert result.cuts == ()
    assert result.loan_amount == Decimal("212000")
    assert result.fee_surcharge_pct == Decimal("0")


def test_680_to_699_adds_a_fee_but_no_ltc_cut():
    result = quote(_request(guarantors=(Guarantor(fico=690, completed_deals=3),)))

    assert result.cuts == ()
    assert result.fee_surcharge_pct == Decimal("0.0025")


def test_stacked_cuts_never_take_the_ltc_below_zero(monkeypatch):
    from config.lending_instant_quote import GridTier

    monkeypatch.setattr("src.lending.instant_quote.engine.PRICING_GRID", (GridTier(0, Decimal("-0.95"), Decimal("0")),))
    result = quote(_request())

    assert result.status == "rejected"
    assert result.reason == "LEVERAGE_EXHAUSTED"


# --- 07 CFPB rural + refinance ---

def test_rural_county_takes_the_rural_cut():
    # 32064 = Live Oak, Suwannee County FL (12121), on the 2026 CFPB rural/underserved list
    result = quote(_request(zip_code="32064"))

    assert ("CFPB_RURAL", Decimal("-0.05")) in [(c.reason, c.pct) for c in result.cuts]
    # 85% - 5% = 80% x 250K = 200K
    assert result.loan_amount == Decimal("200000")


def test_metro_county_takes_no_rural_cut():
    assert "CFPB_RURAL" not in [c.reason for c in quote(_request()).cuts]


def test_zip_outside_the_crosswalk_is_noted_and_takes_no_rural_cut():
    result = quote(_request(zip_code="00000"))

    assert "CFPB_RURAL" not in [c.reason for c in result.cuts]
    assert "RURAL_STATUS_UNKNOWN" in result.rules_fired


def test_refinance_takes_the_refinance_cut():
    # 85% - 5% = 80% x 250K = 200K
    result = quote(_request(purpose="refinance", existing_debt=Decimal("150000")))

    assert ("REFINANCE", Decimal("-0.05")) in [(c.reason, c.pct) for c in result.cuts]
    assert result.loan_amount == Decimal("200000")


def test_purchase_takes_no_refinance_cut():
    assert "REFINANCE" not in [c.reason for c in quote(_request()).cuts]


# --- 06 experience + Backflip new-construction rule (off) ---

def test_one_to_two_deals_takes_the_experience_haircut():
    # 85% - 2.5% experience = 82.5% x 250K = 206.25K
    result = quote(_request(guarantors=(Guarantor(fico=720, completed_deals=2, exits=2),)))

    assert [(c.reason, c.pct) for c in result.cuts] == [("EXPERIENCE", Decimal("-0.025"))]
    assert result.loan_amount == Decimal("206000")


def test_no_completed_deals_takes_the_larger_experience_cut():
    # 85% - 5% experience = 80% x 250K = 200K
    result = quote(_request(guarantors=(Guarantor(fico=720, completed_deals=0),)))

    assert [(c.reason, c.pct) for c in result.cuts] == [("EXPERIENCE", Decimal("-0.05"))]
    assert result.loan_amount == Decimal("200000")


def test_experience_comes_from_the_most_experienced_guarantor():
    result = quote(_request(guarantors=(Guarantor(fico=720, completed_deals=1), Guarantor(fico=730, completed_deals=4))))

    assert "EXPERIENCE" not in [c.reason for c in result.cuts]


def test_new_construction_without_an_exit_is_quoted_while_the_rule_is_off():
    result = quote(_request(purpose="new_construction", guarantors=(Guarantor(fico=720, completed_deals=3, exits=0),)))

    assert result.status == "indicative"
    assert "NEW_CONSTRUCTION_NEEDS_EXIT" not in result.rules_fired


def test_new_construction_needs_an_exit_once_the_rule_is_switched_on(monkeypatch):
    monkeypatch.setattr("src.lending.instant_quote.engine.NEW_CONSTRUCTION_EXIT_RULE_ENABLED", True)

    no_exit = quote(_request(purpose="new_construction", guarantors=(Guarantor(fico=720, completed_deals=3, exits=0),)))
    one_exit = quote(_request(purpose="new_construction", guarantors=(Guarantor(fico=720, completed_deals=3, exits=1),)))

    assert no_exit.status == "rejected"
    assert no_exit.reason == "NEW_CONSTRUCTION_NEEDS_EXIT"
    assert one_exit.status == "indicative"


def test_zip_in_another_state_is_rejected():
    # 30303 is Atlanta, GA
    result = quote(_request(state="FL", zip_code="30303"))

    assert result.status == "rejected"
    assert result.reason == "ZIP_STATE_MISMATCH"
