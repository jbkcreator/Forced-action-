"""Instant quote engine (Cora P2.2): deal facts + guarantor FICOs in, an indicative quote out.

Pure: no database, no clock, no I/O. Base caps come from the lender matrix and the loan is sized by the
soft-approval calculator (unchanged). A quote is never more than "indicative, subject to underwriting".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Optional

from config.lender_matrix import LENDER_MATRIX, LenderRules, effective_rules, fully_confirmed
from config.lender_matrix import field_status_of
from config.lending_instant_quote import (
    CALCULATION_REJECTION_CODES,
    DISCLAIMER,
    ENTITY_ONLY_STATES,
    EXPERIENCE_CUT_REASON,
    EXPERIENCE_HAIRCUT,
    EXPERIENCE_HAIRCUT_DEALS,
    FICO_BELOW_FLOOR,
    GRID_CUT_REASON,
    INTEREST_RESERVE_CONDITION,
    INTEREST_RESERVE_FICO_THRESHOLD,
    INTEREST_RESERVE_RATE,
    INTEREST_RESERVE_TERM_MONTHS,
    LEVERAGE_ADJUSTMENTS_CONFIRMED,
    NEEDS_LLC_FORMATION,
    NEW_CONSTRUCTION_EXIT_RULE_ENABLED,
    NEW_CONSTRUCTION_MIN_EXITS,
    NEW_CONSTRUCTION_NEEDS_EXIT,
    PRICING_GRID,
    PRICING_GRID_CONFIRMED,
    QUOTE_LENDER_KEY,
    QUOTE_UNAVAILABLE,
    REFINANCE_CUT,
    REFINANCE_CUT_REASON,
    RURAL_CUT,
    RURAL_CUT_REASON,
    RURAL_UNKNOWN_NOTE,
    STATE_NOT_ELIGIBLE,
)
from src.lending.contracts import FieldStatus
from src.lending.instant_quote.rural import is_rural
from config.lending_soft_approval import SOFT_APPROVAL_PARAMS, resolve_params
from src.lending.borrower_score import BorrowerFacts, score_borrower
from src.lending.soft_approval.calc import CalculationUnavailable, LenderTerms, calculate_figures

BorrowerType = Literal["individual", "entity"]
Purpose = Literal["purchase", "refinance", "new_construction"]
QuoteStatus = Literal["indicative", "rejected"]


@dataclass(frozen=True)
class Guarantor:
    fico: int
    completed_deals: int = 0
    exits: int = 0


@dataclass(frozen=True)
class QuoteRequest:
    state: str
    zip_code: str
    borrower_type: BorrowerType
    guarantors: tuple[Guarantor, ...]
    purpose: Purpose
    purchase_price: Decimal
    rehab: Decimal
    arv: Decimal
    existing_debt: Optional[Decimal] = None


@dataclass(frozen=True)
class Cut:
    reason: str
    pct: Decimal  # negative: a leverage cut, 0 to 1 scale


@dataclass(frozen=True)
class Condition:
    code: str
    amount: Optional[Decimal] = None


@dataclass(frozen=True)
class QuoteResult:
    status: QuoteStatus
    reason: Optional[str]
    loan_amount: Optional[Decimal]
    rehab_funding: Optional[Decimal]
    purchase_advance: Optional[Decimal]
    effective_ltc: Optional[Decimal]  # the LTC cap the loan was sized against (base cap less cuts)
    fee_surcharge_pct: Decimal
    borrower_score: int
    unconfirmed: bool
    cuts: tuple[Cut, ...] = field(default_factory=tuple)
    conditions: tuple[Condition, ...] = field(default_factory=tuple)
    rules_fired: tuple[str, ...] = field(default_factory=tuple)
    disclaimer: str = DISCLAIMER


def _quote_lender() -> LenderRules:
    return next(rules for rules in LENDER_MATRIX if rules.key == QUOTE_LENDER_KEY)


def _score(request: QuoteRequest, experience: int, loan: Optional[Decimal]) -> int:
    return score_borrower(BorrowerFacts(
        completed_projects_36m=experience, property_state=request.state, loan_amount=loan)).score


def _rejected(reason: str, borrower_score: int, unconfirmed: bool) -> QuoteResult:
    return QuoteResult(
        status="rejected", reason=reason, loan_amount=None, rehab_funding=None, purchase_advance=None,
        effective_ltc=None, fee_surcharge_pct=Decimal("0"), borrower_score=borrower_score,
        unconfirmed=unconfirmed, rules_fired=(reason,),
    )


def _ineligible_reason(request: QuoteRequest, credit_box: LenderRules, lender: LenderRules,
                      pricing_fico: int) -> Optional[str]:
    if request.borrower_type == "individual" and request.state in ENTITY_ONLY_STATES:
        return NEEDS_LLC_FORMATION
    if (NEW_CONSTRUCTION_EXIT_RULE_ENABLED and request.purpose == "new_construction"
            and max(g.exits for g in request.guarantors) < NEW_CONSTRUCTION_MIN_EXITS):
        return NEW_CONSTRUCTION_NEEDS_EXIT
    if credit_box.approved_states and request.state not in credit_box.approved_states:
        return STATE_NOT_ELIGIBLE
    # A placeholder floor never rejects: Central's placeholder 680 would block every quote the 660
    # interest-reserve rule exists for (client Q2). The quote is flagged unconfirmed instead.
    floor_confirmed = field_status_of(lender, "credit_floor") is FieldStatus.CONFIRMED
    if floor_confirmed and credit_box.credit_floor is not None and pricing_fico < credit_box.credit_floor:
        return FICO_BELOW_FLOOR
    return None


def _conditions(pricing_fico: int, loan: Decimal) -> tuple[Condition, ...]:
    if pricing_fico >= INTEREST_RESERVE_FICO_THRESHOLD:
        return ()
    amount = None
    if INTEREST_RESERVE_RATE is not None and INTEREST_RESERVE_TERM_MONTHS is not None:
        amount = (loan * INTEREST_RESERVE_RATE * INTEREST_RESERVE_TERM_MONTHS / 12).quantize(Decimal("1"))
    return (Condition(code=INTEREST_RESERVE_CONDITION, amount=amount),)


def _leverage(request: QuoteRequest, pricing_fico: int) -> tuple[tuple[Cut, ...], Decimal, tuple[str, ...]]:
    """The LTC cuts that apply (each with its reason), the fee surcharge, and notes for the audit row."""
    tier = next(t for t in PRICING_GRID if pricing_fico >= t.min_fico)
    cuts = [Cut(reason=GRID_CUT_REASON, pct=tier.ltc_cut)]
    if max(g.completed_deals for g in request.guarantors) in EXPERIENCE_HAIRCUT_DEALS:
        cuts.append(Cut(reason=EXPERIENCE_CUT_REASON, pct=EXPERIENCE_HAIRCUT))
    notes: list[str] = []
    rural = is_rural(request.zip_code)
    if rural is None:
        notes.append(RURAL_UNKNOWN_NOTE)
    elif rural:
        cuts.append(Cut(reason=RURAL_CUT_REASON, pct=RURAL_CUT))
    if request.purpose == "refinance":
        cuts.append(Cut(reason=REFINANCE_CUT_REASON, pct=REFINANCE_CUT))
    return tuple(cuts), tier.fee_surcharge, tuple(notes)


def quote(request: QuoteRequest) -> QuoteResult:
    lender = _quote_lender()
    credit_box = effective_rules(lender)
    shares, shares_confirmed = resolve_params(SOFT_APPROVAL_PARAMS.get(QUOTE_LENDER_KEY))
    unconfirmed = not (fully_confirmed(lender) and shares_confirmed
                       and PRICING_GRID_CONFIRMED and LEVERAGE_ADJUSTMENTS_CONFIRMED)
    experience = max(g.completed_deals for g in request.guarantors)
    pricing_fico = min(g.fico for g in request.guarantors)

    ineligible = _ineligible_reason(request, credit_box, lender, pricing_fico)
    if ineligible is not None:
        return _rejected(ineligible, _score(request, experience, None), unconfirmed)

    cuts, fee_surcharge, notes = _leverage(request, pricing_fico)
    ltc = max(credit_box.max_ltc + sum(c.pct for c in cuts), Decimal("0"))
    if ltc == 0:
        return _rejected(CALCULATION_REJECTION_CODES["loan_too_small"], _score(request, experience, None), unconfirmed)
    terms = LenderTerms(
        rehab_funding_pct=shares.rehab_funding_pct,
        purchase_advance_pct=shares.purchase_advance_pct,
        arv_cap=credit_box.max_ltv,  # the matrix holds the ARV cap as max_ltv (loan / ARV)
        ltc_cap=ltc,
        max_loan=credit_box.max_loan_amount,
        min_loan=credit_box.min_loan_amount,
    )
    try:
        figures = calculate_figures(request.purchase_price, request.rehab, request.arv, terms)
    except CalculationUnavailable as exc:
        reason = CALCULATION_REJECTION_CODES.get(exc.reason, QUOTE_UNAVAILABLE)
        return _rejected(reason, _score(request, experience, None), unconfirmed)

    conditions = _conditions(pricing_fico, figures.net_loan)
    return QuoteResult(
        status="indicative",
        reason=None,
        loan_amount=figures.net_loan,
        rehab_funding=figures.rehab_funding,
        purchase_advance=figures.purchase_advance,
        effective_ltc=ltc,
        fee_surcharge_pct=fee_surcharge,
        borrower_score=_score(request, experience, figures.net_loan),
        unconfirmed=unconfirmed,
        cuts=cuts,
        conditions=conditions,
        rules_fired=tuple(c.reason for c in cuts) + notes + tuple(c.code for c in conditions),
    )
