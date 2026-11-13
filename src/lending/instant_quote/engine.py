"""Instant quote engine (Cora P2.2): deal facts + guarantor FICOs in, an indicative quote out.

Pure: no database, no clock, no I/O. Base caps come from the lender matrix and the loan is sized by the
soft-approval calculator (unchanged). A quote is never more than "indicative, subject to underwriting".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Optional

from config.lender_matrix import LENDER_MATRIX, LenderRules, effective_rules, fully_confirmed
from config.lending_instant_quote import CALCULATION_REJECTION_CODES, DISCLAIMER, QUOTE_LENDER_KEY, QUOTE_UNAVAILABLE
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


def quote(request: QuoteRequest) -> QuoteResult:
    lender = _quote_lender()
    credit_box = effective_rules(lender)
    shares, shares_confirmed = resolve_params(SOFT_APPROVAL_PARAMS.get(QUOTE_LENDER_KEY))
    ltc = credit_box.max_ltc
    terms = LenderTerms(
        rehab_funding_pct=shares.rehab_funding_pct,
        purchase_advance_pct=shares.purchase_advance_pct,
        arv_cap=credit_box.max_ltv,  # the matrix holds the ARV cap as max_ltv (loan / ARV)
        ltc_cap=ltc,
        max_loan=credit_box.max_loan_amount,
        min_loan=credit_box.min_loan_amount,
    )
    unconfirmed = not (fully_confirmed(lender) and shares_confirmed)
    experience = max(g.completed_deals for g in request.guarantors)
    try:
        figures = calculate_figures(request.purchase_price, request.rehab, request.arv, terms)
    except CalculationUnavailable as exc:
        reason = CALCULATION_REJECTION_CODES.get(exc.reason, QUOTE_UNAVAILABLE)
        return _rejected(reason, _score(request, experience, None), unconfirmed)
    score = _score(request, experience, figures.net_loan)
    return QuoteResult(
        status="indicative",
        reason=None,
        loan_amount=figures.net_loan,
        rehab_funding=figures.rehab_funding,
        purchase_advance=figures.purchase_advance,
        effective_ltc=ltc,
        fee_surcharge_pct=Decimal("0"),
        borrower_score=score,
        unconfirmed=unconfirmed,
    )
