"""The soft approval figures, as pure Decimal arithmetic, to Josh's B1(c) formulas (Oct 8 answer).

Percentages are stored 0 to 1. The formulas are Josh's; the lender inputs (shares, caps, minimum) are
placeholders until he marks them confirmed (A1), and the service says so on the result.
Figures are internal estimates for a non-binding summary; they are never a rate, term or commitment.

    total cost        = purchase + rehab
    requested loan    = purchase x purchase share + rehab x rehab share
    loan              = smallest of requested, LTC cap, ARV cap and max loan, rounded down to $1,000
    rehab funding     = smaller of (rehab x rehab share) and the loan   (rehab stays whole, purchase is cut first)
    purchase advance  = loan - rehab funding, never below zero
    cash at closing   = purchase - purchase advance (before closing costs)
    max purchase price = the highest price, in $1,000 steps, at which no cap cuts the loan
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Optional

from src.lending.contracts import BindingCap, SizingFlag

_STEP = Decimal("1000")
_ZERO = Decimal("0")
_ARV_TYPO_MULTIPLE = Decimal("3")


class CalculationUnavailable(Exception):
    """The lender terms or inputs do not allow a figure to be computed. ``reason`` is a stable code."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class LenderTerms:
    """One lender's calculation inputs. A ``None`` limit means the lender has no such limit."""

    rehab_funding_pct: Decimal
    purchase_advance_pct: Decimal
    arv_cap: Optional[Decimal] = None
    ltc_cap: Optional[Decimal] = None
    max_loan: Optional[Decimal] = None
    min_loan: Optional[Decimal] = None


@dataclass(frozen=True)
class SoftApprovalFigures:
    net_loan: Decimal  # Josh's "loan": the total loan, rounded down to $1,000 (Q6 default for "net loan")
    rehab_funding: Decimal
    purchase_advance: Decimal
    cash_at_closing: Decimal  # internal only; before closing costs
    max_purchase_price: Optional[Decimal]  # None: no price limit, or no price gets the full advance
    full_advance_unavailable: bool  # True when no price at all avoids a cut
    binding_cap: BindingCap  # REQUESTED when no cap cut the loan
    flags: tuple[SizingFlag, ...]


def _round_down(value: Decimal) -> Decimal:
    return (value / _STEP).to_integral_value(rounding=ROUND_DOWN) * _STEP


def _validate(purchase: Decimal, rehab: Decimal, arv: Decimal, terms: LenderTerms) -> None:
    if purchase <= 0 or arv <= 0 or rehab < 0:
        raise CalculationUnavailable("invalid_inputs")
    if not (0 < terms.purchase_advance_pct <= 1) or not (0 <= terms.rehab_funding_pct <= 1):
        raise CalculationUnavailable("invalid_lender_terms")
    for limit in (terms.arv_cap, terms.ltc_cap, terms.max_loan):
        if limit is not None and limit <= 0:
            raise CalculationUnavailable("invalid_lender_terms")
    if terms.min_loan is not None and terms.min_loan < 0:
        raise CalculationUnavailable("invalid_lender_terms")


def _caps(purchase: Decimal, rehab: Decimal, arv: Decimal, terms: LenderTerms) -> list[tuple[BindingCap, Decimal]]:
    caps: list[tuple[BindingCap, Decimal]] = []
    if terms.ltc_cap is not None:
        caps.append((BindingCap.LTC, terms.ltc_cap * (purchase + rehab)))
    if terms.arv_cap is not None:
        caps.append((BindingCap.ARV, terms.arv_cap * arv))
    if terms.max_loan is not None:
        caps.append((BindingCap.MAX_LOAN, terms.max_loan))
    return caps


def _max_purchase_price(rehab: Decimal, arv: Decimal, terms: LenderTerms) -> tuple[Optional[Decimal], bool]:
    """Step the price down in $1,000 steps from the ARV until no cap cuts the loan (not a closed form, so it
    holds whichever cap binds). Returns (price, full_advance_unavailable)."""
    if terms.ltc_cap is None and terms.arv_cap is None and terms.max_loan is None:
        return None, False
    a, f = terms.purchase_advance_pct, terms.rehab_funding_pct
    rehab_need = rehab * f
    price_independent = [limit for limit in (
        terms.arv_cap * arv if terms.arv_cap is not None else None, terms.max_loan) if limit is not None]
    fixed_cap = min(price_independent) if price_independent else None
    price = _round_down(arv)
    while price >= _STEP:
        requested = price * a + rehab_need
        limit = fixed_cap
        if terms.ltc_cap is not None:
            ltc_limit = terms.ltc_cap * (price + rehab)
            limit = ltc_limit if limit is None else min(limit, ltc_limit)
        if requested <= limit:
            return price, False
        price -= _STEP
    return None, True


def calculate_figures(purchase: Decimal, rehab: Decimal, arv: Decimal, terms: LenderTerms) -> SoftApprovalFigures:
    _validate(purchase, rehab, arv, terms)
    a, f = terms.purchase_advance_pct, terms.rehab_funding_pct
    rehab_need = rehab * f
    requested = purchase * a + rehab_need

    binding, raw_loan = BindingCap.REQUESTED, requested
    for cap, value in _caps(purchase, rehab, arv, terms):
        if value < raw_loan:  # strict: a tie leaves the loan uncut, so REQUESTED wins it
            binding, raw_loan = cap, value

    loan = _round_down(raw_loan)
    if loan <= 0 or (terms.min_loan is not None and loan < terms.min_loan):
        raise CalculationUnavailable("loan_too_small")

    rehab_funding = min(rehab_need, loan)
    purchase_advance = max(loan - rehab_funding, _ZERO)
    max_price, full_advance_unavailable = _max_purchase_price(rehab, arv, terms)

    flags: list[SizingFlag] = []
    if arv < purchase + rehab:
        flags.append(SizingFlag.UPSIDE_DOWN)
    if arv > _ARV_TYPO_MULTIPLE * purchase:
        flags.append(SizingFlag.ARV_OVER_3X)

    return SoftApprovalFigures(
        net_loan=loan,
        rehab_funding=rehab_funding,
        purchase_advance=purchase_advance,
        cash_at_closing=max(purchase - purchase_advance, _ZERO),
        max_purchase_price=max_price,
        full_advance_unavailable=full_advance_unavailable,
        binding_cap=binding,
        flags=tuple(flags),
    )
