"""The three soft approval figures, as pure Decimal arithmetic.

PROVISIONAL: the formulas are our proposal, not confirmed by Josh (see docs/lending/soft-approval.md).
Figures are internal estimates for a non-binding summary; they are never a rate, term or commitment.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Optional

_ONE_DOLLAR = Decimal("1")


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


@dataclass(frozen=True)
class SoftApprovalFigures:
    net_loan: Decimal
    rehab_funding: Decimal
    max_purchase_price: Optional[Decimal]  # None: no price limit, or no price gets the full advance
    full_advance_unavailable: bool  # True when the price limit works out at zero or below
    cash_needed: Decimal  # internal only; excludes fees, points and closing costs


def _validate(purchase: Decimal, rehab: Decimal, arv: Decimal, terms: LenderTerms) -> None:
    if purchase <= 0 or arv <= 0 or rehab < 0:
        raise CalculationUnavailable("invalid_inputs")
    if not (0 < terms.purchase_advance_pct <= 1) or not (0 <= terms.rehab_funding_pct <= 1):
        raise CalculationUnavailable("invalid_lender_terms")
    if terms.arv_cap is None and terms.ltc_cap is None and terms.max_loan is None:
        raise CalculationUnavailable("no_loan_limit")


def calculate_figures(purchase: Decimal, rehab: Decimal, arv: Decimal, terms: LenderTerms) -> SoftApprovalFigures:
    _validate(purchase, rehab, arv, terms)
    a, f = terms.purchase_advance_pct, terms.rehab_funding_pct
    rehab_need = rehab * f

    limits = []
    if terms.arv_cap is not None:
        limits.append(arv * terms.arv_cap)
    if terms.ltc_cap is not None:
        limits.append(terms.ltc_cap * (purchase + rehab))
    if terms.max_loan is not None:
        limits.append(terms.max_loan)
    cap = min(limits)

    rehab_funding = min(rehab_need, cap)
    net_loan = min(purchase * a + rehab_funding, cap)

    # Each limit is solved for price on its own; the LTC limit depends on price, so it cannot reuse ``cap``.
    price_limits = []
    if terms.arv_cap is not None:
        price_limits.append((arv * terms.arv_cap - rehab_need) / a)
    if terms.max_loan is not None:
        price_limits.append((terms.max_loan - rehab_need) / a)
    if terms.ltc_cap is not None and a > terms.ltc_cap:
        price_limits.append(rehab * (terms.ltc_cap - f) / (a - terms.ltc_cap))

    max_purchase: Optional[Decimal] = min(price_limits) if price_limits else None
    full_advance_unavailable = max_purchase is not None and max_purchase <= 0
    if full_advance_unavailable:
        max_purchase = None

    return SoftApprovalFigures(
        net_loan=net_loan.quantize(_ONE_DOLLAR, rounding=ROUND_DOWN),
        rehab_funding=rehab_funding.quantize(_ONE_DOLLAR, rounding=ROUND_DOWN),
        max_purchase_price=max_purchase.quantize(_ONE_DOLLAR, rounding=ROUND_DOWN) if max_purchase is not None else None,
        full_advance_unavailable=full_advance_unavailable,
        cash_needed=(purchase + rehab - net_loan).quantize(_ONE_DOLLAR, rounding=ROUND_UP),
    )
