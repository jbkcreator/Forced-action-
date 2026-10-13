"""Seam between the pre-qual letter and the Lender Box engine (T-05).

``get_fit_evaluator()`` maps a ``PrequalLead`` to the engine's ``BorrowerProfile`` / ``LoanRequest``,
calls ``evaluate_lender_fit`` and turns each fitting lender's loan bounds into ``FitLimits``. No
fitting lender means no letter (client: if no lender fits, send nothing). While every lender in the
matrix is unverified nothing could ever fit, so the seam stays closed and letters wait. A lead
the engine cannot read (unknown loan type) gets no fits, so its letter is skipped rather than guessed.

When T-05 adds the "possible, confirm on the call" outcome, those lenders count here too: the
letter already says every figure is subject to lender confirmation.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Callable, Optional, Sequence

from config.lender_matrix import LENDER_MATRIX, LenderRules
from src.lending.contracts import BorrowerProfile, LoanRequest, LoanType
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.lendingflow import _credit_band_min_fico
from src.lending.prequal import FitLimits, PrequalLead

FitEvaluator = Callable[[PrequalLead], Sequence[FitLimits]]

# A lender with no stated bound does not limit the range on that side.
_NO_MAX = 10**12


def _limits(rules: LenderRules) -> FitLimits:
    return FitLimits(
        min_amount=int(rules.min_loan_amount) if rules.min_loan_amount is not None else 0,
        max_amount=int(rules.max_loan_amount) if rules.max_loan_amount is not None else _NO_MAX,
    )


def evaluate_prequal_fit(lead: PrequalLead, matrix: tuple[LenderRules, ...] = LENDER_MATRIX) -> list[FitLimits]:
    try:
        loan_type = LoanType(lead.loan_type.strip().upper())
    except (AttributeError, ValueError):
        return []
    state = lead.property_state.strip().upper()
    result = evaluate_lender_fit(
        BorrowerProfile(credit_band_min_fico=_credit_band_min_fico(lead.credit_band), state=state),
        LoanRequest(loan_type=loan_type, loan_amount=Decimal(lead.loan_amount), state=state),
        matrix=matrix,
    )
    rules_by_key = {rules.key: rules for rules in matrix}
    return [_limits(rules_by_key[fit.lender_key]) for fit in result.fitting if fit.lender_key in rules_by_key]


def get_fit_evaluator(matrix: tuple[LenderRules, ...] = LENDER_MATRIX) -> Optional[FitEvaluator]:
    """None while no lender in the matrix can fit (all unverified): queued letters then wait as
    pending instead of being skipped for good. Returns the engine as soon as one lender is usable."""
    if not any(rules.verified for rules in matrix):
        return None
    return lambda lead: evaluate_prequal_fit(lead, matrix)
