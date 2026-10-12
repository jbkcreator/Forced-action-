"""Seam between the pre-qual letter and the Lender Box engine (T-05).

``get_fit_evaluator()`` is the one place to wire Dev 2's ``evaluate_lender_fit`` once T-04/T-05
merge: map ``PrequalLead`` -> ``BorrowerProfile``/``LoanRequest``, call the engine, and map each
fitting lender's min/max loan amount to ``FitLimits``. Until then it returns None and queued
letters wait (pending) instead of failing.
"""
from __future__ import annotations

from typing import Callable, Optional, Sequence

from config.lender_matrix import LENDER_MATRIX, LenderRules
from src.lending.contracts import LenderFitResult
from src.lending.prequal import FitLimits, PrequalLead

FitEvaluator = Callable[[PrequalLead], Sequence[FitLimits]]


def get_fit_evaluator() -> Optional[FitEvaluator]:
    return None


def fit_limits_from_result(result: LenderFitResult,
                           matrix: Sequence[LenderRules] = LENDER_MATRIX) -> list[FitLimits]:
    """Loan-amount limits of each fitting lender, for the pre-qual range.

    A fitting lender without both a minimum and a maximum in the matrix is left out rather than
    guessed, so the range is never wider than the lenders actually allow.
    """
    rules_by_key = {rules.key: rules for rules in matrix}
    limits = []
    for fit in result.fitting:
        rules = rules_by_key.get(fit.lender_key)
        if rules is not None and rules.min_loan_amount is not None and rules.max_loan_amount is not None:
            limits.append(FitLimits(int(rules.min_loan_amount), int(rules.max_loan_amount)))
    return limits
