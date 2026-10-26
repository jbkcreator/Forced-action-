"""Seam between the pre-qual letter and the Lender Box engine (T-05).

``get_fit_evaluator()`` maps a ``PrequalLead`` to the engine's ``BorrowerProfile`` / ``LoanRequest``,
calls ``evaluate_lender_fit`` and turns each fitting lender's loan bounds into ``FitLimits``. No
fitting lender means no letter (client: if no lender fits, send nothing). While every lender in the
matrix is unverified nothing could ever fit, so the seam stays closed and letters wait.

A lead the engine cannot judge (unknown loan type, unreadable credit band) is undecided, not a
decline: the evaluator returns ``None`` and its letter stays pending (client: an unknown eligibility
field is "possible, confirm on the call", never a decline). When T-05 adds that outcome, those
lenders count here like fits: the letter already says every figure is subject to lender confirmation.
"""
from __future__ import annotations

from decimal import Decimal
from functools import partial
from typing import Callable, Optional, Sequence

from config.lender_matrix import LenderRules
from src.lending.contracts import BorrowerProfile, LenderFitResult, LoanRequest, LoanType
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.lender_rules_store import current_matrix
from src.lending.lendingflow import credit_band_min_fico
from src.lending.prequal import FitLimits, PrequalLead

# None = undecided for this lead (letter waits); [] = no lender fits (letter skipped).
FitEvaluator = Callable[[PrequalLead], Optional[Sequence[FitLimits]]]


def _limits(rules: LenderRules) -> FitLimits:
    """Loan bounds of one lender; a side the lender does not state does not limit the range."""
    return FitLimits(
        min_amount=int(rules.min_loan_amount) if rules.min_loan_amount is not None else 0,
        max_amount=int(rules.max_loan_amount) if rules.max_loan_amount is not None else None,
    )


def fit_limits_from_result(result: LenderFitResult,
                           matrix: Optional[Sequence[LenderRules]] = None) -> list[FitLimits]:
    """Loan bounds of each lender in an already-scored result (partner files score themselves)."""
    matrix = current_matrix() if matrix is None else matrix
    rules_by_key = {rules.key: rules for rules in matrix}
    return [_limits(rules_by_key[fit.lender_key]) for fit in result.fitting if fit.lender_key in rules_by_key]


def evaluate_prequal_fit(lead: PrequalLead, matrix: Optional[tuple[LenderRules, ...]] = None) -> Optional[list[FitLimits]]:
    """Loan bounds of every lender that fits the lead, or None when the lead cannot be judged yet.

    The matrix is ``current_matrix()`` (the config, or ``lending.lender_rule_values`` when
    LENDING_RULES_FROM_DB is on) unless a caller injects one.
    """
    matrix = current_matrix() if matrix is None else matrix
    try:
        loan_type = LoanType(lead.loan_type.strip().upper())
    except (AttributeError, ValueError):
        return None
    min_fico = credit_band_min_fico(lead.credit_band)
    if min_fico is None:
        return None
    state = lead.property_state.strip().upper()
    result = evaluate_lender_fit(
        BorrowerProfile(credit_band_min_fico=min_fico, state=state),
        LoanRequest(loan_type=loan_type, loan_amount=Decimal(lead.loan_amount), state=state),
        matrix=matrix,
    )
    return fit_limits_from_result(result, matrix)


def get_fit_evaluator(matrix: Optional[tuple[LenderRules, ...]] = None) -> Optional[FitEvaluator]:
    """None while no lender in the matrix can fit (all unverified): queued letters then wait as
    pending instead of being skipped for good. Returns the engine as soon as one lender is usable.

    The matrix is read once per call, so a sweep scores every letter against the same rules."""
    matrix = current_matrix() if matrix is None else matrix
    if not any(rules.verified for rules in matrix):
        return None
    return partial(evaluate_prequal_fit, matrix=matrix)
