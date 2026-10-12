"""Seam between the pre-qual letter and the Lender Box engine (T-05).

``get_fit_evaluator()`` is the one place to wire Dev 2's ``evaluate_lender_fit`` once T-04/T-05
merge: map ``PrequalLead`` -> ``BorrowerProfile``/``LoanRequest``, call the engine, and map each
fitting lender's min/max loan amount to ``FitLimits``. Until then it returns None and queued
letters wait (pending) instead of failing.
"""
from __future__ import annotations

from typing import Callable, Optional, Sequence

from src.lending.prequal import FitLimits, PrequalLead

FitEvaluator = Callable[[PrequalLead], Sequence[FitLimits]]


def get_fit_evaluator() -> Optional[FitEvaluator]:
    return None
