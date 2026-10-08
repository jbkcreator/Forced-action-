"""T-04 — Fake implementations for use during development.

FakeLenderFitEvaluator satisfies the LenderFitEvaluator Protocol and is
injected into T-07 (Pre-Qual PDF) and T-08 (Soft Approval PDF) so both can
be built and tested independently while the real engine (T-05) is in progress.

Import path for downstream tasks:
    from src.lending.fakes import FakeLenderFitEvaluator

Replace at the call site once T-05 merges:
    from src.lending.lender_fit import evaluate_lender_fit
    evaluator = evaluate_lender_fit   # callable that returns LenderFitResult
"""
from __future__ import annotations

from decimal import Decimal

from src.lending.contracts import (
    BorrowerProfile,
    LenderFit,
    LenderFitEvaluator,  # noqa: F401 — re-exported so importers need only fakes
    LenderFitResult,
    LoanRequest,
)


class FakeLenderFitEvaluator:
    """Test-double evaluator that returns a fixed, caller-supplied result.

    Usage in tests:
        result = LenderFitResult(
            fitting=[LenderFit(lender_key="rcn_capital", lender_name="RCN Capital")],
        )
        evaluator = FakeLenderFitEvaluator(result)
        assert evaluator.evaluate(profile, request) is result

    When constructed with no arguments, returns a canned "one fitting lender"
    result so callers that don't care about the specific response don't need
    to build one themselves.

    Do NOT assert on the default result's lender_fit_score (or any other
    default value) in downstream tests.  The score is a placeholder: its scale
    is still open (Q2) and will change when T-05 ships the real formula.
    Tests that care about a specific value must pass their own LenderFitResult.
    """

    _DEFAULT_RESULT = LenderFitResult(
        fitting=[
            LenderFit(
                lender_key="generic_backflip",
                lender_name="Backflip (Generic)",
                total_borrower_cost=None,  # Q2: formula not yet confirmed
            )
        ],
        non_fitting=[],
        missing_fields=[],
        lender_fit_score=Decimal("75"),  # placeholder; Q2 pending
    )

    def __init__(self, fixed_result: LenderFitResult | None = None) -> None:
        self._result = fixed_result if fixed_result is not None else self._DEFAULT_RESULT

    def evaluate(
        self,
        borrower_profile: BorrowerProfile,
        loan_request: LoanRequest,
    ) -> LenderFitResult:
        """Return the fixed result regardless of inputs.

        Does not validate inputs beyond type — that is T-05's responsibility.
        The only guarantee here is that the return value satisfies the
        LenderFitEvaluator Protocol so PDFs compile without the real engine.
        """
        return self._result


# Structural check: confirm FakeLenderFitEvaluator satisfies the Protocol
# at import time rather than waiting for a runtime isinstance call.
assert isinstance(FakeLenderFitEvaluator(), LenderFitEvaluator), (
    "FakeLenderFitEvaluator must satisfy the LenderFitEvaluator Protocol"
)
