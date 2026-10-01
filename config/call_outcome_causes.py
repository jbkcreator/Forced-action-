"""Why an outcome did not fund, from the client's go-live brief.

Every unfunded outcome carries exactly one cause. A good borrower lost to a
slow response is an execution failure on our side, not a bad source, which is
why execution is split into lender execution and our execution.
"""
from __future__ import annotations

UNFUNDED_CAUSES: dict[str, str] = {
    "contactability": "Could not reach the borrower (wrong number, no answer, no callback)",
    "timing": "Borrower's deal or need was not ready yet",
    "fit": "Deal or borrower outside what the lenders will fund",
    "borrower_choice": "Borrower chose another lender or not to proceed",
    "lender_execution": "Lender declined, stalled or changed terms",
    "our_execution": "Lost on our side: slow response, missed follow-up, dropped file",
}
