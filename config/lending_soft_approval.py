"""Call-One soft approval PDF (T-08): constants and the per-lender calculation inputs.

``SOFT_APPROVAL_PARAMS`` holds the two inputs the lender matrix does not carry:
the share of the purchase price a lender advances and the share of the rehab it funds.
A value of ``None`` means Josh has not confirmed it, and a lender without both values is
never used for a PDF. Nothing here is a confirmed lender rule.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Optional

from src.lending.contracts import LoanType

NON_BINDING_SOFT_APPROVAL_WATERMARK = "NON-BINDING SOFT APPROVAL ESTIMATE — FOR INFORMATIONAL PURPOSES ONLY"
SOFT_APPROVAL_TEMPLATE = "soft_approval.html"

# Flip only until Josh confirms bridge sizing; DSCR has no rehab or ARV and construction has no stated ARV cap.
SOFT_APPROVAL_LOAN_TYPES: frozenset[LoanType] = frozenset({LoanType.FIX_AND_FLIP})

OPEN_FORM_ACTION_ID = "soft_approval_open"
FORM_CALLBACK_ID = "soft_approval_form"

# Card retry (src/tasks/lending_soft_approval_card_retry.py): a finished call with no card row is retried every
# cron cycle. The age floor keeps the sweep off a call whose first attempt is still running; the window bounds
# the backfill (it is also what enabling the feature can post for calls that ended just before it was switched on).
CARD_RETRY_MIN_AGE_SECONDS = 120
CARD_RETRY_WINDOW_HOURS = 6
CARD_RETRY_BATCH_LIMIT = 50


@dataclass(frozen=True)
class SoftApprovalParams:
    """Calculation inputs for one lender key (same keys as ``config.lender_matrix``)."""

    rehab_funding_pct: Optional[Decimal] = None
    purchase_advance_pct: Optional[Decimal] = None

    def confirmed(self) -> bool:
        return self.rehab_funding_pct is not None and self.purchase_advance_pct is not None


SOFT_APPROVAL_PARAMS: Mapping[str, SoftApprovalParams] = {
    # "Funds up to 100% of rehab" is Josh's Oct 1 wording (client-stated, not confirmed as a lender rule).
    # The purchase advance is not stated anywhere, so this lender stays unusable until Josh gives it.
    "generic_backflip_flip": SoftApprovalParams(rehab_funding_pct=Decimal("1"), purchase_advance_pct=None),
}
