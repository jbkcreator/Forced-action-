"""R-1 Overflow Router: rule values and open-question defaults.

Every value that is still pending Josh's confirmation is labelled with its
Q-number from the task-analysis plan (Q1–Q8).  Defaults are conservative and
fail-closed.

PENDING ANSWERS (these defaults ship until Josh replies)
---------------------------------------------------------
Q1  Seat cap counting action.  Default: 'funded' (only funded loans count
    against the monthly cap).  Change SEAT_CAP_COUNTED_ACTION to 'routed'
    if Josh wants the cap on files accepted, not funded.
Q2  One-click decline records reason 'unspecified' by default.
Q3  'held' is the 5th action added to the events table (test 3 uses it for
    the exhausted terminal state).
Q5  Rule gaps (ground-up >= $500k outside primary footprint, etc.) fall
    through to rule 6 (hold queue, reason 'no_match').
Q6  Loan amount for threshold checks uses loan_amount (= band minimum from
    LendingFlow).  Josh to confirm.
Q7  Admin API is built on lending-api with LENDING_ROUTING_ADMIN_SECRET.
    UI is in forced-action-ui (out of scope for this PR).
Q8  Files with suppressed=True are routed (routing is not borrower contact).
    Josh or compliance to confirm.
"""
from __future__ import annotations

from zoneinfo import ZoneInfo

ROUTING_TZ = ZoneInfo("America/New_York")

# Seat cap: the action that increments the primary lender's monthly counter.
# Q1: Josh to confirm whether to count 'funded' or 'routed'.
SEAT_CAP_COUNTED_ACTION: str = "funded"

# Default reason code recorded on a one-click Backflip decline (Q2).
DEFAULT_DECLINE_REASON: str = "unspecified"

# Seed decline reason codes. Josh to supply the real enum.
SEED_DECLINE_REASONS: list[tuple[str, str]] = [
    ("unspecified", "Not specified"),
    ("ltv", "LTV out of range"),
    ("credit", "Credit too low"),
    ("experience", "Insufficient borrower experience"),
    ("product", "Product mismatch"),
    ("geography", "Property outside footprint"),
    ("amount", "Loan amount out of range"),
    ("cap_exceeded", "Monthly cap reached"),
    ("other", "Other (see free text)"),
]

# Routing engine: maximum qualifying lenders evaluated per file per run.
MAX_WATERFALL_DEPTH = 50

# Sweep: unrouted files older than this many minutes are picked up by the sweep.
SWEEP_UNROUTED_AFTER_MINUTES = 5
SWEEP_BATCH_SIZE = 50

# Slack hold-queue summary: posted at 8:00 AM ET.  Two cron lines (DST).
HOLD_SUMMARY_HOUR_ET = 8

# Agreement statuses a lender must have to be considered by the router.
ROUTABLE_AGREEMENT_STATUSES: frozenset[str] = frozenset({"signed"})

# All allowed agreement status values.
AGREEMENT_STATUS_CHOICES: tuple[str, ...] = ("signed", "pending", "none", "terminated")
