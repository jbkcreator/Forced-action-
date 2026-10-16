"""R-1 Overflow Router: rule values and open-question defaults.

Every value that is still pending Josh's confirmation is labelled with its
Q-number from the task-analysis plan (Q1–Q8).  Defaults are conservative and
fail-closed.

PENDING ANSWERS (these defaults ship until Josh replies)
---------------------------------------------------------
Q1  Seat cap counting action.  Default: 'funded' (only funded loans count
    against the monthly cap).  Change SEAT_CAP_COUNTED_ACTION to 'routed'
    if Josh wants the cap on files accepted, not funded.
Q2  One-click decline records reason 'other' by default (Josh's enum has no
    'unspecified').  The reason must be a code in lending.decline_reason_codes,
    which holds Josh's enum.  Filling a reason in afterwards is not supported:
    the events table is append-only.
Q3  'held' is a 5th action beyond the spec's routed/declined/approved/funded
    (used for the hold-queue and exhausted terminal states).  Josh to confirm
    what "all four events" in acceptance test 3 means.
Q5  Rule gaps (ground-up >= $500k outside primary footprint, BRIDGE, "other")
    fall through to rule 6 (hold queue).  Matches the spec's "never silently
    dropped".
Q6  Loan amount for threshold checks uses loan_amount (= band minimum from
    LendingFlow).  Josh to confirm.
Q7  Admin API is built on lending-api with LENDING_ROUTING_ADMIN_SECRET.
    The admin screen / decline button (UI) is in forced-action-ui, not in
    this PR.
Q8  Files with suppressed=True are NOT routed: enrichment skips them before
    routing and route_file holds them with reason 'suppressed'.  Compliance
    to confirm that is the wanted behaviour.

Seed values stated by the spec (not open questions): the primary lender's
seat cap defaults to 15 and its ground-up minimum to $500,000.
"""
from __future__ import annotations

from zoneinfo import ZoneInfo

ROUTING_TZ = ZoneInfo("America/New_York")

# Seat cap: the action that increments the primary lender's monthly counter.
# Q1: Josh to confirm whether to count 'funded' or 'routed'.
SEAT_CAP_COUNTED_ACTION: str = "funded"

# Spec defaults for the primary lender's row: "seat cap is a config value, default 15 funded
# per month"; ground-up routes to the primary at >= $500,000.
PRIMARY_DEFAULT_SEAT_CAP: int = 15
PRIMARY_DEFAULT_CONSTRUCTION_MIN: int = 500_000

# Actions an operator may record against a file's current lender (spec: routed / declined /
# approved / funded; routed and declined are written by the router and the decline handler).
OUTCOME_ACTIONS: tuple[str, ...] = ("approved", "funded")

# The catch-all reason. It is never a default: a decline always needs a reason picked (Josh's answer to Q2),
# and choosing this one also needs a free-text explanation. It cannot be deactivated, so there is always an
# honest choice when none of Josh's specific reasons fits.
CATCH_ALL_DECLINE_REASON: str = "other"

# Slack action ids on the buttons of a routed message (R-1 decline / outcome buttons).
ACTION_DECLINE: str = "routing_decline"
ACTION_APPROVED: str = "routing_approved"
ACTION_FUNDED: str = "routing_funded"

# Josh's fixed decline-reason enum (his reply, "Inputs for the addendum"). Stored as data, so it stays editable.
SEED_DECLINE_REASONS: list[tuple[str, str]] = [
    ("credit_below_min", "Credit below the lender's minimum"),
    ("experience_below_min", "Experience below the lender's minimum"),
    ("ltv_over_max", "LTV over the lender's maximum"),
    ("ltc_over_max", "LTC over the lender's maximum"),
    ("loan_too_small", "Loan too small"),
    ("loan_too_large", "Loan too large"),
    ("property_type_excluded", "Property type excluded"),
    ("state_not_covered", "State not covered"),
    ("owner_occupied", "Owner occupied"),
    ("seat_cap_reached", "Lender's seat cap reached"),
    ("borrower_unresponsive", "Borrower unresponsive"),
    ("borrower_withdrew", "Borrower withdrew"),
    ("docs_incomplete", "Documents incomplete"),
    ("other", "Other (see free text)"),
]

# Placeholder codes seeded before Josh supplied his enum, with the exact label each was seeded with. The
# migration switches off a row only while it still carries that placeholder label (never deletes it, so any
# event that used one still reads correctly); a code someone has since re-labelled or added is left alone.
LEGACY_PLACEHOLDER_REASONS: tuple[tuple[str, str], ...] = (
    ("unspecified", "Not specified"),
    ("ltv", "LTV out of range"),
    ("credit", "Credit too low"),
    ("experience", "Insufficient borrower experience"),
    ("product", "Product mismatch"),
    ("geography", "Property outside footprint"),
    ("amount", "Loan amount out of range"),
    ("cap_exceeded", "Monthly cap reached"),
)

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
