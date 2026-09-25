"""config/fa_max_campaigns.py

WP-T3-4 (Campaign Selection Agent) thresholds and switches -- config-as-data
per CLAUDE.md, mirroring config/fa_max_stage_monitoring.py's
validate_*_config() convention.

Client decision (23/9): v1 runs three campaigns only -- capital_desk_loop,
exit_desk, rescue_circuit. Everything else (campaigns 4-10) stays unbuilt
per the client's own instruction ("do not build anything specific to the
others yet"). See tasks/FA_Max_build/WP-T3-4_Campaign_Selection_Agent_
Implementation_Plan.md for the full reasoning behind every value below.
"""
from __future__ import annotations

# ── Campaigns ────────────────────────────────────────────────────────────────

CAMPAIGN_CAPITAL_DESK_LOOP = "capital_desk_loop"
CAMPAIGN_EXIT_DESK = "exit_desk"
CAMPAIGN_RESCUE_CIRCUIT = "rescue_circuit"

CAMPAIGNS: frozenset[str] = frozenset({
    CAMPAIGN_CAPITAL_DESK_LOOP, CAMPAIGN_EXIT_DESK, CAMPAIGN_RESCUE_CIRCUIT,
})

# Highest priority first. Exit Desk is the client's "hardest and most
# important" -- see plan Section 2, decision 2.
CAMPAIGN_PRIORITY: tuple[str, ...] = (
    CAMPAIGN_EXIT_DESK, CAMPAIGN_CAPITAL_DESK_LOOP, CAMPAIGN_RESCUE_CIRCUIT,
)

AUDIENCE_INVESTOR = "investor"
AUDIENCE_PARTNER = "partner"
AUDIENCES: frozenset[str] = frozenset({AUDIENCE_INVESTOR, AUDIENCE_PARTNER})

# Which drafting agent's work queue a campaign's touches hand off to
# (Tier 3 split: Campaign Selection feeds both WP-T3-5 Outreach and
# WP-T3-6 Partner Nurture -- plan R5).
CAMPAIGN_HANDOFF_QUEUE: dict[str, str] = {
    CAMPAIGN_EXIT_DESK: "fa_max_outreach",
    CAMPAIGN_CAPITAL_DESK_LOOP: "fa_max_outreach",  # wholesaler rows are audience=partner and route below
    CAMPAIGN_RESCUE_CIRCUIT: "fa_max_partner_nurture",
}

AUDIENCE_HANDOFF_QUEUE: dict[str, str] = {
    AUDIENCE_INVESTOR: "fa_max_outreach",
    AUDIENCE_PARTNER: "fa_max_partner_nurture",
}

# Exit Desk is switched off until the bought lending-data feed exists
# (plan Gap C / D-1). Capital Desk Loop and Rescue Circuit are buildable
# today against data already in FA.
CAMPAIGN_ENABLED: dict[str, bool] = {
    CAMPAIGN_CAPITAL_DESK_LOOP: True,
    CAMPAIGN_EXIT_DESK: False,
    CAMPAIGN_RESCUE_CIRCUIT: True,
}

# Exit Desk's flip-to-rent add-on needs listing status from the bought
# source -- switched on separately once that field is confirmed present.
EXIT_DESK_FLIP_TO_RENT_ENABLED: bool = False

# ── Eligibility thresholds ───────────────────────────────────────────────────

# Q-C4 (open): how recent a purchase must be to count as a "recent cash
# buyer". The client gave 72 hours for the no-mortgage check, not this
# window. Default proposed pending confirmation.
CASH_BUYER_RECENT_DAYS: int = 90

# The client's own wording: "no mortgage recorded within 72 hours" of the
# cash-buyer deed.
CASH_BUYER_NO_MORTGAGE_WINDOW_HOURS: int = 72

# "Active investor": 2+ purchases in 24 months (client, 23/9, Campaign 1).
ACTIVE_INVESTOR_MIN_PURCHASES: int = 2
ACTIVE_INVESTOR_WINDOW_MONTHS: int = 24

# Exit Desk mortgage age window (client, 23/9, Campaign 2).
EXIT_DESK_MIN_LOAN_AGE_MONTHS: int = 8
EXIT_DESK_MAX_LOAN_AGE_MONTHS: int = 15

# Flip-to-rent: 45+ days on market, or expired/withdrawn (client, 23/9).
EXIT_DESK_STALE_LISTING_DAYS: int = 45

# "Top 25 in each class gets the week-one asset" (spec, item 14). Partner
# mining never sets status='active' in the current codebase (plan R3/D-4),
# so the wholesaler rule ranks on `rank` directly rather than `status`.
WHOLESALER_TOP_N: int = 25

# ── Priority / switching / cooling ───────────────────────────────────────────

# Minimum gap since the last sent touch before a switched-up campaign's
# first step goes out -- ported from Banks' governance 14-day spacing rule.
MIN_GAP_DAYS: int = 14

# No re-enrollment in the SAME campaign for this long after it completes.
REENROLL_COOLDOWN_DAYS: int = 60

# ── Volume control (plan Section 6.9 / D-5) ──────────────────────────────────

# Conservative starting cap while the new sending domain warms up. The
# client: "Domains burned in week one cannot be unburned." Raise as warm-up
# progresses -- team lead sets the real value (D-5).
MAX_NEW_ENROLLMENTS_PER_DAY: dict[str, int] = {
    CAMPAIGN_CAPITAL_DESK_LOOP: 25,
    CAMPAIGN_EXIT_DESK: 25,
    CAMPAIGN_RESCUE_CIRCUIT: 25,
}

# ── Sweep cadence ────────────────────────────────────────────────────────────

ENROLLMENT_SWEEP_CRON: str = "30 6 * * *"   # after partner mining (06:00 UTC)
DUE_STEP_SWEEP_MINUTES: int = 15

# ── Partner classes per campaign (plan Section 6.3) ─────────────────────────

WHOLESALER_PARTNER_CLASS: str = "wholesaler"

RESCUE_CIRCUIT_PARTNER_CLASSES: frozenset[str] = frozenset({
    "title_rep", "closing_attorney", "broker", "loan_officer",
})

# Only rows created by the manual import (Section 6.12) qualify for Rescue
# Circuit -- partner mining's automatic classifier never writes these
# classes (plan Gap D), but the source check is the actual enforcement.
RESCUE_CIRCUIT_PARTNER_SOURCE: str = "manual_import"

# ── Bought-data contract (plan D-1, Gap C) ──────────────────────────────────

# Working name for the table/view Exit Desk reads once the bought lending
# data lands. Confirm the real name/shape with the bought-data loader
# owner before switching CAMPAIGN_ENABLED[exit_desk] on (Build Step 1).
LENDING_MORTGAGE_RECORDS_TABLE: str = "lending_mortgage_records"

LENDING_MORTGAGE_LENDER_TYPES: frozenset[str] = frozenset({
    "private", "bridge", "hard_money",
})


def validate_campaign_config() -> None:
    """Fail fast at import/startup time on an inconsistent config."""
    if set(CAMPAIGN_PRIORITY) != CAMPAIGNS:
        raise ValueError("CAMPAIGN_PRIORITY must list exactly the campaigns in CAMPAIGNS")
    if len(CAMPAIGN_PRIORITY) != len(set(CAMPAIGN_PRIORITY)):
        raise ValueError("CAMPAIGN_PRIORITY must not repeat a campaign")
    if set(CAMPAIGN_ENABLED) != CAMPAIGNS:
        raise ValueError("CAMPAIGN_ENABLED must have exactly one entry per campaign")
    if set(CAMPAIGN_HANDOFF_QUEUE) != CAMPAIGNS:
        raise ValueError("CAMPAIGN_HANDOFF_QUEUE must have exactly one entry per campaign")
    if set(MAX_NEW_ENROLLMENTS_PER_DAY) != CAMPAIGNS:
        raise ValueError("MAX_NEW_ENROLLMENTS_PER_DAY must have exactly one entry per campaign")
    if any(v <= 0 for v in MAX_NEW_ENROLLMENTS_PER_DAY.values()):
        raise ValueError("MAX_NEW_ENROLLMENTS_PER_DAY values must be positive")
    if CASH_BUYER_RECENT_DAYS <= 0 or CASH_BUYER_NO_MORTGAGE_WINDOW_HOURS <= 0:
        raise ValueError("cash-buyer thresholds must be positive")
    if EXIT_DESK_MIN_LOAN_AGE_MONTHS <= 0 or EXIT_DESK_MAX_LOAN_AGE_MONTHS <= EXIT_DESK_MIN_LOAN_AGE_MONTHS:
        raise ValueError("Exit Desk loan-age window must be a positive, ordered range")
    if MIN_GAP_DAYS < 0 or REENROLL_COOLDOWN_DAYS < 0:
        raise ValueError("gap/cooldown thresholds must not be negative")
    if WHOLESALER_TOP_N <= 0:
        raise ValueError("WHOLESALER_TOP_N must be positive")
    if not RESCUE_CIRCUIT_PARTNER_CLASSES:
        raise ValueError("RESCUE_CIRCUIT_PARTNER_CLASSES must not be empty")
