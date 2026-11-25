"""R1 1.3 morning list ("Today's list" in Cora's morning digest): reason codes, owners, tiers and limits.

Decisions: release1-task/1.3-GRILL-DECISIONS.md. Ranking is the fixed tier order below, not expected fee
(config/lending_money_model.py stays unused until the client gives the numbers).
"""
from __future__ import annotations

from datetime import timedelta

CLOSING_BLOCKER = "CLOSING_BLOCKER"
ACTIVE_DEAL = "ACTIVE_DEAL"
BOOKED_TODAY = "BOOKED_TODAY"
CALLBACK = "CALLBACK"
NEW_WARM_LEAD = "NEW_WARM_LEAD"
COLD_TOP = "COLD_TOP"

# Tier per reason: lower ranks first. Booked calls and callbacks share tier 3.
TIER: dict[str, int] = {
    CLOSING_BLOCKER: 1, ACTIVE_DEAL: 2, BOOKED_TODAY: 3, CALLBACK: 3, NEW_WARM_LEAD: 4, COLD_TOP: 5,
}
REASON_LABELS: dict[str, str] = {
    CLOSING_BLOCKER: "Stuck deal", ACTIVE_DEAL: "Active deal", BOOKED_TODAY: "Booked today", CALLBACK: "Callback",
    NEW_WARM_LEAD: "New lead", COLD_TOP: "Top of queue",
}

JOSH = "josh"
DAVID = "david"
# Owner per reason (D6). A callback's owner is the caller who logged it (callback_owner below).
OWNER: dict[str, str] = {
    CLOSING_BLOCKER: JOSH, ACTIVE_DEAL: JOSH, BOOKED_TODAY: JOSH, CALLBACK: DAVID, NEW_WARM_LEAD: DAVID,
    COLD_TOP: DAVID,
}
# Caller names (lower-cased prefix) whose callbacks belong to Josh; everyone else's go to David.
JOSH_CALLER_PREFIXES: tuple[str, ...] = ("josh",)

# Reasons that must carry a Mobile line type to be listed at all (D7, D5). Others are listed and flagged.
MOBILE_REQUIRED = frozenset({NEW_WARM_LEAD, COLD_TOP})
MOBILE_LINE_TYPE = "mobile"  # lending.contacts.line_type, compared lower-cased (Tracerfy writes "Mobile")

# Reasons that can get a morning draft (D8): consented warm leads and callbacks only.
DRAFTABLE = frozenset({NEW_WARM_LEAD, CALLBACK})

CALLBACK_WINDOW = timedelta(days=7)        # D17
DRAFT_MIN_LEAD_AGE = timedelta(hours=24)   # D16: speed-to-lead owns the first 24 hours
NO_TOUCH_WINDOW = timedelta(days=3)        # D16: no outbound text/email from us in this window
DRAFT_TTL = timedelta(hours=12)            # approval card expiry
DRAFT_REQUESTED_BY = "morning_list"

# Active loan stages, latest first (D2 tier 2). FUNDED / PAID are done; a closed file is never active.
ACTIVE_STAGES_LATEST_FIRST: tuple[str, ...] = (
    "CLOSING", "TERM_SHEET_ISSUED", "ACCEPTED", "SUBMITTED", "FIT", "INTAKE",
)

BRIEF_MAX_CHARS = 160

# Template fallback when the LLM draft fails or quotes a number (D18). No rates, points, fees or amounts.
FALLBACK_SMS = ("Hi {first_name}, this is Josh with Next Deal Lending. Following up on your loan request: "
                "do you have a few minutes today to talk through your deal? Reply STOP to opt out.")
FALLBACK_EMAIL_SUBJECT = "Your loan request with Next Deal Lending"
FALLBACK_EMAIL_BODY = ("Hi {first_name},\n\nThis is Josh with Next Deal Lending. I wanted to follow up on your loan "
                       "request. Do you have a few minutes today to talk through your deal?\n\nJosh")
TEMPLATE_VERSION = "morning_list_v1"
