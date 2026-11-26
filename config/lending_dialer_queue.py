"""R1 1.6: David's 6 AM dialer queue check and 2:45 PM caller scorecard.

The queue check only counts and alerts; the loaders that push to BatchDialer are unchanged.
"""
from __future__ import annotations

from config.lending_compliance import ReasonCode
from config.lending_queues import DIAL_ORDER, LENDINGFLOW_POOL, WEBSITE_POOL

QUEUE_TARGET = 50
QUEUE_CHECK_HOUR_ET = 6
SCORECARD_HOUR_ET = 14
WORKDAYS = frozenset({0, 1, 2, 3, 4})  # Mon-Fri

CALLBACKS = "callbacks"
LF_LEADS = "lendingflow_leads"
WEB_LEADS = "web_leads"

# A phone is counted once, in the first source it appears in.
QUEUE_SOURCES: tuple[str, ...] = (CALLBACKS, LF_LEADS, WEB_LEADS, *DIAL_ORDER, LENDINGFLOW_POOL, WEBSITE_POOL)

SOURCE_LABELS: dict[str, str] = {
    CALLBACKS: "Callbacks",
    LF_LEADS: "New LendingFlow leads",
    WEB_LEADS: "New website leads",
    "partners": "Partners",
    "wholesalers": "Wholesalers",
    "transaction_ready": "Transaction ready",
    "builders": "Builders",
    "verified_maturity": "Verified maturity",
    LENDINGFLOW_POOL: "LendingFlow (dialer)",
    WEBSITE_POOL: "Website (dialer)",
}

# These clear up within the day, so they do not make a contact ineligible at 6 AM.
TRANSIENT_BLOCKS = frozenset({ReasonCode.OUTSIDE_CALL_WINDOW.value, ReasonCode.ATTEMPT_CAP_REACHED.value})

QUALITY_RUBRIC_VERSION = "provisional_v1"  # client has not confirmed the rubric yet
QUALITY_CHECKS: tuple[str, ...] = (
    "disposition logged", "2+ min talk", "next action", "decision maker", "objection or deal status",
)

TOP_CALLS = 10
NOTES_LIST_MAX = 20
IN_USE_DAYS = 14  # a pool with no load row in this many days is "not in use", never "dry"

NOTES_OPEN_ACTION_ID = "call_notes_open"
NOTES_CALLBACK_ID = "call_notes_submit"
