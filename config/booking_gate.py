"""
WP-GL-5 booking gate configuration.

Seven fields, per Josh's locked bar (Oct 1 email, D2; reconfirmed Oct 4 email
§2 "Booking bar stays all seven from D2"): experience, real deal / actively
looking, credit band, liquidity, occupancy, decision maker, property address
or target market. Exit strategy is captured but optional — it never gates.

Answers are stored as enum codes (e.g. "cash", "loc") — never free text —
to satisfy the _FINANCIAL_TERMS voice-intake regex and the relay payload
CHECK constraint. See src/services/fa_max_voice_intake.py:38.

Credit band is a caller-asked estimate only ("just ballpark, is your credit
generally above 640" — Caller Playbook), never a pulled score or bureau
check. It is stored as the enum below, not a number — this keeps it
structurally the same shape as every other gate field (a short code, never
a financial record) rather than a credit-score column.

BLOCKED_LIST_KEYS blocks List 4 (brokers/LOs) from booking, per Josh's
Oct 4 email §2 — List 2 (cash buyers) moved out of nurture into an active
dial rank and is no longer blocked. The key format ("list_4") matches
src/services/lending/pool_extraction.py's source_tag_for(), confirmed
against calling_pool_staging.source_tag on the real DB (list_1, list_3
through list_9 all present). Override via env var
GATE_BLOCKED_LIST_KEYS (comma-separated list keys).
"""
import os
from datetime import date
from typing import FrozenSet

# ---------------------------------------------------------------------------
# Answer vocabularies — codes stored in JSONB, never spoken aloud.
# ---------------------------------------------------------------------------

LIQUIDITY_SOURCES: FrozenSet[str] = frozenset({"cash", "loc", "partner", "none"})

# "none" means no liquidity — auto-kill.
LIQUIDITY_KILL_VALUES: FrozenSet[str] = frozenset({"none"})

# Experience: at least one completed fix-and-flip or ground-up build in the
# last 3 years counts. "0" does not auto-kill by itself — it fails the BOOK
# condition (see evaluate_gate), which routes to nurture rather than killing
# outright, since a loose launch-week bar still wants these captured.
COMPLETED_PROJECTS: FrozenSet[str] = frozenset({"0", "1_to_2", "3_plus"})

# Exit strategy is captured but optional per D2 — never validated as
# required and never gates the booking.
EXIT_STRATEGIES: FrozenSet[str] = frozenset({"sale", "refinance", "other"})

OCCUPANCY_TYPES: FrozenSet[str] = frozenset({"investment", "homestead"})

# "homestead" means owner-occupied primary residence — auto-kill.
OCCUPANCY_KILL_VALUES: FrozenSet[str] = frozenset({"homestead"})

DECISION_MAKER_VALUES: FrozenSet[str] = frozenset({"yes", "no"})

# "no" means the decision-maker is not on the call — kill.
DECISION_MAKER_KILL_VALUES: FrozenSet[str] = frozenset({"no"})

# Real deal / actively looking — at least one of the two must be true to
# book; neither is itself a kill, the combination with experience and
# credit decides book vs nurture.
DEAL_STATUS_VALUES: FrozenSet[str] = frozenset({"real_deal", "actively_looking", "neither"})
DEAL_STATUS_QUALIFYING_VALUES: FrozenSet[str] = frozenset({"real_deal", "actively_looking"})

# Credit band — caller-asked estimate only, never a pulled score. "below_640"
# does not auto-kill by itself (same treatment as 0 completed projects): it
# fails the BOOK condition and routes to nurture, not a hard kill, per the
# loose launch-week bar.
CREDIT_BANDS: FrozenSet[str] = frozenset({"at_or_above_640", "below_640", "unsure"})
CREDIT_BAND_QUALIFYING_VALUES: FrozenSet[str] = frozenset({"at_or_above_640"})

# ---------------------------------------------------------------------------
# Pool / list blocking.
# Per Josh's Oct 4 email §2: only List 4 (brokers/LOs) blocks from booking.
# List 2 (cash buyers) moved to an active dial rank and is not blocked.
# Default matches source_tag_for()'s "list_4" for pool_name="mortgage_broker".
# ---------------------------------------------------------------------------

_raw_blocked_list_keys = os.environ.get("GATE_BLOCKED_LIST_KEYS", "list_4")
BLOCKED_LIST_KEYS: FrozenSet[str] = frozenset(
    key.strip().lower() for key in _raw_blocked_list_keys.split(",") if key.strip()
)

# ISO date string — gate blocks bookings from BLOCKED_LIST_KEYS until this date.
# Value chosen as December 1 2026; update via env var GATE_LIST_UNBLOCK_DATE
# (format YYYY-MM-DD) once the exact date is confirmed.
_raw_date = os.environ.get("GATE_LIST_UNBLOCK_DATE", "2026-12-01")
try:
    GATE_LIST_UNBLOCK_DATE = date.fromisoformat(_raw_date)
except ValueError:
    GATE_LIST_UNBLOCK_DATE = date(2026, 12, 1)

# ---------------------------------------------------------------------------
# Daily cap — configurable via FA_MAX_CALENDAR_DAILY_CAP.
# Josh's Oct 1 email (E1) + Oct 4 "held call cap" item: 8 held calls/day.
# ---------------------------------------------------------------------------

try:
    CALENDAR_DAILY_CAP: int = int(os.environ.get("FA_MAX_CALENDAR_DAILY_CAP", "8"))
except ValueError:
    CALENDAR_DAILY_CAP = 8

# Postgres advisory lock key for the daily-cap counter. Must be a stable int.
# sha256("fa_max_daily_cap") & 0x7FFFFFFF gives a safe positive int32.
DAILY_CAP_ADVISORY_KEY: int = 1_823_047_291

GATE_RULES_VERSION = "2.0"
