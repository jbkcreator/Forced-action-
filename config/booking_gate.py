"""
WP-GL-5 booking gate configuration.

Six fields are required before a caller may book Josh's calendar.
Answers are stored as enum codes (e.g. "cash", "loc") — never free text —
to satisfy the _FINANCIAL_TERMS voice-intake regex and the relay payload
CHECK constraint. See src/services/fa_max_voice_intake.py:38.

Open: BLOCKED_LIST_KEYS must be populated once the brief's List 2 / List 4
numbering is mapped to the actual pool names in src/services/lending/.
Until that mapping arrives, the set is empty (no pool is blocked).
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

COMPLETED_PROJECTS: FrozenSet[str] = frozenset({"0", "1_to_2", "3_plus"})

EXIT_STRATEGIES: FrozenSet[str] = frozenset({"sale", "refinance", "other"})

OCCUPANCY_TYPES: FrozenSet[str] = frozenset({"investment", "homestead"})

# "homestead" means owner-occupied primary residence — auto-kill.
OCCUPANCY_KILL_VALUES: FrozenSet[str] = frozenset({"homestead"})

DECISION_MAKER_VALUES: FrozenSet[str] = frozenset({"yes", "no"})

# "no" means the decision-maker is not on the call — kill.
DECISION_MAKER_KILL_VALUES: FrozenSet[str] = frozenset({"no"})

# ---------------------------------------------------------------------------
# Pool / list blocking — OPEN QUESTION (Q2 from task-analysis).
# Populate once brief List 2 / List 4 → pool name mapping is confirmed.
# Until then, no pool is blocked and gate evaluations are unaffected.
# ---------------------------------------------------------------------------

BLOCKED_LIST_KEYS: FrozenSet[str] = frozenset()

# ISO date string — gate blocks bookings from BLOCKED_LIST_KEYS until this date.
# Value chosen as December 1 2026; update via env var GATE_LIST_UNBLOCK_DATE
# (format YYYY-MM-DD) once the exact date is confirmed.
_raw_date = os.environ.get("GATE_LIST_UNBLOCK_DATE", "2026-12-01")
try:
    GATE_LIST_UNBLOCK_DATE = date.fromisoformat(_raw_date)
except ValueError:
    GATE_LIST_UNBLOCK_DATE = date(2026, 12, 1)

# ---------------------------------------------------------------------------
# Daily cap — configurable via FA_MAX_CALENDAR_DAILY_CAP (default 6).
# Brief says "6 to 8 held calls/day"; 6 is the conservative default.
# ---------------------------------------------------------------------------

try:
    CALENDAR_DAILY_CAP: int = int(os.environ.get("FA_MAX_CALENDAR_DAILY_CAP", "6"))
except ValueError:
    CALENDAR_DAILY_CAP = 6

# Postgres advisory lock key for the daily-cap counter. Must be a stable int.
# sha256("fa_max_daily_cap") & 0x7FFFFFFF gives a safe positive int32.
DAILY_CAP_ADVISORY_KEY: int = 1_823_047_291

GATE_RULES_VERSION = "1.0"
