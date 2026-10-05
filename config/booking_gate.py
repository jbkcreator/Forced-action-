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

Per Josh's Oct 4 email §2, only List 4 (brokers and LOs) is meant to be
blocked from booking; List 2 (cash buyers) moved out of nurture into an
active dial rank and is no longer blocked.

The list-to-pool tagging mechanism this depends on exists (PR #318): every
staged lending record carries its list_key as `source_tag`
(src/services/lending/pool_extraction.py:source_tag_for(),
lending.calling_pool_staging.source_tag, documented in
lending.list_catalog). What still has to happen per booking is supplying
`list_key` on the gate submission itself (POST /api/fa-max/gates — see
src/api/fa_max_router.py) — `tracked_link_id`/`person_id` (the self-serve
pre-fill flow's own identity) have no FK or join path to
lending.calling_pool_staging today, so this can't be resolved by a
server-side lookup. It is caller-supplied, the same way every other gate
field already is: the dialer card shown to the caller already carries the
contact's campaign/list, so whatever UI or script submits a lending
booking's gate must include that list_key in the payload, same as
experience/credit/deal_status/etc.
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
# Real, not a no-op: PR #318 tags every staged lending record's source_tag
# (list_1..list_9); this only blocks a gate submission whose caller-supplied
# list_key (see module docstring) matches one of these keys.
# ---------------------------------------------------------------------------

BLOCKED_LIST_KEYS: FrozenSet[str] = frozenset({"list_4"})

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
