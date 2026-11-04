"""Lending Engine Wave 0 compliance floor — rule values (spec §3.1, §3.2).

Lending-only: FA SMS/voice keeps its own 8 AM–9 PM window and
``settings.dnc_recheck_days`` (30). See
lender-engine/dev2-compliance-floor/GRILL-DECISIONS.md.
"""
from __future__ import annotations

from datetime import time
from enum import Enum

CALL_WINDOW_START = time(8, 0)   # inclusive, recipient local time (FTSA floor)
CALL_WINDOW_END = time(20, 0)    # exclusive
# Client hard stop (Go Live Brief): every call also sits inside 09:00-19:15 Eastern.
ET_WINDOW_START = time(9, 0)     # inclusive
ET_WINDOW_END = time(19, 15)     # exclusive
# Seat shift groups, Eastern. No group -> the widest ET window applies.
SHIFT_GROUPS: dict[str, tuple[time, time]] = {
    "A": (time(9, 0), time(15, 0)),
    "B": (time(13, 0), time(19, 15)),
}
# BatchDialer agent id -> shift group. Filled once callers are named. An unmapped agent's
# call is allowed (the 09:00-19:15 ET and recipient-local rails still apply) and warned.
AGENT_SHIFT_GROUPS: dict[str, str] = {}
MAX_ATTEMPTS_PER_PERIOD = 3
ATTEMPT_PERIOD_HOURS = 24        # rolling
# Josh (Oct 4 answers, §6 "Attempts"): 3 per 24h, 6 total over 10 business days, then nurture.
MAX_ATTEMPTS_TOTAL = 6
ATTEMPT_HISTORY_BUSINESS_DAYS = 10
DNC_SCRUB_MAX_AGE_DAYS = 7
# The weekly job (cron: Mondays) must rescrub every phone whose scrub would turn stale for the
# sweep before the next run: age > DNC_SCRUB_MAX_AGE_DAYS - period. With period == max age that
# is every loaded phone. A threshold equal to the max age (the old behaviour) skips a phone that
# is 6d23h old, which then sits stale for the sweep until the following Monday (~half the pool).
WEEKLY_SCRUB_PERIOD_DAYS = 7
WEEKLY_RESCRUB_AFTER_DAYS = max(0, DNC_SCRUB_MAX_AGE_DAYS - WEEKLY_SCRUB_PERIOD_DAYS)
# If more than this share of the loaded pool looks stale the weekly job (or Tracerfy) is down,
# not 20% of the numbers: the sweep alerts instead of mass-pulling the pool from the dialer.
SCRUB_STALE_BREAKER_PCT = 25
SCRUB_STALE_BREAKER_MIN_POOL = 20
STOP_PROPAGATION_SLA_SECONDS = 60
OPT_OUT_POLL_SECONDS = 15        # FA opt-out poller interval; worst case well inside the SLA
# GoHighLevel opt-out sync: every lending opt-out becomes DND on the GHL contact.
GHL_DND_CHANNELS = ("SMS", "Email", "GMB", "FB", "WhatsApp", "Call")
GHL_OPT_OUT_TAG = "lending-opt-out"
# Cold leads in GHL (client rule: no texts or emails unless they agree to texts on a call): every
# messaging channel is do-not-disturb, calls stay open. The SMS sender lifts SMS once consent is recorded.
COLD_LEAD_DND_CHANNELS = tuple(channel for channel in GHL_DND_CHANNELS if channel != "Call")
GHL_TAG_COLD_LEAD = "ndl-cold"
# The do-not-disturb message set on a cold push. Warming up lifts only do-not-disturb carrying this message, so a
# genuine opt-out (set with another message) is never lifted.
COLD_LEAD_DND_MESSAGE = "Cold lead: calls only"
# One calling rule for every lead (TL decision, Nov 4): both platforms dial until someone gets through, then the other
# stops. "Got through" inside this window = a live-conversation result (connected, callback, DNC request, nurture,
# booked) or, for a GHL call with no result posted, GHL's answered status with REACHED_MIN_TALK_SECONDS of talk.
# Reached in GHL -> the number leaves BatchDialer; reached in BatchDialer -> held off Josh's GHL call lists.
GHL_CALLING_LOOKBACK_DAYS = 14
REACHED_CODES: tuple[str, ...] = ("CONNECTED_NOT_INTERESTED", "CALLBACK_REQUESTED", "DNC_REQUEST", "NURTURE", "BOOKED",
                                  "DATA_NURTURE_ONLY", "GATE_FAILED_NURTURE")
REACHED_GHL_STATUSES: tuple[str, ...] = ("ghl:completed", "ghl:answered")
REACHED_MIN_TALK_SECONDS = 60
GHL_DND_BATCH = 50               # GHL updates per poll cycle (GHL client throttles ~2 req/s)
GHL_BACKSTOP_PAGE_SIZE = 100     # GHL contact search page size (DND backstop)
GHL_BACKSTOP_MAX_PAGES = 50      # bound per backstop run (~5,000 DND contacts)
DIALER_SWEEP_SECONDS = 60        # window/cap pull-and-restore sweep interval
GEORGIA_ALLOWED_ENTITY_TYPES = frozenset({"LLC", "LP", "CORPORATION"})
# F8 (Josh, Oct 4 §2): "Owner is an LLC, LP or corporation, or a non owner occupied
# investor. Homestead is out." Universal (not Georgia-specific); same allowed set as
# the Georgia stop today, kept as its own name since the two rules can diverge later.
INVESTOR_ENTITY_TYPES = frozenset({"LLC", "LP", "CORPORATION"})
# List 4 (brokers and LOs) is a professional referral list, never screened as a
# property owner — the homestead/entity gate does not apply to it.
HOMESTEAD_GATE_EXCLUDED_SOURCE_TAGS = frozenset({"list_4"})

# Recipient timezone by area code (lending-owned copy; FA SMS keeps its own).
# 850 spans ET/CT → Central, the over-suppressing direction.
DEFAULT_TZ = "America/New_York"
AREA_CODE_TZ: dict[str, str] = {
    "850": "America/Chicago",
    **{ac: "America/New_York" for ac in (
        # Florida
        "239", "305", "321", "352", "386", "407", "561", "727", "754", "772",
        "786", "813", "863", "904", "941", "954",
        # Georgia
        "229", "404", "470", "478", "678", "706", "762", "770", "912",
    )},
}

# Every IANA zone a state touches, by USPS code. A state that spans zones lists them all: the intro-text window
# (compliance.text_window_open) must hold in each of them, the over-suppressing direction.
_ET, _CT, _MT, _PT = "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles"
STATE_TZ: dict[str, tuple[str, ...]] = {
    **{s: (_ET,) for s in ("CT", "DE", "DC", "GA", "ME", "MD", "MA", "NH", "NJ", "NY", "NC", "OH", "PA", "RI", "SC", "VT", "VA", "WV")},
    **{s: (_ET, _CT) for s in ("FL", "IN", "KY", "MI", "TN")},
    **{s: (_CT,) for s in ("AL", "AR", "IL", "IA", "LA", "MN", "MS", "MO", "OK", "WI")},
    **{s: (_CT, _MT) for s in ("KS", "NE", "ND", "SD", "TX")},
    **{s: (_MT,) for s in ("CO", "MT", "NM", "UT", "WY")},
    **{s: (_MT, _PT) for s in ("ID", "NV", "OR")},
    **{s: (_PT,) for s in ("CA", "WA")},
    "AZ": ("America/Phoenix",),
    "AK": ("America/Anchorage",),
    "HI": ("Pacific/Honolulu",),
}

# FA sms_opt_outs rows written by the Tracerfy DNC refresh are national-DNC hits,
# re-verified inside the scrub freshness window — never a permanent opt-out.
TRACERFY_DNC_SOURCE = "tracerfy_dnc_refresh"

# suppress_contact() source used by the dialer path; the poller skips it (already propagated).
DIALER_OPT_OUT_SOURCE = "lending_dialer"

# suppress_contact() sources that are deliverability failures, not a person saying
# "stop" (spec §3.2 covers STOP / UNSUBSCRIBE / verbal decline only). FA still blocks
# them on its own stores (ADR 0028); lending does not record them as opt-outs.
NON_OPT_OUT_SOURCES = frozenset({
    "mandrill_hard_bounce",
    "mandrill_reject",
    "mandrill_soft_bounce_threshold",
    "instantly_webhook_bounce",
})

# FA opt-out rows the lending poller/backfill must never treat as a person's opt-out.
OPT_OUT_EXCLUDED_SOURCES = frozenset(NON_OPT_OUT_SOURCES | {TRACERFY_DNC_SOURCE, DIALER_OPT_OUT_SOURCE})

# Postgres advisory-lock key: one poller cycle at a time across all processes.
OPT_OUT_POLL_LOCK_KEY = 7_302_020_801
DIALER_SWEEP_LOCK_KEY = 7_302_020_802
GHL_CALL_GATE_LOCK_KEY = 7_302_020_805
GHL_LIST_SCRUB_LOCK_KEY = 7_302_020_806

# Josh's GHL call lists (G5): one smart list per lead-type tag, each excluding GHL_DO_NOT_CALL_NOW_TAG.
# src.lending.ghl_call_gate adds that tag to a listed contact that cannot be called now and removes it
# when calling is allowed again. Lead-type tags are never added or removed: removing and re-adding
# "lendingflow" re-fires the Speed to Lead and API Catch workflows (re-entry on).
GHL_DO_NOT_CALL_NOW_TAG = "ndl-do-not-call-now"
GHL_CALL_LIST_TAGS = (
    "ndl-verified-maturity",
    "ndl-builder",
    "ndl-nurture",
    "lendingflow",
    "web-lead",
    "ndl-wholesaler",
    "ndl-partner",
    "ndl-transaction-ready",
)
# BatchDialer load -> GHL sync (src/lending/ghl_load_sync.py): dialer pool -> the call-list tag its contacts get.
# A pool missing here is never pushed (logged), so a new pool cannot reach GHL unmapped.
GHL_LOAD_SYNC_POOL_TAGS: dict[str, str] = {
    "verified_maturity": "ndl-verified-maturity",
    "transaction_ready": "ndl-transaction-ready",
    "builders": "ndl-builder",
    "partners": "ndl-partner",
    "wholesalers": "ndl-wholesaler",
    "nurture": "ndl-nurture",
    "lendingflow": "lendingflow",  # LendingFlow phones are warm, so the sync skips them; mapped for completeness
    "website": "web-lead",  # website phones are warm too (skipped); mapped for completeness
}
GHL_LOAD_SYNC_BATCH_SIZE = 200       # rows per run
GHL_LOAD_SYNC_MAX_ATTEMPTS = 5       # then status 'failed' and one ops notice
GHL_LOAD_SYNC_HALT_CHECK_EVERY = 10  # rows between emergency-halt rechecks inside a run
GHL_LOAD_SYNC_LOCK_KEY = 7_302_020_807
GHL_CALL_GATE_PAGE_SIZE = 100
GHL_CALL_GATE_MAX_PAGES = 20             # per tag per run (2,000 contacts a list)
GHL_CALL_GATE_ALERT_AFTER_MINUTES = 10   # ops alert once no run has succeeded for this long
# Listed numbers are rescrubbed (src.lending.ghl_list_scrub, hourly) once their newest scrub is this old,
# so none reaches DNC_SCRUB_MAX_AGE_DAYS and drops off Josh's lists. LendingFlow numbers are never scrubbed.
GHL_LIST_RESCRUB_AFTER_DAYS = 6

# FA suppress_contact() source → opt-out channel. Anything unlisted is email-origin.
SMS_OPT_OUT_SOURCES = frozenset({"inbound_sms", "twilio_inbound", "cascaded_from_sms"})


class OptOutChannel(str, Enum):
    SMS = "sms"
    EMAIL = "email"
    DIALER = "dialer"
    GHL = "ghl"


class OptOutStatus(str, Enum):
    PENDING = "pending"
    COMPLETE = "complete"
    DIALER_PENDING = "dialer_pending"


class SuppressionReason(str, Enum):
    OPT_OUT = "OPT_OUT"
    LITIGATOR = "LITIGATOR"
    # Josh's personal/warm network (Oct 4 §2): permanently suppressed from the cold
    # queue, never an opt-out event — he still works these relationships himself.
    WARM_NETWORK = "WARM_NETWORK"


class ReasonCode(str, Enum):
    """Shared exclusion reasons returned by every gate and stored in load_exclusions."""

    INVALID_PHONE = "INVALID_PHONE"
    NO_FRESH_SCRUB = "NO_FRESH_SCRUB"
    SCRUB_FAILED = "SCRUB_FAILED"
    NATIONAL_DNC = "NATIONAL_DNC"
    STATE_DNC = "STATE_DNC"
    LITIGATOR = "LITIGATOR"
    SUPPRESSED = "SUPPRESSED"
    GA_NATURAL_PERSON = "GA_NATURAL_PERSON"
    HOMESTEAD_OWNER_OCCUPIED = "HOMESTEAD_OWNER_OCCUPIED"
    OUTSIDE_CALL_WINDOW = "OUTSIDE_CALL_WINDOW"
    ATTEMPT_CAP_REACHED = "ATTEMPT_CAP_REACHED"
    ATTEMPT_HISTORY_EXCEEDED = "ATTEMPT_HISTORY_EXCEEDED"
    # Owned by WP-W0-4 (Developer 3); tag name is O6.
    BACKFLIP_CONFLICT = "BACKFLIP_CONFLICT"
    BACKFLIP_FEED_STALE = "BACKFLIP_FEED_STALE"
    BACKFLIP_FEED_UNAVAILABLE = "BACKFLIP_FEED_UNAVAILABLE"
    # Worked from GHL (warm lead, or called from GHL recently): never dialed by BatchDialer too.
    GHL_CALLING = "GHL_CALLING"


class RemovalReason(str, Enum):
    """Why lending pulled a contact from the Aircall pool. Developer 3's restore
    only reinstates CALL_WINDOW / ATTEMPT_CAP removals, never OPT_OUT."""

    OPT_OUT = "opt_out"
    CALL_WINDOW = "call_window"
    ATTEMPT_CAP = "attempt_cap"
    SCRUB_STALE = "scrub_stale"
    ATTEMPT_HISTORY = "attempt_history"
    # The phone is worked from GHL (a LendingFlow / website lead, or called from GHL recently): deleted from the
    # dialer by the sweep, never restored.
    GHL_CALLING = "ghl_calling"
    # The lead was booked or reached by a person in GHL: deleted from the dialer, never restored.
    HANDLED_IN_GHL = "handled_in_ghl"


# Removals that delete the dialer contact for good (never restored): an opt-out, a phone handed to GHL
# calling, and a lead handled in GHL. Every other reason only leaves the campaign and can be restored.
PERMANENT_REMOVAL_REASONS: frozenset[RemovalReason] = frozenset({
    RemovalReason.OPT_OUT, RemovalReason.GHL_CALLING, RemovalReason.HANDLED_IN_GHL,
})


def validate_lending_compliance_config() -> None:
    if CALL_WINDOW_START >= CALL_WINDOW_END:
        raise ValueError("CALL_WINDOW_START must be before CALL_WINDOW_END")
    if ET_WINDOW_START >= ET_WINDOW_END:
        raise ValueError("ET_WINDOW_START must be before ET_WINDOW_END")
    for group, (start, end) in SHIFT_GROUPS.items():
        if not (ET_WINDOW_START <= start < end <= ET_WINDOW_END):
            raise ValueError(f"shift group {group} must sit inside the ET window")
    if MAX_ATTEMPTS_PER_PERIOD < 1:
        raise ValueError("MAX_ATTEMPTS_PER_PERIOD must be >= 1")
    if OPT_OUT_POLL_SECONDS * 2 >= STOP_PROPAGATION_SLA_SECONDS:
        raise ValueError("OPT_OUT_POLL_SECONDS must leave room inside the stop-propagation SLA")
    if ATTEMPT_PERIOD_HOURS < 1 or DNC_SCRUB_MAX_AGE_DAYS < 1 or STOP_PROPAGATION_SLA_SECONDS < 1:
        raise ValueError("periods and SLAs must be positive")
    if not 1 <= WEEKLY_SCRUB_PERIOD_DAYS <= DNC_SCRUB_MAX_AGE_DAYS:
        raise ValueError("WEEKLY_SCRUB_PERIOD_DAYS must not exceed DNC_SCRUB_MAX_AGE_DAYS")
    if not 0 < SCRUB_STALE_BREAKER_PCT <= 100 or SCRUB_STALE_BREAKER_MIN_POOL < 1:
        raise ValueError("scrub-stale breaker thresholds must be positive (pct <= 100)")
    if not 0 < GHL_LIST_RESCRUB_AFTER_DAYS < DNC_SCRUB_MAX_AGE_DAYS:
        raise ValueError("GHL_LIST_RESCRUB_AFTER_DAYS must be positive and below DNC_SCRUB_MAX_AGE_DAYS")
    if GHL_DO_NOT_CALL_NOW_TAG in GHL_CALL_LIST_TAGS or len(set(GHL_CALL_LIST_TAGS)) != len(GHL_CALL_LIST_TAGS):
        raise ValueError("GHL_CALL_LIST_TAGS must be distinct and must not contain GHL_DO_NOT_CALL_NOW_TAG")
    if not set(GHL_LOAD_SYNC_POOL_TAGS.values()) <= set(GHL_CALL_LIST_TAGS):
        raise ValueError("GHL_LOAD_SYNC_POOL_TAGS must map pools to tags in GHL_CALL_LIST_TAGS")
    if min(GHL_LOAD_SYNC_BATCH_SIZE, GHL_LOAD_SYNC_MAX_ATTEMPTS, GHL_LOAD_SYNC_HALT_CHECK_EVERY) < 1:
        raise ValueError("GHL load sync batch size, max attempts and halt check interval must be positive")
