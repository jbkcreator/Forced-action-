"""config/fa_max_stage_monitoring.py

WP-T2-6 thresholds -- config-as-data per CLAUDE.md ("every threshold is
configurable, not hardcoded"). Mirrors config/learning_hygiene.py's
validate_*_config() convention.

Defaults chosen against the client's own stated tolerance ("a borrower who
hears nothing for a week calls other lenders" -> 5 business days) rather
than an arbitrary number -- see the plan doc for full reasoning. All are
plain module constants (not env-driven) because nothing else in the FA Max
config surface makes per-threshold values env-configurable either
(abandonment's _TOUCH_OFFSETS is the same pattern) -- change here, not via
.env, if the client wants different numbers.
"""
from __future__ import annotations

# A file with no stage change for this many business days is flagged
# stalled and surfaced to EXCEPTIONS. Matches the client's own stated
# tolerance for borrower silence.
STALL_THRESHOLD_BUSINESS_DAYS: float = 5.0

# Every open file gets a proactive status touch at least this often.
STATUS_TOUCH_INTERVAL_BUSINESS_DAYS: float = 5.0

# Document chase: first touch is immediate on request detection (no
# threshold needed). Follow-up fires this many business days after the
# first touch if the doc still hasn't arrived.
DOC_CHASE_FOLLOWUP_BUSINESS_DAYS: float = 2.0

# Escalate to EXCEPTIONS this many business days after the original request
# (4 total from the original request) if still outstanding.
DOC_CHASE_ESCALATE_BUSINESS_DAYS: float = 4.0

# Backflip-side stage taxonomy this build tracks. NOT the same enum as
# fa_max_opportunity_stage_config.stage_key (the coarse borrower-journey
# FSM) -- see Assumption 6 in the plan doc. Unconfirmed against Backflip's
# real portal terminology (Q13/Q14 open) -- revisit once real samples land.
BACKFLIP_STAGE_KEYS: frozenset[str] = frozenset({
    "submitted",
    "under_review",
    "conditional_approval",
    "docs_requested",
    "cleared_to_close",
    "funded",
    "declined",
})

TERMINAL_BACKFLIP_STAGES: frozenset[str] = frozenset({"funded", "declined"})


def validate_stage_monitoring_config() -> None:
    """Fail fast at import/startup time on an inconsistent threshold set."""
    if not TERMINAL_BACKFLIP_STAGES <= BACKFLIP_STAGE_KEYS:
        raise ValueError("TERMINAL_BACKFLIP_STAGES must be a subset of BACKFLIP_STAGE_KEYS")
    if DOC_CHASE_ESCALATE_BUSINESS_DAYS <= 0 or DOC_CHASE_FOLLOWUP_BUSINESS_DAYS <= 0:
        raise ValueError("document chase thresholds must be positive")
    if STALL_THRESHOLD_BUSINESS_DAYS <= 0 or STATUS_TOUCH_INTERVAL_BUSINESS_DAYS <= 0:
        raise ValueError("stall/status-touch thresholds must be positive")
