"""Lending Engine Wave 0 compliance floor — rule values (spec §3.1, §3.2).

Lending-only: FA SMS/voice keeps its own 8 AM–9 PM window and
``settings.dnc_recheck_days`` (30). See
lender-engine/dev2-compliance-floor/GRILL-DECISIONS.md.
"""
from __future__ import annotations

from datetime import time
from enum import Enum

CALL_WINDOW_START = time(8, 0)   # inclusive, recipient local time
CALL_WINDOW_END = time(20, 0)    # exclusive
MAX_ATTEMPTS_PER_PERIOD = 3
ATTEMPT_PERIOD_HOURS = 24        # rolling
DNC_SCRUB_MAX_AGE_DAYS = 31
STOP_PROPAGATION_SLA_SECONDS = 60
GEORGIA_ALLOWED_ENTITY_TYPES = frozenset({"LLC", "LP", "CORPORATION"})


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
    OUTSIDE_CALL_WINDOW = "OUTSIDE_CALL_WINDOW"
    ATTEMPT_CAP_REACHED = "ATTEMPT_CAP_REACHED"
    BACKFLIP_CONFLICT = "BACKFLIP_CONFLICT"  # owned by WP-W0-4; tag name pending O6


def validate_lending_compliance_config() -> None:
    if CALL_WINDOW_START >= CALL_WINDOW_END:
        raise ValueError("CALL_WINDOW_START must be before CALL_WINDOW_END")
    if MAX_ATTEMPTS_PER_PERIOD < 1:
        raise ValueError("MAX_ATTEMPTS_PER_PERIOD must be >= 1")
    if ATTEMPT_PERIOD_HOURS < 1 or DNC_SCRUB_MAX_AGE_DAYS < 1 or STOP_PROPAGATION_SLA_SECONDS < 1:
        raise ValueError("periods and SLAs must be positive")
