"""T-12 LendingFlow background enrichment card + pipeline routing: rule values."""
from __future__ import annotations

from zoneinfo import ZoneInfo

# SPEC §4.4: address present AND target close date within this many days -> FULL_MACHINE (inclusive).
FULL_MACHINE_MAX_DAYS = 30
ROUTING_TZ = ZoneInfo("America/New_York")  # "days until close" is counted on the Eastern calendar date

# Forced Action coverage: a card carries property facts only for a match in these counties.
COVERED_STATE = "FL"
COVERED_COUNTIES = ("hillsborough", "pinellas")

# Lender-fit score = % of lenders that fit (Josh A3: 3 of 5 = 60). Backflip is modelled as several
# rule rows under one lender, so rows are grouped by key prefix into one lender.
BACKFLIP_LENDER = "backflip"
BACKFLIP_KEY_PREFIX = "generic_backflip"

NOT_AVAILABLE = "Not available"
FIT_RULES_PENDING = "Lender rules pending confirmation"
CREDIT_STRADDLE_NOTE = "needs confirmation on the call"  # Josh A4

# Comps shown in the deal thread (internal only; never sent to the borrower).
COMPS_SHOWN = 5
OFFICERS_SHOWN = 5
DEEDS_SHOWN = 3
PERMITS_SHOWN = 3
# The ARV engine normalises comps to a target repaired condition (1-5); 3 = "Average", the neutral value
# the engine itself uses when condition is unknown. Josh has not specified one.
COMPS_AFTER_REPAIR_CONDITION = 3

# Sweep: minutes to wait after the Nth failed attempt (last value repeats), give up after the cap.
RETRY_BACKOFF_MINUTES = (1, 5, 15, 60)
MAX_ATTEMPTS = 8
RUNNING_STALE_MINUTES = 10
SWEEP_BATCH_SIZE = 50

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_READY = "ready"
STATUS_FAILED = "failed"
