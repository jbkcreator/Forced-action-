"""
AutoPilot Lite upgrade detection config.
"""

# Action types that count as "manual" work AP Lite would automate
MANUAL_ACTION_TYPES: frozenset = frozenset({
    "lead_unlock",
    "skip_trace",
    "outbound_text",
    "voicemail",
})

# Weekly threshold triggering AP Lite upsell offer
AP_LITE_THRESHOLD_PER_WEEK: int = 10

# Only Lock-tier subscribers are candidates (annual_lock is the DB tier name)
AP_LITE_ELIGIBLE_TIERS = frozenset({"annual_lock"})

# Idempotency: one Cora event per subscriber per ISO week
AP_LITE_IDEMPOTENCY_WINDOW: str = "%Y-W%W"  # strftime ISO week format
