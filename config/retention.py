"""
Retention event producer config.

Inactivity thresholds per tier — days since last engagement before
a retention_summary_due event fires.
"""

from typing import Dict

# Days of inactivity per tier before retention event fires
RETENTION_CADENCE_DAYS: Dict[str, int] = {
    "wallet":         3,
    "annual_lock":    5,
    "autopilot_lite": 5,
    "autopilot_pro":  7,
}

# Tiers excluded from retention events (non-paying or churned)
RETENTION_EXCLUDED_TIERS = frozenset({"free", "data_only"})

# Idempotency: max one retention event per subscriber per calendar day
RETENTION_IDEMPOTENCY_WINDOW: str = "%Y%m%d"
