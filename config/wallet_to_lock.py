"""
Wallet-to-Lock upgrade detection config.

Tune LOCK_THRESHOLD_CREDITS after first 30 days of production data.
"""

# Credits spent in a single ZIP within WINDOW_DAYS that triggers a Lock close
LOCK_THRESHOLD_CREDITS: int = 40

# Rolling window for credit aggregation
LOCK_WINDOW_DAYS: int = 30

# Idempotency: one Cora event per subscriber per ZIP per calendar month
LOCK_IDEMPOTENCY_WINDOW: str = "%Y-%m"  # strftime format

# Tiers that qualify as "already on Lock or above" — skip these
LOCK_OR_ABOVE_TIERS = frozenset({
    "annual_lock",
    "autopilot_lite",
    "autopilot_pro",
    "annual_lock",
    "partner",
    "white_label",
})
