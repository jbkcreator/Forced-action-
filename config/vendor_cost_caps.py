"""
Hard per-pause_target daily cost caps for vendor cost monitoring.
Code-config only — no DB-backed editing in v1.
"""

# Daily hard caps in USD, keyed by pause_target.
# Used when historic data is insufficient for anomaly detection (< 5 non-zero days).
HARD_CAPS_USD: dict[str, dict[str, float | None]] = {
    "default": {
        "soft_cap_usd": None,       # No soft cap by default
        "hard_cap_usd": 20.0,       # Generic hard cap
    },
    "ap_lite_sweep": {
        "soft_cap_usd": 5.0,
        "hard_cap_usd": 10.0,
    },
    "accelerated_wallet_push": {
        "soft_cap_usd": 3.0,
        "hard_cap_usd": 8.0,
    },
    "wallet_to_lock": {
        "soft_cap_usd": 2.0,
        "hard_cap_usd": 5.0,
    },
    "bundle_dispatcher": {
        "soft_cap_usd": 5.0,
        "hard_cap_usd": 15.0,
    },
    "retention_event_producer": {
        "soft_cap_usd": 3.0,
        "hard_cap_usd": 8.0,
    },
    "nws_poll": {
        "soft_cap_usd": 1.0,
        "hard_cap_usd": 3.0,
    },
    "synthflow_voice_drop": {
        "soft_cap_usd": 5.0,
        "hard_cap_usd": 15.0,
    },
    "learning_card": {
        "soft_cap_usd": 2.0,
        "hard_cap_usd": 5.0,
    },
}

# Global defaults for anomaly detection
LOOKBACK_DAYS: int = 14
MIN_HISTORY_DAYS: int = 5  # Minimum non-zero usage days before anomaly detection kicks in