"""
Churn Risk scoring configuration (fa051).

All values here are calibration parameters — tuned conservatively at launch
to bias toward precision (fewer false positives) over recall.
Adjust after churn_validation_report shows sane lead-time distributions.
"""

# ── Signal weights (must sum to 1.0) ──────────────────────────────────────
# Three additive risk signals; engagement_dampener is a separate reducer.
WEIGHTS = {
    "inactivity_trajectory": 0.45,
    "usage_slope":           0.35,
    "payment_stress":        0.20,
}

# ── Dampener strength ─────────────────────────────────────────────────────
# Fraction by which strong engagement (score=1.0) reduces the raw risk score.
# At 0.40: final = raw * (1 - 0.40 * dampener_score).
DAMPENER_MAX_REDUCTION = 0.40

# ── Band thresholds (inclusive lower bounds) ──────────────────────────────
# Mirror Revenue Signal Score bands for consistency.
BAND_THRESHOLDS = [
    (80, "very_high"),
    (60, "high"),
    (30, "medium"),
    (0,  "low"),
]

# ── Proactive-save firing criteria ────────────────────────────────────────
# Band must be in this set AND predicted_inactivity_at must be ≤ HORIZON_DAYS.
FIRE_BANDS = frozenset({"high", "very_high"})
HORIZON_DAYS = 3  # days out from predicted_inactivity_at to fire save offer

# ── Cooldown ──────────────────────────────────────────────────────────────
COOLDOWN_DAYS = 21  # min days between save offer sends per subscriber

# ── Save Offer Holdout ────────────────────────────────────────────────────
HOLDOUT_PCT = 10  # integer: 10 means 10%, stable hash-based

# ── New-account fallback ──────────────────────────────────────────────────
# Accounts younger than this fall back to global flat thresholds in signals.
NEW_ACCOUNT_DAYS = 21
