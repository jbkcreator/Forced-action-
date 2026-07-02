"""
Predictive Engagement & Churn Defense configuration (Task 6.3, Phase 6).

Calibration constants for the weekly engagement-decay worker
(src/tasks/churn_defense_engagement_decay.py). The formula and thresholds come
directly from the Phase 5 & 6 Developer Specification, page 8.
"""

# ── Engagement-decay formula weights (spec p.8) ────────────────────────────
# engagement_decay = (views_7 * VIEW_WEIGHT + downloads_7 * DOWNLOAD_WEIGHT)
#                    / (baseline_30day_mean + 1)
VIEW_WEIGHT = 0.4
DOWNLOAD_WEIGHT = 0.6

# ── Trigger threshold (spec p.8) ───────────────────────────────────────────
# Decay strictly below this (>65% drop vs baseline) flags the account.
DECAY_THRESHOLD = 0.35

# ── Rolling windows ────────────────────────────────────────────────────────
RECENT_WINDOW_DAYS = 7
BASELINE_WINDOW_DAYS = 30
# baseline_30day_mean = weighted 30-day activity / BASELINE_WEEKS
# (weekly-equivalent mean, so a steady user's recent-7-day activity ~= baseline).
BASELINE_WEEKS = BASELINE_WINDOW_DAYS / RECENT_WINDOW_DAYS  # 30/7 ≈ 4.2857

# ── Cold-start guard (history-based, NOT account age) ──────────────────────
# An account can only be flagged once it has at least this many days of TRACKED
# engagement history (measured from its first tracked event) AND a non-zero
# baseline. This prevents flagging brand-new / never-engaged accounts and, more
# importantly, prevents a launch-day false-positive flood: every established
# account has zero tracked events until the frontend listeners start emitting,
# so nothing is flagged for the first ~30 days of real data.
MIN_BASELINE_HISTORY_DAYS = 30

# ── Outreach de-duplication ────────────────────────────────────────────────
# Do not stage a new churn_defense_leads row for a subscriber that already has a
# non-terminal (not CONVERTED) row within this window.
OUTREACH_COOLDOWN_DAYS = 30

# ── Storage guard ──────────────────────────────────────────────────────────
# engagement_decay_scalar is numeric(3,2) (max 9.99). A very active user with a
# near-zero baseline can exceed that; clamp before insert to avoid overflow.
DECAY_SCALAR_MAX = 9.99

# ── GoHighLevel ────────────────────────────────────────────────────────────
# Tag applied to the subscriber's GHL contact; presence fires the GHL-side
# retention check-in workflow.
GHL_CHURN_DEFENSE_TAG = "churn_defense"
