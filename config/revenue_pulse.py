"""
Revenue Pulse config - Item 9.

Daily + weekly founder SMS template and kill-switch thresholds.
"""

PULSE_SCHEDULE = {
    "daily":  "30 7 * * *",   # 7:30 AM UTC (after 7 AM scoring)
    "weekly": "0 9 * * 1",    # Monday 9 AM UTC
}

KILL_SWITCH_LEVELS = [
    {
        "status": "GREEN",
        "min_avg_revenue_score": 60,
        "max_churn_rate_pct": 5,
        "label": "healthy",
    },
    {
        "status": "YELLOW",
        "min_avg_revenue_score": 40,
        "max_churn_rate_pct": 10,
        "label": "watch churn",
    },
    {
        "status": "RED",
        "min_avg_revenue_score": 0,
        "max_churn_rate_pct": 999,
        "label": "investigate",
    },
]

MAX_DAILY_SMS_CHARS = 320

# ── Kill-Switch Metrics for Weekly Scorecard ────────────────────────────────
# These 7 active metrics are graded Green/Yellow/Red every Monday in Revenue
# Pulse. Each entry maps metric_name → abbreviated label for the SMS line.
# Metrics that are intentionally inactive (cac_paid_channels, sms_cost_per_signup)
# or partially computed with different semantics (free_tier_cost_ratio) are
# excluded from the scorecard — they'd always show '?' and add noise.
KILL_SWITCH_METRICS_WEEKLY = [
    ("first_payment_rate",     "FPR"),
    ("saved_card_rate",        "SCR"),
    ("wallet_adoption",        "WA"),
    ("lock_conversion",        "LC"),
    ("retention_30d",          "R30"),
    ("sms_reply_rate",         "SMS"),
    ("offer_acceptance_rate",  "OAR"),
]

MAX_KILL_SWITCH_SCORECARD_CHARS = 80

DAILY_PULSE_TEMPLATE = (
    "FA {date}: {lead_count} leads | {lanes} lanes | {wallet_active} wallets | {top_deal}\n"
    "Alert: {alert}\n"
    "{vendor_cost}"
    "Signal: {kill_switch}"
)

VENDOR_COST_LINE_MAX_CHARS = 80

WEEKLY_PULSE_TEMPLATE = (
    "FA Wk{week}: ${revenue} est | +{new_subs} subs | -{churned} churned\n"
    "Kill switch: {kill_switch} ({kill_label})\n"
    "Top learning: {learning}"
)
