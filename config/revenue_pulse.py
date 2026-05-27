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

DAILY_PULSE_TEMPLATE = (
    "FA {date}: {lead_count} leads | {wallet_active} wallets | {top_deal}\n"
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
