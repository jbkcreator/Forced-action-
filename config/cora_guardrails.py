"""
Cora Guardrail Ranges — Phase 2B

Concrete bounds for every autonomous Cora decision. Cora can optimize freely
within these ranges. Anything outside requires Josh approval via Revenue Pulse.

Usage:
    from config.cora_guardrails import GUARDRAILS, get_guardrail
"""


GUARDRAILS = {
    "lock_pricing": {
        "label": "Lock pricing",
        "min_cents": 14700,               # $147/mo
        "max_cents": 24700,               # $247/mo
        "unit": "cents/mo",
        "rollback_trigger": "Conv rate drops >2 std devs vs control for 48 hrs",
    },
    "wallet_tier_pricing": {
        "label": "Wallet tier pricing",
        "min_cents": 3900,                # $39/mo
        "max_cents": 24900,               # $249/mo
        "unit": "cents/mo",
        "rollback_trigger": "Conv rate drops >2 std devs vs control for 48 hrs",
    },
    "bundle_pricing": {
        "label": "Bundle pricing",
        "variance_pct": 25,               # ±25% of base price
        "rollback_trigger": "Margin drops below 60%",
    },
    "discount_max": {
        "label": "Discount max (any offer)",
        "max_pct": 20,                    # 20% off list
        "rollback_trigger": "Never exceed",
        "hard_limit": True,
    },
    "credit_bonus_max": {
        "label": "Credit bonus max",
        "max_credits": 10,                # per event
        "rollback_trigger": "Never exceed per event",
        "hard_limit": True,
    },
    "ab_test_traffic_cap": {
        "label": "A/B test traffic cap",
        "max_pct": 10,                    # % of segment
        "rollback_trigger": "Auto-rollback if losing variant >2 std devs",
    },
    "message_variant_swap": {
        "label": "Message variant swap",
        "retire_after_sends": 200,        # retire lowest of 3 after 200 sends
        "prove_within_sends": 200,        # new variant must beat retired within 200 sends
        "rollback_trigger": "New variant must beat retired within 200 sends or revert",
    },
    "urgency_window": {
        "label": "Urgency window duration",
        "min_minutes": 10,
        "max_minutes": 60,
        "rollback_trigger": "Never shorten below 10 min",
    },
    "save_offer": {
        "label": "Save offer (downgrade)",
        "allowed_offers": ["data_only_97", "pause_60_days"],
        "rollback_trigger": "No lower offers without approval",
    },
    "annual_discount": {
        "label": "Annual discount",
        "max_discount": "2 months free",
        "max_annual_cents": 197000,       # $1,970/yr
        "rollback_trigger": "No deeper annual discounts",
        "hard_limit": True,
    },
    "auto_reload_threshold": {
        "label": "Auto-reload threshold",
        "threshold_credits": 5,           # <5 credits triggers reload
        "rollback_trigger": "Never change threshold without approval",
        "hard_limit": True,
    },
    "paid_acquisition_spend": {
        "label": "Paid acquisition spend",
        "min_cents_per_week": 50000,      # $500/wk per channel
        "max_cents_per_week": 200000,     # $2,000/wk per channel
        "rollback_trigger": "Pause if CAC >$25 for 7 days",
    },
    "county_activation": {
        "label": "County activation",
        "gates_required": "all_7_green",
        "rollback_trigger": "Never override gates",
        "hard_limit": True,
    },
}


# ── 7 Expansion Gates ────────────────────────────────────────────────────────
# All must be green before any new county or ICP expansion channel activates.

EXPANSION_GATES = {
    "first_payment_rate":   {"threshold_pct": 30, "description": ">=30% of free users within 30 days"},
    "saved_card_rate":      {"threshold_pct": 70, "description": ">=70% of payers within 7 days"},
    "wallet_adoption":      {"threshold_pct": 15, "description": ">=15% of saved-card users within 30 days"},
    "lock_conversion":      {"threshold_pct": 5,  "description": ">=5% of free users within 60 days"},
    "payer_retention_30d":  {"threshold_pct": 70, "description": ">=70% 30-day payer retention"},
    "free_tier_cost_ratio": {"threshold_pct": 40, "description": "<=40% of revenue"},
    "county_profitability": {"threshold": "net_positive_monthly", "description": "Net positive monthly"},
}


# ── Kill-Switch Thresholds ───────────────────────────────────────────────────
# Every channel and feature gets a 4-week window. Green/Yellow/Red scoring.
# Red for 7 days after adjustment → kill or pivot.

KILL_SWITCH = {
    "first_payment_rate":  {"green": 30, "yellow": (20, 30), "red": 20, "action": "simplify proof, cut friction"},
    "saved_card_rate":     {"green": 70, "yellow": (50, 70), "red": 50, "action": "default harder, bonus credits"},
    "wallet_adoption":     {"green": 15, "yellow": (10, 15), "red": 10, "action": "trigger sooner, missing-leads frame"},
    "lock_conversion":     {"green": 5,  "yellow": (3, 5),   "red": 3,  "action": "live-data close, voice drop, urgency"},
    "retention_30d":       {"green": 70, "yellow": (55, 70), "red": 55, "action": "earlier saves, missed-opp summaries"},
    "sms_reply_rate":      {"green": 8,  "yellow": (5, 8),   "red": 5,  "action": "swap copy, change timing"},
    "cac_paid_channels":   {"green": 25, "yellow": (25, 40), "red": 40, "action": "pause channel, fix targeting"},
    "free_tier_cost_ratio": {"green": 40, "yellow": (40, 50), "red": 50, "action": "tighten free cap, earlier wall"},
    "twilio_cost_per_signup": {"green": 2, "yellow": (2, 4),  "red": 4,  "action": "pause offending sequence"},
}


def get_guardrail(name: str) -> dict:
    """Look up a guardrail by name. Raises KeyError if not found."""
    return GUARDRAILS[name]


def is_within_guardrail(name: str, value: float) -> bool:
    """Check if a value is within the allowed range for a guardrail."""
    g = GUARDRAILS[name]
    if "min_cents" in g and "max_cents" in g:
        return g["min_cents"] <= value <= g["max_cents"]
    if "max_pct" in g:
        return value <= g["max_pct"]
    if "max_credits" in g:
        return value <= g["max_credits"]
    return True  # guardrails without numeric bounds (e.g. save_offer) need custom checks
