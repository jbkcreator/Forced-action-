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
    "first_payment_rate":  {
        "green": 30, "yellow": (20, 30), "red": 20,
        "action": "simplify proof, cut friction",
        # fa034 self-healing knobs. first_payment_rate touches the checkout
        # funnel + pricing — too sensitive for autonomous action. At 48hr
        # in red Cora opens an incident, posts Slack, surfaces in Revenue
        # Pulse; human decides. Never auto-pauses or auto-falls-back.
        "duration_hours_for_action": 48,
        "auto_action_type": "human_escalated",
        "fallback_feature_flag": None,
        "requires_approval": True,
        "kill_after_red_days": 7,
        # Metric direction: higher is better. A drop BELOW threshold is bad.
        "direction": "higher_is_better",
    },
    "saved_card_rate":     {
        "green": 70, "yellow": (50, 70), "red": 50,
        "action": "default harder, bonus credits",
        "duration_hours_for_action": 48,
        "auto_action_type": "human_escalated",
        "fallback_feature_flag": None,
        "requires_approval": True,
        "kill_after_red_days": 7,
        "direction": "higher_is_better",
    },
    "wallet_adoption":     {
        "green": 15, "yellow": (10, 15), "red": 10,
        "action": "trigger sooner, missing-leads frame",
        # fa016 Accelerated Wallet Push — Day-35 rollback floor.
        # If accelerated_wallet_push_take_rate < floor_pct after floor_check_after_days,
        # kill_switch_metric_ingest flips Redis kill_switch:accelerated_wallet_push=red.
        "floor_pct": 12,
        "floor_check_after_days": 35,
        # fa034 self-healing: wallet adoption tolerates auto-fallback —
        # downgrading accelerated_wallet_push to its baseline triggers is
        # reversible and doesn't touch pricing.
        "duration_hours_for_action": 48,
        "auto_action_type": "fallback_enabled",
        "fallback_feature_flag": "accelerated_wallet_push_paused",
        "requires_approval": False,
        "kill_after_red_days": 7,
        "direction": "higher_is_better",
    },
    "lock_conversion":     {
        "green": 5,  "yellow": (3, 5),   "red": 3,
        "action": "live-data close, voice drop, urgency",
        # Auto-fallback drops Cora to static template SMS for the lock
        # close path, away from Claude-composed copy. Reversible.
        "duration_hours_for_action": 48,
        "auto_action_type": "fallback_enabled",
        "fallback_feature_flag": "lock_close_use_fallback",
        "requires_approval": False,
        "kill_after_red_days": 7,
        "direction": "higher_is_better",
    },
    "retention_30d":       {
        "green": 70, "yellow": (55, 70), "red": 55,
        "action": "earlier saves, missed-opp summaries",
        # Retention drift is structural — needs human attention, not a
        # mid-stream automated fix.
        "duration_hours_for_action": 72,    # slower-moving metric
        "auto_action_type": "human_escalated",
        "fallback_feature_flag": None,
        "requires_approval": True,
        "kill_after_red_days": 14,           # longer kill window
        "direction": "higher_is_better",
    },
    "sms_reply_rate":      {
        "green": 8,  "yellow": (5, 8),   "red": 5,
        "action": "swap copy, change timing",
        # Reply-rate drop = copy/timing issue. Fallback to static template
        # is the spec-approved automatic response.
        "duration_hours_for_action": 48,
        "auto_action_type": "fallback_enabled",
        "fallback_feature_flag": "cora_use_static_copy",
        "requires_approval": False,
        "kill_after_red_days": 7,
        "direction": "higher_is_better",
    },
    "cac_paid_channels":   {
        "green": 25, "yellow": (25, 40), "red": 40,
        "action": "pause channel, fix targeting",
        # CAC blow-up triggers human review — pausing a channel is a real
        # business decision, not a copy change. Direction: LOWER is better.
        "duration_hours_for_action": 168,   # 7 days per spec
        "auto_action_type": "human_escalated",
        "fallback_feature_flag": None,
        "requires_approval": True,
        "kill_after_red_days": 7,
        "direction": "lower_is_better",
    },
    "free_tier_cost_ratio": {
        "green": 40, "yellow": (40, 50), "red": 50,
        "action": "tighten free cap, earlier wall",
        # KNOWN GAP: free_tier_cost_ratio is not currently computed by
        # kill_switch_metric_ingest (no cost-allocation table). The guardrail
        # remains in place so when data becomes available, the self-healing
        # job will start enforcing it automatically.
        "duration_hours_for_action": 72,
        "auto_action_type": "human_escalated",
        "fallback_feature_flag": None,
        "requires_approval": True,
        "kill_after_red_days": 14,
        "direction": "lower_is_better",
    },
    # SMS unit-cost guardrail. Calibrated to Telnyx pricing ($0.004/segment, May 2026),
    # which is ~52% cheaper than the Twilio rate this guardrail was originally tuned for.
    # GREEN ≤$1/signup, YELLOW $1–2, RED >$2.
    "sms_cost_per_signup": {
        "green": 1, "yellow": (1, 2),  "red": 2,
        "action": "pause offending sequence",
        # KNOWN GAP: per-message cost not tracked on MessageOutcome since
        # the project moved to Telnyx (May 2026). Guardrail kept for when
        # cost data becomes available.
        "duration_hours_for_action": 48,
        "auto_action_type": "human_escalated",
        "fallback_feature_flag": None,
        "requires_approval": True,
        "kill_after_red_days": 7,
        "direction": "lower_is_better",
    },
    "offer_acceptance_rate": {
        # Wallet-push offers + bundle offers. Spec example: drop from 35% → 12%.
        # Below 15% is yellow, below 10% is red.
        "green": 15, "yellow": (10, 15), "red": 10,
        "action": "swap variants, lower friction",
        "duration_hours_for_action": 48,
        "auto_action_type": "fallback_enabled",
        "fallback_feature_flag": "offer_use_baseline_template",
        "requires_approval": False,
        "kill_after_red_days": 7,
        "direction": "higher_is_better",
    },
}


# ── fa034 Cora self-healing — rate limits and global knobs ──────────────────
# Safety rails on the hourly self-healing job. Reads these at the top of every
# run; aborts (with a WARN log) when a limit is hit.
CORA_SELF_HEALING = {
    # Max number of automatic-actions taken in a single run (across all metrics).
    # Prevents a multi-metric breach from triggering a cascade of changes Cora
    # can't reason about in one pass.
    "max_actions_per_run": 3,
    # Cap on kill-recommendation rows per day. A "kill" is a Slack message
    # asking Josh to disable a feature; one per day is plenty for human review.
    "max_feature_kill_recommendations_per_day": 1,
    # Cap on new incidents opened per hour — prevents a metric storm from
    # opening 50 nearly-identical rows.
    "max_new_incidents_per_hour": 5,
    # Baseline window for compute_baseline() in kill_switch_metric_ingest.
    "baseline_window_days": 7,
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
