"""
Stage 10 — A/B + Self-Healing Configuration

Guardrails and thresholds for:
  - 3-variant message mutation (retire / replace / prove cycle)
  - Pricing cohort activation (per-trade, per-county)
  - Self-healing incident triggers specific to Stage 10

These supplement (never override) config/lifecycle_guardrails.py.
"""

# ── 3-Variant Message Mutation ───────────────────────────────────────────────

VARIANT_TEST = {
    # Retire the lowest-performing slot after this many sends.
    "retire_after_sends": 200,
    # Replacement variant must beat retired slot within this many sends or revert.
    "prove_within_sends": 200,
    # Hard cap on traffic entering the 3-variant pool (guardrail: 10% of segment).
    "traffic_cap_pct": 10,
    # Z-score threshold — auto-rollback a variant if it falls this many sigma
    # below the current best active slot.
    "rollback_sigma_threshold": 2.0,
    # Minimum sends per slot before statistical comparison is valid.
    "min_sends_for_comparison": 30,
    # Claude Haiku model used for replacement copy generation.
    "haiku_model": "claude-haiku-4-5-20251001",
    # Max tokens for replacement generation.
    "haiku_max_tokens": 256,
    # Max characters for a generated SMS body.
    "max_sms_chars": 160,
}

# ── Pricing Cohort Activation Gates ─────────────────────────────────────────

PRICING_COHORT = {
    # Minimum weeks of deal data required before per-trade/county adjustments activate.
    "activation_min_weeks": 6,
    # Minimum number of deals for a cohort to be statistically meaningful.
    "min_deal_count": 10,
    # Maximum price adjustment in either direction (± pct of base price).
    "max_price_adjustment_pct": 25,
    # Sigma threshold for rollback trigger (conv rate drops beyond this → rollback).
    "rollback_trigger_sigma": 2.0,
    # Allowed price types for cohort overrides.
    # "lock"/"wallet_*"/"bundle" belong to the Wallet/Territory-Lock revenue-ladder
    # funnel (config/revenue_ladder.py). "starter"/"pro"/"founder"/"annual_lock"
    # are the separate Block 1 storefront subscription tiers (src/api/deps.py
    # VALID_TIERS) — distinct product, same generic cohort mechanism.
    "allowed_price_types": [
        "lock",
        "wallet_starter",
        "wallet_growth",
        "wallet_power",
        "bundle",
        "starter",
        "pro",
        "annual_lock",
    ],
    # Allowed trade verticals for cohort overrides.
    "allowed_trade_verticals": [
        "wholesalers",
        "fix_flip",
        "roofing",
        "attorneys",
        "insurance",
        "tax_investors",
    ],
}

# ── Stage 10 Self-Healing Additions ─────────────────────────────────────────

# Extends KILL_SWITCH in lifecycle_guardrails.py with Stage 10-specific knobs.
# Merged into KILL_SWITCH at runtime by self-healing; never replaces it.
STAGE10_KILL_SWITCH_OVERRIDES = {
    # Override first_payment_rate: enable auto variant promotion in Stage 10.
    "first_payment_rate": {
        "auto_action_type": "variant_promotion",
        "variant_sequence_name": "wallet_push_v1",
        "requires_approval": False,
        "duration_hours_for_action": 48,
        # Alert threshold used in Stage 10 spec (stricter than the green/red bands).
        "stage10_alert_threshold": 25,
    },
}

# ── Prometheus Metrics Config ────────────────────────────────────────────────

PROMETHEUS = {
    # Controlled by settings.prometheus_enabled (PROMETHEUS_ENABLED in .env).
    # Hardcoded False here is the fallback default; the env var overrides it.
    "enabled": False,
    # Namespace for all Stage 10 Prometheus metrics.
    "namespace": "lifecycle_stage10",
    # Labels exposed per variant metric.
    "variant_labels": ["sequence_name", "slot"],
    # Labels exposed per pricing cohort metric.
    "cohort_labels": ["county_id", "trade_vertical", "price_type"],
}
