"""
Revenue Ladder — Phase 2B

Defines the full 12-step monetization ladder, wallet tiers, bundles,
annual plan, and Data-Only save tier. Each step specifies pricing,
trigger conditions, and the Stripe price ID env var name.

Usage:
    from config.revenue_ladder import REVENUE_LADDER, WALLET_TIERS, BUNDLES
"""

# ── Revenue Ladder Steps ─────────────────────────────────────────────────────
# Ordered by progression. Each step's `trigger` describes what moves a user here.

REVENUE_LADDER = [
    {
        "step": 1,
        "name": "free_signup",
        "label": "Free Signup",
        "price_cents": 0,
        "billing": None,
        "trigger": "DBPR email / Cora SMS / missed call / referral",
        "sold_by": "automated",
    },
    {
        "step": 2,
        "name": "proof_moment",
        "label": "Proof Moment",
        "price_cents": 0,
        "billing": None,
        "trigger": "Signup complete — 1 enriched lead free, 2 blurred",
        "sold_by": "product",
    },
    {
        "step": 3,
        "name": "paid_unlock",
        "label": "Paid Unlock",
        "price_cents_range": (250, 700),  # $2.50–$7.00 per unlock
        "billing": "per_action",
        "trigger": "Tap to unlock blurred lead",
        "sold_by": "product",
        "stripe_price_env": "stripe_price_paid_unlock",
    },
    {
        "step": 4,
        "name": "wallet",
        "label": "Credit Wallet",
        "price_cents_range": (4900, 19900),  # $49–$199/mo depending on tier
        "billing": "monthly",
        "trigger": "Dynamic enrollment — see WALLET_ENROLLMENT_TRIGGERS",
        "sold_by": "auto_enrollment",
    },
    {
        "step": 5,
        "name": "auto_mode",
        "label": "Auto Mode",
        "price_cents_range": (7900, 9900),  # $79–$99/mo add-on
        "billing": "monthly",
        "trigger": "Included in Growth/Power wallets; $79–$99 add-on for Starter wallet",
        "sold_by": "cora",
        "stripe_price_env": "stripe_price_auto_mode",
    },
    {
        "step": 6,
        "name": "territory_lock",
        "label": "Territory Lock",
        "price_cents": 19700,  # $197/mo
        "billing": "monthly",
        "trigger": "FOMO + Cora conversational close + scarcity + crowding",
        "sold_by": "cora",
        "stripe_price_env": "stripe_price_territory_lock",
    },
    {
        "step": 7,
        "name": "autopilot_lite",
        "label": "AutoPilot Lite",
        "price_cents": 29900,  # $299/mo
        "billing": "monthly",
        "trigger": "Lock holders with 10+ manual actions/week",
        "sold_by": "cora",
        "stripe_price_env": "stripe_price_autopilot_lite",
    },
    {
        "step": 8,
        "name": "autopilot_pro",
        "label": "AutoPilot Pro",
        "price_cents": 49700,  # $497/mo
        "billing": "monthly",
        "trigger": "Lite users 30+ days with high close rate",
        "sold_by": "cora",
        "stripe_price_env": "stripe_price_autopilot_pro",
    },
    {
        "step": 9,
        "name": "annual_lock",
        "label": "Annual Lock",
        "price_cents": 197000,  # $1,970/yr ($164/mo effective)
        "billing": "annual",
        "trigger": "See ANNUAL_PUSH_TRIGGERS",
        "sold_by": "cora",
        "stripe_price_env": "stripe_price_annual_lock",
    },
    {
        "step": 10,
        "name": "data_only",
        "label": "Data-Only Save",
        "price_cents": 9700,  # $97/mo
        "billing": "monthly",
        "trigger": "At-risk (5–7 days inactive) — proactive save offer",
        "sold_by": "cora",
        "stripe_price_env": "stripe_price_data_only",
    },
    {
        "step": 11,
        "name": "partner",
        "label": "Partner",
        "price_cents": 200000,  # $2,000/mo
        "billing": "monthly",
        "trigger": "Multi-ZIP power users — self-serve page",
        "sold_by": "cora",
        "stripe_price_env": "stripe_price_partner",
    },
    {
        "step": 12,
        "name": "white_label",
        "label": "White-Label",
        "price_cents_range": (250000, 500000),  # $2,500–$5,000/mo
        "billing": "monthly",
        "trigger": "Proptech platforms — Stage 12+",
        "sold_by": "sales",
        "stripe_price_env": "stripe_price_white_label",
    },
]


# ── Wallet Tiers ─────────────────────────────────────────────────────────────

WALLET_TIERS = {
    "starter_wallet": {
        "label": "Starter Wallet",
        "price_cents": 4900,          # $49/mo
        "credits_per_cycle": 20,
        "auto_mode_included": False,   # $79–$99 add-on
        "stripe_price_env": "stripe_price_wallet_starter",
    },
    "growth": {
        "label": "Growth Wallet",
        "price_cents": 9900,           # $99/mo
        "credits_per_cycle": 50,
        "auto_mode_included": True,
        "stripe_price_env": "stripe_price_wallet_growth",
    },
    "power": {
        "label": "Power Wallet",
        "price_cents": 19900,          # $199/mo
        "credits_per_cycle": 120,
        "auto_mode_included": True,
        "stripe_price_env": "stripe_price_wallet_power",
    },
}

# Auto-reload fires when balance drops below this threshold
WALLET_AUTO_RELOAD_THRESHOLD = 5


# ── Wallet Enrollment Triggers ───────────────────────────────────────────────
# User is auto-enrolled into wallet when ANY of these conditions is met.
# Saved-card users skip the trigger threshold (pre-qualified).

WALLET_ENROLLMENT_TRIGGERS = [
    {"name": "two_unlocks_24h",     "description": "2 paid unlocks in 24 hours"},
    {"name": "three_total_unlocks", "description": "3 total paid unlocks"},
    {"name": "eight_dollar_day",    "description": "$8+ spend in a single day"},
    {"name": "repeat_zip_48h",      "description": "Repeat ZIP activity within 48 hours"},
]


# ── Bundles ──────────────────────────────────────────────────────────────────

BUNDLES = {
    "weekend": {
        "label": "Weekend Pack",
        "price_cents": 1900,          # $19
        "description": "5 bonus leads, available Friday–Sunday only",
        "leads": 5,
        "availability": "fri_sun",
        "stripe_price_env": "stripe_price_bundle_weekend",
    },
    "storm": {
        "label": "Storm Pack",
        "price_cents": 3900,          # $39
        "description": "10 storm-affected property leads in your ZIP",
        "leads": 10,
        "availability": "nws_trigger",  # available during active weather + 72h post
        "stripe_price_env": "stripe_price_bundle_storm",
    },
    "zip_booster": {
        "label": "ZIP Booster",
        "price_cents": 2900,          # $29
        "description": "10 additional leads in your existing ZIP for 48 hours",
        "leads": 10,
        "duration_hours": 48,
        "stripe_price_env": "stripe_price_bundle_zip_booster",
    },
    "monthly_reload": {
        "label": "Monthly Reload",
        "price_cents": 8900,          # $89
        "description": "Auto-recurring credit bundle (alternative to wallet)",
        "credits": 30,
        "billing": "monthly",
        "stripe_price_env": "stripe_price_bundle_monthly_reload",
    },
}


# ── Annual Plan ──────────────────────────────────────────────────────────────

ANNUAL_PLAN = {
    "price_cents": 197000,            # $1,970/yr
    "effective_monthly_cents": 16417,  # ~$164/mo
    "discount": "2 months free",
    "stripe_price_env": "stripe_price_annual_lock",
}

# Conditions under which Cora auto-pushes the annual offer.
# ANY of these being true triggers the push.
ANNUAL_PUSH_TRIGGERS = [
    {"name": "charter_day_7",        "description": "Day 7 for first 50 charter users"},
    {"name": "day_10_14",            "description": "Day 10–14 for all users"},
    {"name": "two_deals",            "description": "2 confirmed deals"},
    {"name": "spend_250",            "description": "$250 cumulative spend"},
    {"name": "deal_win_10k",         "description": "Deal-win reported at $10K+"},
    {"name": "auto_switch_day_60",   "description": "Automated annual offer at Day 60 mark"},
]


# ── Data-Only Save Tier ──────────────────────────────────────────────────────

DATA_ONLY_TIER = {
    "price_cents": 9700,              # $97/mo
    "features": ["lead_data_feed"],   # data only — no enrichment, no auto-mode, no VM
    "excluded": ["enrichment", "auto_mode", "voicemail", "skip_trace"],
    "trigger": "5–7 days inactive OR Day 5 of Stripe failed payment recovery",
    "stripe_price_env": "stripe_price_data_only",
}


# ── Free Tier Allotments ─────────────────────────────────────────────────────

FREE_ALLOTMENT = {
    "skips_per_week": 3,
    "texts_per_week": 3,
    "voicemails_per_week": 1,
    "cost_cap_per_user_cents": 650,   # $6.50 max cost per free user
}


# ── Credit Costs (per action) ────────────────────────────────────────────────

CREDIT_COSTS = {
    "lead_unlock":     1,
    "skip_trace":      2,
    "outbound_text":   1,
    "voicemail":       2,
    "report":          3,
    "brief":           5,
    "transfer":        26,
    "byol":            2,
}
