"""
Supplier Intelligence Foundation — Phase 1 configuration.

Pricing tiers, data-readiness thresholds, and report section registry.
This implementation is explicitly Phase 1 (foundation/shell). Advanced
analytics sections (deal benchmarks, recommendations) return N/A until
sufficient deal outcome volume exists — see DATA_READINESS_THRESHOLDS.
"""

# ── Pricing tiers ──────────────────────────────────────────────────────────────

SUPPLIER_INTEL_TIERS: dict[str, dict] = {
    "foundation": {
        "display_name": "Foundation",
        "price_cents": 49700,   # $497/mo
        "stripe_key": "supplier_intel_foundation",
        "description": "Lead volume, signal counts, ZIP activity, trade coverage.",
    },
    "standard": {
        "display_name": "Standard",
        "price_cents": 99700,   # $997/mo
        "stripe_key": "supplier_intel_standard",
        "description": "Foundation + signal trend analysis and benchmarks when data available.",
    },
    "premium": {
        "display_name": "Premium",
        "price_cents": 149700,  # $1,497/mo
        "stripe_key": "supplier_intel_premium",
        "description": "Standard + full analytics and API access when available.",
    },
}

TRIAL_DAYS = 14
VALID_TIERS = frozenset(SUPPLIER_INTEL_TIERS.keys())

# ── Data-readiness thresholds ──────────────────────────────────────────────────
# Sections that require these minimums return {"status": "insufficient_data"}
# instead of synthetic or unreliable numbers. These are intentionally conservative.

DATA_READINESS_THRESHOLDS: dict[str, int] = {
    "min_deals_for_benchmarks": 50,     # closed_won deal_outcomes in territory
    "min_subs_for_trend":       5,      # active subscribers in county/vertical
    "min_days_of_data":         30,     # minimum data window before trend shown
    "min_leads_for_zip_map":    10,     # leads in a ZIP before it appears in map
}

# ── Report sections ──────────────────────────────────────────────────────────────
# "safe" = always generated from available data
# "gated" = returns N/A until threshold met
# "phase2" = always N/A in Phase 1; deferred to Phase 2

REPORT_SECTIONS: list[dict] = [
    {
        "key": "market_activity",
        "label": "Market Activity",
        "availability": "safe",
        "description": "Total leads, signal breakdown, period-over-period change.",
    },
    {
        "key": "top_zips",
        "label": "Top ZIPs",
        "availability": "safe",
        "description": "Top 10 ZIPs by lead count with tier distribution.",
    },
    {
        "key": "signal_movement",
        "label": "Signal Movement",
        "availability": "safe",
        "description": "30/60/90-day trend for each distress signal type.",
    },
    {
        "key": "property_tier_dist",
        "label": "Property Tier Distribution",
        "availability": "safe",
        "description": "Platinum/Gold/Silver/Bronze lead counts and percentages.",
    },
    {
        "key": "trade_coverage",
        "label": "Trade Coverage",
        "availability": "safe",
        "description": "Active verticals in covered territory with lead counts.",
    },
    {
        "key": "closed_deal_benchmarks",
        "label": "Closed-Deal Benchmarks",
        "availability": "gated",
        "threshold_key": "min_deals_for_benchmarks",
        "description": "Avg deal size, days-to-close, close rate. Requires deal outcome volume.",
    },
    {
        "key": "contractor_demand",
        "label": "Contractor Demand Trend",
        "availability": "gated",
        "threshold_key": "min_subs_for_trend",
        "description": "Subscriber lead consumption and activity trend. Requires sufficient subscribers.",
    },
    {
        "key": "recommendations",
        "label": "Recommendations",
        "availability": "phase2",
        "description": "AI-driven supplier opportunity recommendations. Phase 2 — not yet available.",
    },
]

SAFE_SECTIONS = [s["key"] for s in REPORT_SECTIONS if s["availability"] == "safe"]
GATED_SECTIONS = [s["key"] for s in REPORT_SECTIONS if s["availability"] == "gated"]
PHASE2_SECTIONS = [s["key"] for s in REPORT_SECTIONS if s["availability"] == "phase2"]

# ── Export ─────────────────────────────────────────────────────────────────────

REPORT_OUTPUT_DIR = "reports/supplier_intel"
REPORT_RETENTION_DAYS = 90

# Statuses eligible to receive reports
ALERT_ELIGIBLE_STATUSES = ("trialing", "active")
