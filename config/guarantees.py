"""
Tiered volume guarantee.

Each paid tier promises a minimum number of delivered leads per ~30-day
cycle. src/tasks/guarantee_shortfall_sweep.py compares actual deliveries
(src/core/models.py Delivery, via the CustomerAccount bridge) against the
quota below; any shortfall is credited to the subscriber's Stripe balance,
prorated against their actual plan_price.

TIER_LEAD_QUOTAS values are a placeholder pending a business decision —
confirm before enabling the sweep in production.
"""

TIER_LEAD_QUOTAS = {
    "starter":   10,
    "pro":       20,
    "dominator": 40,
}
