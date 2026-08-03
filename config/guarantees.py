"""
Tiered volume guarantee.

Each paid tier promises a minimum number of delivered leads per ~30-day
cycle. src/tasks/guarantee_shortfall_sweep.py compares actual deliveries
(src/core/models.py Delivery, via the CustomerAccount bridge) against the
quota below; any shortfall is credited to the subscriber's Stripe balance,
prorated against their actual plan_price.
"""

TIER_LEAD_QUOTAS = {
    "starter":  15,
    "pro":      40,
    "founder":  40,
}

from datetime import datetime, timezone

# When the deliveries ledger (scripts/apply_fa085_m10_lead_delivery.py) went
# live. No lead delivery could be recorded before this date, so a guarantee
# cycle that starts earlier would count unavoidable zero-delivery days as a
# false shortfall — even for a customer_account created before it. The sweep
# floors every cycle at max(account_created_at, DELIVERY_TRACKING_START_UTC).
DELIVERY_TRACKING_START_UTC = datetime(2026, 6, 23, tzinfo=timezone.utc)
