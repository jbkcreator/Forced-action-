"""
Offer rule config — configurable placeholders pending client ratification.
All thresholds live here so rule logic in contracts.py never has magic numbers.
"""
from __future__ import annotations

OFFER_RULE_CONFIG: dict[str, object] = {
    "winback_discount_pct": 50,         # configurable placeholder
    "multi_purchase_threshold": 3,       # min purchases for core_sub upgrade
    "whale_confidence_threshold": 0.6,   # min confidence to fire whale rule
    "config_version": "v1.0.0",
}

# concierge_wedge eligibility signals (placeholder — pending client ratification)
CONCIERGE_ELIGIBLE_SIGNALS: list[str] = [
    "high_response_rate",
    "prior_demo_attended",
    "multi_county_interest",
]
