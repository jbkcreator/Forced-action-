"""Founder tier is exempt from the 6-month founding-rate escalation.

The founder card promises the founding rate is "locked for life". Founder
resolves a flat price straight from `plans` and never sets
`Subscriber.founding_member`, so the escalation query (which filters on
`founding_member == True`) never selects it. This test guards the second,
belt-and-braces invariant: even if a founder subscriber were somehow flagged
founding_member, no regular price is configured for the tier, so escalation
would skip it rather than move it off the founder rate. See ADR 0036.
"""

from src.tasks.price_escalation import _REGULAR_PRICE_ATTR


def test_founder_has_no_regular_escalation_price():
    # No founder key -> run_price_escalation cannot escalate a founder sub;
    # it logs "No regular price configured" and skips (never changes the price).
    assert "founder" not in _REGULAR_PRICE_ATTR


def test_escalation_price_map_covers_only_time_limited_tiers():
    # The escalation map is exactly the tiers whose founding rate expires.
    # Founder must never appear here.
    assert set(_REGULAR_PRICE_ATTR) == {"starter", "pro", "dominator"}
