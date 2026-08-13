"""Founder tier is exempt from the 6-month founding-rate escalation.

The founder card promises the founding rate is "locked for life". Founder
resolves a flat price straight from `plans` and never sets
`Subscriber.founding_member`, so the escalation query (which filters on
`founding_member == True`) never selects it. This test guards the second,
belt-and-braces invariant: even if a founder subscriber were somehow flagged
founding_member, no regular price is configured for the tier, so escalation
would skip it rather than move it off the founder rate. See ADR 0036.
"""

from src.tasks.price_escalation import _REGULAR_PRICE_ATTR, run_price_escalation


def test_founder_has_no_regular_escalation_price():
    # No founder key -> run_price_escalation cannot escalate a founder sub;
    # it logs "No regular price configured" and skips (never changes the price).
    assert "founder" not in _REGULAR_PRICE_ATTR


def test_price_escalation_disabled():
    # Escalation permanently disabled 2026-08-12 — must no-op regardless of args.
    result = run_price_escalation(dry_run=False)
    assert result["disabled"] is True
    assert result["escalated"] == 0

    result_dry = run_price_escalation(dry_run=True)
    assert result_dry["disabled"] is True
    assert result_dry["escalated"] == 0


def test_escalation_price_map_covers_only_time_limited_tiers():
    # Dominator tier retired (apply_retire_dominator_tier.py). Map is starter + pro only.
    # Founder must never appear here (rate locked for life, ADR 0036).
    assert set(_REGULAR_PRICE_ATTR) == {"starter", "pro"}
