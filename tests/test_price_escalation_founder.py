"""Price escalation is permanently disabled (2026-08-12) — founder rate is
locked for life regardless. See ADR 0036.

_REGULAR_PRICE_ATTR (the per-tier escalation price map this file used to
assert against) was removed when price_escalation.py was tombstoned; the
module now always no-ops, so the escalation-price-map tests it backed no
longer apply and were dropped rather than resurrecting a dead constant.
"""

from src.tasks.price_escalation import run_price_escalation


def test_price_escalation_disabled():
    # Escalation permanently disabled 2026-08-12 — must no-op regardless of args.
    result = run_price_escalation(dry_run=False)
    assert result["disabled"] is True
    assert result["escalated"] == 0

    result_dry = run_price_escalation(dry_run=True)
    assert result_dry["disabled"] is True
    assert result_dry["escalated"] == 0
