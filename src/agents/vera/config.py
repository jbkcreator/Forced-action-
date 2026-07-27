"""
Vera constants — freshness classes, kill-switch key, fact-record enums.

Kept separate from config/settings.py (env-backed connection strings live
there) because these are fixed vocabulary, not deployment configuration.
"""
from __future__ import annotations

# Kill-switch feature key Vera checks before every standing job. Mirrors the
# existing src.services.kill_switch_service.get_kill_switch_status(feature)
# rather than a bespoke mechanism (Agent Lane v2.2 dev-split §6b). Hunter and
# Relay use "hunter_global" / "relay_global" the same way.
KILL_SWITCH_FEATURE = "vera_global"

# freshness_class values written to vera_facts.freshness_class. A fact whose
# observed_at is older than its class's max age reads as expired ("unknown
# because stale") rather than being deleted — see src/agents/vera/facts.py.
FRESHNESS_REVENUE_24H = "revenue_24h"
FRESHNESS_DEED_90D = "deed_90d"
FRESHNESS_MARKET_30D = "market_30d"
FRESHNESS_STATIC = "static"  # never expires (e.g. a one-time config fact)

FRESHNESS_MAX_AGE_HOURS = {
    FRESHNESS_REVENUE_24H: 24,
    FRESHNESS_DEED_90D: 90 * 24,
    FRESHNESS_MARKET_30D: 30 * 24,
    FRESHNESS_STATIC: None,  # None = never expires
}
