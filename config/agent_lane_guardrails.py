"""
Agent Lane Guardrail Ranges

Concrete bounds for Agent Lane's own autonomous experiment decisions
(Cora cold outbound, REVINT price-band tests, Hunter's vertical autopilot,
LEARN's adaptive allocation). Deliberately separate from
config/lifecycle_guardrails.py — Agent Lane (pre-customer) and Lifecycle
(post-customer) are different engines with different guardrail owners;
reusing Lifecycle's GUARDRAILS dict here would recreate the same coupling
this file exists to remove. See docs/agent-lane-data-access-matrix.md.

Usage:
    from config.agent_lane_guardrails import GUARDRAILS, get_guardrail
"""


GUARDRAILS = {
    "agent_lane_experiment_traffic_cap": {
        "label": "Agent Lane experiment traffic cap",
        "max_pct": 10,                    # % of segment/audience
        "rollback_trigger": "Auto-rollback if losing arm >2 std devs",
    },
    "agent_lane_challenger_floor": {
        "label": "Agent Lane challenger protected floor",
        "min_pct": 30,                    # Cora's Challenger Board 30/20/10 rule (spec Part 1.5/§9.4)
        "rollback_trigger": "Never starve a challenger below this floor",
        "hard_limit": True,
    },
}


def get_guardrail(name: str) -> dict:
    """Look up a guardrail by name. Raises KeyError if not found."""
    return GUARDRAILS[name]


def is_within_guardrail(name: str, value: float) -> bool:
    """Check if a value is within the allowed range for a guardrail."""
    g = GUARDRAILS[name]
    if "min_pct" in g and "max_pct" in g:
        return g["min_pct"] <= value <= g["max_pct"]
    if "max_pct" in g:
        return value <= g["max_pct"]
    if "min_pct" in g:
        return value >= g["min_pct"]
    return True
