"""
Shared Green/Yellow/Red grading for kill-switch metrics.

Extracted from lifecycle_self_healing._grade so the scorecard and the engine
use identical logic. The engine re-exports this function — no behavior change.
"""
from typing import Optional

from config.lifecycle_guardrails import KILL_SWITCH


def grade(metric_name: str, observed: Optional[float]) -> str:
    """Return 'green' | 'yellow' | 'red' | 'unknown'.

    Respects the ``direction`` field in KILL_SWITCH thresholds:
    - higher_is_better (default): below red threshold = red, below green = yellow
    - lower_is_better: above red threshold = red, above green = yellow
    Unknown metric name or None observed value → 'unknown'.
    """
    cfg = KILL_SWITCH.get(metric_name)
    if cfg is None or observed is None:
        return "unknown"

    green = cfg["green"]
    red = cfg["red"]
    direction = cfg.get("direction", "higher_is_better")

    if direction == "higher_is_better":
        if observed >= green:
            return "green"
        if observed < red:
            return "red"
        return "yellow"

    # lower_is_better
    if observed <= green:
        return "green"
    if observed > red:
        return "red"
    return "yellow"
