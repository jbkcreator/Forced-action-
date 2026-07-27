"""
Hunter — kill-switch check.

Reuses the existing, generic src.services.kill_switch_service rather than
building Hunter its own stop mechanism. That service's manual-override path
(src/agents/tools/gating_tools.py:kill_switch_status) checks the Redis keys
`kill_switch_override:global` and `kill_switch_override:{feature}`
UNCONDITIONALLY, before it ever looks at Cora's metric-threshold
config.cora_guardrails.KILL_SWITCH dict — so Josh's manual "STOP ALL" /
"STOP Hunter" already works against this feature key with zero changes to
that Cora-specific config file:

    redis-cli SET kill_switch_override:global red EX 3600        # STOP ALL
    redis-cli SET kill_switch_override:hunter_global red EX 3600 # STOP Hunter

"hunter_global" is deliberately NOT registered in
config.cora_guardrails.KILL_SWITCH — Hunter has no metric-threshold guardrail,
only a manual on/off, and that file's entries assume auto-triggered
metric bands (green/yellow/red numeric thresholds, auto_action_type,
fallback_feature_flag, ...) that don't apply here. The consequence: with no
override active, get_kill_switch_status("hunter_global") returns
color="unknown" (unregistered feature) rather than "green". Unlike Cora's
graphs — where "unknown" correctly fails safe to red because it usually
signals a misconfigured metric — for Hunter "unknown" is the EXPECTED
steady state (no metric was ever supposed to exist), so this module treats
only an explicit "red" as a halt.
"""
from __future__ import annotations

import logging

from src.services.kill_switch_service import get_kill_switch_status

logger = logging.getLogger(__name__)

HUNTER_GLOBAL_FEATURE = "hunter_global"


def hunter_halted() -> bool:
    """
    True if Hunter's standing runs should stop.

    Call at the start of every cron entry (nightly sweep, whale scoring,
    auction profiling, ...), and re-check mid-run for anything long enough
    that "halts within one cycle" wouldn't otherwise hold against a run
    already in progress.
    """
    status = get_kill_switch_status(HUNTER_GLOBAL_FEATURE)
    color = status.get("color")
    if color == "red":
        logger.warning(
            "Hunter kill switch active (reason=%s) — halting.",
            status.get("reason"),
        )
        return True
    return False
