"""
Cora — kill-switch check.

Exact mirror of src/agents/hunter/kill_switch.py. Reuses the existing,
generic src.services.kill_switch_service rather than building Cora its own
stop mechanism. That service's manual-override path checks the Redis keys
`kill_switch_override:global` and `kill_switch_override:{feature}`
unconditionally, before ever looking at any metric-threshold config — so
Josh's manual "STOP ALL" / "STOP Cora" already works against this feature
key with zero changes to any existing config file:

    redis-cli SET kill_switch_override:global red EX 3600      # STOP ALL
    redis-cli SET kill_switch_override:cora_global red EX 3600 # STOP Cora

"cora_global" is deliberately NOT registered in any metric-threshold
guardrail config — Cora has no metric-threshold guardrail, only a manual
on/off, so get_kill_switch_status("cora_global") returns color="unknown"
(unregistered feature) with no override active. Unlike the old Lifecycle
runtime's graphs — where "unknown" fails safe to red because it usually
signals a misconfigured metric — for Cora "unknown" is the expected steady
state (no metric was ever supposed to exist), so this module treats only an
explicit "red" as a halt. Same reasoning Hunter's module documents for
itself.
"""
from __future__ import annotations

import logging

from src.services.kill_switch_service import get_kill_switch_status

logger = logging.getLogger(__name__)

CORA_GLOBAL_FEATURE = "cora_global"


def cora_halted() -> bool:
    """
    True if Cora's standing runs (worker loop, target producer, follow-up
    scheduler) should stop.

    Call at the start of every unit of work, and re-check for anything long
    enough that "halts within one cycle" wouldn't otherwise hold against a
    run already in progress.
    """
    status = get_kill_switch_status(CORA_GLOBAL_FEATURE)
    color = status.get("color")
    if color == "red":
        logger.warning(
            "Cora kill switch active (reason=%s) — halting.",
            status.get("reason"),
        )
        return True
    return False
