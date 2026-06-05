"""
Kill-switch service — shared helpers for reading kill-switch state.

Consolidates two things previously scattered:
  1. get_cached_metric() — moved here from src.tasks.kill_switch_metric_ingest
     so agent graphs don't import from the tasks layer.
  2. get_kill_switch_status() — thin wrapper over the agents gating tool for
     use by non-agents code (admin_router, etc.) without crossing the boundary.

The src.tasks.kill_switch_metric_ingest module re-exports get_cached_metric
from here so existing callers keep working without change.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_REDIS_PREFIX = "fa:ks_metric:"
_REDIS_TTL = 25 * 3600  # 25 hours — survives one missed cron run


def get_cached_metric(feature: str, county_id: Optional[str] = None) -> Optional[float]:
    """
    Return the last-cached kill-switch metric value for a feature, or None.

    Reads from Redis key: fa:ks_metric:{feature} or fa:ks_metric:{county_id}:{feature}
    Written by src.tasks.kill_switch_metric_ingest (every 6h).
    """
    from src.core.redis_client import redis_available, rget
    if not redis_available():
        return None
    key = f"{_REDIS_PREFIX}{county_id}:{feature}" if county_id else f"{_REDIS_PREFIX}{feature}"
    val = rget(key)
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def get_kill_switch_status(feature: str, observed_value: Optional[float] = None) -> Dict[str, Any]:
    """
    Return the kill-switch color band for a feature.

    Thin wrapper over src.agents.tools.gating_tools.kill_switch_status for
    use by non-agents code (admin_router, API endpoints) that needs to read
    kill-switch state without importing from the agents tool layer directly.

    Returns: {"color": "green"|"yellow"|"red"|"unknown", "feature": str, ...}
    """
    try:
        from src.agents.tools.gating_tools import kill_switch_status
        return kill_switch_status(feature, observed_value)
    except Exception as exc:
        logger.warning("get_kill_switch_status: failed for feature=%s: %s", feature, exc)
        return {"color": "unknown", "feature": feature, "reason": str(exc)}
