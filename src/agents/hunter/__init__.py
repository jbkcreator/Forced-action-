"""Hunter — enrichment and signal-mining identity.

Not a LangGraph agent (see docs/plans/agent_lane_phase1_week1_dev_split.md
for why): Hunter's capabilities (src/services/buyer_entity_resolution.py,
whale_detection.py, ...) are plain, mostly-deterministic modules triggered by
cron. This package is the thin governance layer every one of those modules
honors — confidence gating and the kill-switch check — not a runtime.
"""

__all__ = ["gating", "kill_switch"]
