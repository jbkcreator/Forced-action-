"""
Event → Graph routing for the Cora supervisor.

Adding a new graph is one entry in EVENT_TO_GRAPH plus the graph module
itself. Registration is at import time; the supervisor reads this dict on
every event.

Each entry describes:
  - graph_name     — used for the agent_decisions audit row + kill-switch lookup
  - runner         — callable(event_payload, subscriber_id, decision_id=...) → dict
  - required_keys  — fields that must be present on event_payload.subscriber_id
					 (None means subscriber_id comes from event_payload directly)

The supervisor calls runner(event_payload=..., subscriber_id=..., decision_id=...)
and records the result to agent_decisions via the compose_and_send subgraph
(the graphs themselves already log, so the supervisor does not double-write).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional

from src.agents.graphs.abandonment import run_wave1 as _run_abandonment_wave1
from src.agents.graphs.abandonment import run_wave2 as _run_abandonment_wave2
from src.agents.graphs.ap_lite_close import run_ap_lite_close as _run_ap_lite_close
from src.agents.graphs.fomo import run_fomo as _run_fomo
from src.agents.graphs.human_close_route import run_human_close_route as _run_human_close_route
from src.agents.graphs.retention import run_retention as _run_retention_inner
from src.agents.graphs.synthflow_voice_drop import run_synthflow_voice_drop as _run_synthflow_voice_drop
from src.agents.graphs.wallet_to_lock_close import run_wallet_to_lock_close as _run_wallet_to_lock_close


def _run_retention_adapter(event_payload, subscriber_id, decision_id=None):
	"""Uniform (payload, subscriber_id, decision_id) signature over retention."""
	tier = (event_payload or {}).get("tier") or "wallet"
	return _run_retention_inner(
		subscriber_id=subscriber_id,
		tier_cohort=tier,
		decision_id=decision_id,
	)


@dataclass(frozen=True)
class GraphSpec:
	graph_name: str
	runner: Callable
	requires_decision_id: bool = False   # True for Wave 2 (shares with Wave 1)


EVENT_TO_GRAPH: Dict[str, GraphSpec] = {
	"competitor_acted_on_lead": GraphSpec(
		graph_name="fomo",
		runner=_run_fomo,
	),
	"wall_session_abandoned": GraphSpec(
		graph_name="abandonment_wave1",
		runner=_run_abandonment_wave1,
	),
	"abandonment_click_no_complete": GraphSpec(
		graph_name="abandonment_wave2",
		runner=_run_abandonment_wave2,
		requires_decision_id=True,
	),
	"retention_summary_due": GraphSpec(
		graph_name="retention",
		runner=_run_retention_adapter,
	),
	"subscriber_crossed_lock_threshold": GraphSpec(
		graph_name="wallet_to_lock_close",
		runner=_run_wallet_to_lock_close,
	),
	"subscriber_crossed_ap_lite_threshold": GraphSpec(
		graph_name="ap_lite_close",
		runner=_run_ap_lite_close,
	),
	"flash_scarcity_window_open": GraphSpec(
		graph_name="fomo",
		runner=_run_fomo,
	),
	"escalate_to_human_closer": GraphSpec(
		graph_name="human_close_route",
		runner=_run_human_close_route,
	),
	"high_intent_no_convert": GraphSpec(
		graph_name="synthflow_voice_drop",
		runner=_run_synthflow_voice_drop,
	),
}


def get_graph_spec(event_type: str) -> Optional[GraphSpec]:
	"""Return the GraphSpec for an event type, or None if unknown."""
	return EVENT_TO_GRAPH.get(event_type)
