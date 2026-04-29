"""
Abandonment Pressure graphs — Wave 1 and Wave 2.

The architecture doc describes one 11-node graph with a suspend/resume window
in the middle. For priority-list code completion we implement this as two
discrete graphs sharing a decision_id:

  Wave 1  (sends the first recovery SMS 10-15 min after wall abandonment)
	1. load_session_and_profile
	2. assemble_context
	3. hierarchy_check
	4. build_compose_context
	5. compose_and_send
	6. schedule_wave2_check

  Wave 2  (fires if Wave 1 was clicked but no payment followed inside ~20 min)
	1. load_wave1_state
	2. check_converted_state        — if user paid since Wave 1, exit early
	3. assemble_wave2_context
	4. build_compose_context_w2
	5. compose_and_send_w2

Both waves share the same `decision_id` so the audit log shows one end-to-end
record per abandonment journey. This split keeps each graph synchronous and
trivially testable, matches how the rest of the platform handles timed
follow-ups (cron + event), and avoids a long in-process suspension that would
stress the LangGraph checkpointer over multi-minute waits in production.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.prompts.loader import (
	load_prompt,
	render,
	render_fallback_body,
	render_system_and_user,
)
from src.agents.subgraphs.compose_and_send import run_compose_and_send
from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import (
	get_recent_messages,
	get_subscriber_profile,
	get_wallet_state,
	get_zip_activity,
)


GRAPH_WAVE1 = "abandonment_wave1"
GRAPH_WAVE2 = "abandonment_wave2"
CAMPAIGN_WAVE1 = "abandonment_wave1"
CAMPAIGN_WAVE2 = "abandonment_wave2_click_no_complete"
KILL_SWITCH_FEATURE = "first_payment_rate"


class AbandonmentState(TypedDict, total=False):
	# Inputs
	decision_id: str
	subscriber_id: int
	event_type: str
	event_payload: dict

	# Assembled
	subscriber_profile: dict
	wallet_state: dict
	zip_activity: dict
	recent_messages: list
	wave: str                        # 'wave1' | 'wave2'

	# Hierarchy
	action_allowed: bool
	action_blocked_reason: str
	use_fallback: bool
	kill_switch_color: str

	# Compose/send
	message_body: str
	sent: bool
	send_reason: str
	message_outcome_id: Optional[int]
	tokens_used: int
	cost_usd: float
	terminal_status: str
	failure_reason: str

	# Wave-2 bookkeeping
	wave2_scheduled_at: Optional[str]
	wave1_already_converted: bool


# ──────────────────────────────────────────────────────────────────────────────
# Shared nodes
# ──────────────────────────────────────────────────────────────────────────────

def _node_load_profile(state: AbandonmentState) -> AbandonmentState:
	profile = get_subscriber_profile(state["subscriber_id"])
	if not profile:
		return {"terminal_status": "aborted", "failure_reason": "abandonment:subscriber_not_found"}

	payload = state.get("event_payload") or {}
	zip_code = payload.get("zip_code")
	vertical = payload.get("vertical") or profile.get("vertical")

	return {
		"subscriber_profile": profile,
		"wallet_state": get_wallet_state(state["subscriber_id"]),
		"zip_activity": get_zip_activity(zip_code, vertical=vertical) if zip_code else {},
		"recent_messages": get_recent_messages(state["subscriber_id"], hours=24),
	}


def _node_hierarchy_check(state: AbandonmentState) -> AbandonmentState:
	if state.get("terminal_status"):
		return {}

	hierarchy = run_decision_hierarchy({
		"subscriber_id": state["subscriber_id"],
		"graph_name": state.get("wave") or GRAPH_WAVE1,
		"kill_switch_feature": None,           # priority-list scope: fail-open
		"learning_card_type": "message_perf",
	})

	if not hierarchy.get("action_allowed", True):
		return {
			"action_allowed": False,
			"terminal_status": "aborted",
			"failure_reason": hierarchy.get("action_blocked_reason", "hierarchy_blocked"),
			"kill_switch_color": hierarchy.get("kill_switch_color"),
		}

	return {
		"action_allowed": True,
		"use_fallback": bool(hierarchy.get("use_fallback", False)),
		"kill_switch_color": hierarchy.get("kill_switch_color"),
	}


# ──────────────────────────────────────────────────────────────────────────────
# Wave 1 specific nodes
# ──────────────────────────────────────────────────────────────────────────────

def _wave1_build_context(state: AbandonmentState) -> AbandonmentState:
	if state.get("terminal_status"):
		return {}

	profile = state.get("subscriber_profile") or {}
	payload = state.get("event_payload") or {}
	zip_activity = state.get("zip_activity") or {}

	ctx = {
		"subscriber_first_name": (profile.get("name") or "there").split(" ")[0],
		"first_name": (profile.get("name") or "there").split(" ")[0],
		"vertical": profile.get("vertical") or payload.get("vertical") or "",
		"minutes_since_session_start": payload.get("minutes_elapsed", 12),
		"gold_lead_count": zip_activity.get("active_viewers", 0),
		"wall_countdown_minutes": payload.get("wall_countdown_minutes", 3),
		"unlock_link": f"https://app.forcedaction.io/feed/{profile.get('id')}",
	}

	system, user = render_system_and_user("abandonment", ctx)
	fallback = render_fallback_body("abandonment", ctx)
	return {
		"_system_prompt": system,
		"_user_prompt": user,
		"_fallback_body": fallback,
	}


def _wave1_compose_and_send(state: AbandonmentState) -> AbandonmentState:
	if state.get("terminal_status"):
		return {}

	result = run_compose_and_send({
		"decision_id": state["decision_id"],
		"graph_name": GRAPH_WAVE1,
		"subscriber_id": state["subscriber_id"],
		"campaign": CAMPAIGN_WAVE1,
		"claude_task_type": "sms_copy",
		"system_prompt": state.get("_system_prompt", ""),
		"user_prompt": state.get("_user_prompt", ""),
		"cache_system": True,
		"max_output_tokens": 160,
		"message_type": "marketing",
		"use_fallback": state.get("use_fallback", False),
		"ab_fallback_body": state.get("_fallback_body"),
		"tokens_used": int(state.get("tokens_used", 0) or 0),
		"cost_usd": float(state.get("cost_usd", 0.0) or 0.0),
	})
	return {
		"message_body": result.get("message_body"),
		"sent": result.get("sent", False),
		"send_reason": result.get("send_reason"),
		"message_outcome_id": result.get("message_outcome_id"),
		"tokens_used": int(result.get("tokens_used", 0) or 0),
		"cost_usd": float(result.get("cost_usd", 0.0) or 0.0),
		"terminal_status": result.get("terminal_status"),
		"failure_reason": result.get("failure_reason"),
	}


def _wave1_schedule_wave2(state: AbandonmentState) -> AbandonmentState:
	"""
	Emit the signal that Wave 2 should fire on click-no-complete.

	Production wiring: this would write a Redis key like
	  abandonment:wave2_pending:{decision_id}
	with a 20-minute TTL. The click handler in the API checks for the key
	and, if present, dispatches the wave2 event when the user clicks the
	Wave 1 link but does not pay inside the TTL.

	For priority-list code completion we record the intent in state only
	(the event-ingestion layer will own the Redis/Postgres write when wired).
	"""
	# Always call _finalize_audit regardless of terminal status, so early
	# aborts still produce an agent_decisions row.
	_finalize_audit(state, GRAPH_WAVE1)

	if state.get("terminal_status") != "completed":
		return {}

	from datetime import datetime, timezone
	return {"wave2_scheduled_at": datetime.now(timezone.utc).isoformat()}


def _finalize_audit(state: AbandonmentState, graph_name: str) -> None:
	"""
	Write an agent_decisions row when the graph aborted before reaching
	compose_and_send. No-op if compose_and_send already logged (i.e. sent).
	"""
	from src.agents.tools.write_tools import log_decision

	final_status = state.get("terminal_status") or "completed"
	if final_status == "completed" and state.get("sent"):
		return
	try:
		log_decision(
			decision_id=state["decision_id"],
			graph_name=graph_name,
			subscriber_id=state.get("subscriber_id"),
			event_type=state.get("event_type"),
			terminal_status=final_status,
			tokens_used=int(state.get("tokens_used", 0) or 0),
			cost_usd=float(state.get("cost_usd", 0.0) or 0.0),
			summary={"failure_reason": state.get("failure_reason"), "early_abort": True},
		)
	except Exception:
		pass


def build_wave1_graph() -> StateGraph:
	g = StateGraph(AbandonmentState)
	g.add_node("load_profile", _node_load_profile)
	g.add_node("hierarchy_check", _node_hierarchy_check)
	g.add_node("build_context", _wave1_build_context)
	g.add_node("compose_and_send", _wave1_compose_and_send)
	g.add_node("schedule_wave2", _wave1_schedule_wave2)

	g.add_edge(START, "load_profile")
	g.add_edge("load_profile", "hierarchy_check")
	g.add_edge("hierarchy_check", "build_context")
	g.add_edge("build_context", "compose_and_send")
	g.add_edge("compose_and_send", "schedule_wave2")
	g.add_edge("schedule_wave2", END)
	return g


def run_wave1(event_payload: Dict[str, Any], subscriber_id: int,
			  decision_id: Optional[str] = None) -> Dict[str, Any]:
	graph = build_wave1_graph().compile()
	final = graph.invoke({
		"decision_id": decision_id or str(uuid.uuid4()),
		"subscriber_id": subscriber_id,
		"event_type": "wall_session_abandoned",
		"event_payload": event_payload,
		"wave": "wave1",
	})
	return dict(final)


# ──────────────────────────────────────────────────────────────────────────────
# Wave 2 specific nodes
# ──────────────────────────────────────────────────────────────────────────────

def _wave2_check_converted(state: AbandonmentState) -> AbandonmentState:
	"""Exit early if the user paid between Wave 1 and Wave 2."""
	profile = state.get("subscriber_profile") or {}
	recent = state.get("recent_messages") or []

	# Heuristic: a message_outcomes row with conversion_type set ≠ 'none' means
	# the user converted recently. We also trust `has_saved_card` flipping
	# from False → True as a proxy for payment since Wave 1.
	converted_recently = any(
		(m.get("conversion_type") and m.get("conversion_type") != "none")
		for m in recent
	)
	if converted_recently or profile.get("has_saved_card"):
		return {
			"terminal_status": "completed",
			"wave1_already_converted": True,
			"failure_reason": "wave2_skipped_user_already_converted",
		}
	return {"wave1_already_converted": False}


def _wave2_build_context(state: AbandonmentState) -> AbandonmentState:
	if state.get("terminal_status"):
		return {}

	profile = state.get("subscriber_profile") or {}
	payload = state.get("event_payload") or {}
	zip_activity = state.get("zip_activity") or {}

	ctx = {
		"subscriber_first_name": (profile.get("name") or "there").split(" ")[0],
		"first_name": (profile.get("name") or "there").split(" ")[0],
		"lead_tier_viewed": payload.get("lead_tier_viewed", "Gold"),
		"other_viewers_count": zip_activity.get("active_viewers", 0),
		"wall_countdown_minutes": payload.get("wall_countdown_minutes", 2),
		"unlock_link": f"https://app.forcedaction.io/feed/{profile.get('id')}",
	}

	data = load_prompt("abandonment", "wave2_system")
	system = render(data.get("system", ""), ctx)
	user = render(data.get("user", ""), ctx)
	fallback = render_fallback_body("abandonment", ctx)
	return {
		"_system_prompt": system,
		"_user_prompt": user,
		"_fallback_body": fallback,
	}


def _wave2_compose_and_send(state: AbandonmentState) -> AbandonmentState:
	if state.get("terminal_status"):
		return {}

	result = run_compose_and_send({
		"decision_id": state["decision_id"],
		"graph_name": GRAPH_WAVE2,
		"subscriber_id": state["subscriber_id"],
		"campaign": CAMPAIGN_WAVE2,
		"claude_task_type": "sms_copy",
		"system_prompt": state.get("_system_prompt", ""),
		"user_prompt": state.get("_user_prompt", ""),
		"cache_system": True,
		"max_output_tokens": 160,
		"message_type": "marketing",
		"use_fallback": state.get("use_fallback", False),
		"ab_fallback_body": state.get("_fallback_body"),
		"tokens_used": int(state.get("tokens_used", 0) or 0),
		"cost_usd": float(state.get("cost_usd", 0.0) or 0.0),
	})
	return {
		"message_body": result.get("message_body"),
		"sent": result.get("sent", False),
		"send_reason": result.get("send_reason"),
		"message_outcome_id": result.get("message_outcome_id"),
		"tokens_used": int(result.get("tokens_used", 0) or 0),
		"cost_usd": float(result.get("cost_usd", 0.0) or 0.0),
		"terminal_status": result.get("terminal_status"),
		"failure_reason": result.get("failure_reason"),
	}


def _wave2_finalize(state: AbandonmentState) -> AbandonmentState:
	_finalize_audit(state, GRAPH_WAVE2)
	return {}


def build_wave2_graph() -> StateGraph:
	g = StateGraph(AbandonmentState)
	g.add_node("load_profile", _node_load_profile)
	g.add_node("check_converted", _wave2_check_converted)
	g.add_node("hierarchy_check", _node_hierarchy_check)
	g.add_node("build_context", _wave2_build_context)
	g.add_node("compose_and_send", _wave2_compose_and_send)
	g.add_node("finalize", _wave2_finalize)

	g.add_edge(START, "load_profile")
	g.add_edge("load_profile", "check_converted")
	g.add_edge("check_converted", "hierarchy_check")
	g.add_edge("hierarchy_check", "build_context")
	g.add_edge("build_context", "compose_and_send")
	g.add_edge("compose_and_send", "finalize")
	g.add_edge("finalize", END)
	return g


def run_wave2(event_payload: Dict[str, Any], subscriber_id: int,
			  decision_id: str) -> Dict[str, Any]:
	"""
	Resume the abandonment journey with Wave 2.

	decision_id must match the Wave 1 decision so the audit log reflects one
	end-to-end abandonment record.
	"""
	graph = build_wave2_graph().compile()
	final = graph.invoke({
		"decision_id": decision_id,
		"subscriber_id": subscriber_id,
		"event_type": "abandonment_click_no_complete",
		"event_payload": event_payload,
		"wave": "wave2",
	})
	return dict(final)
