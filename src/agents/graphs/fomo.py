"""
FOMO Engine graph.

Triggered when a competitor acts on a Gold-tier lead inside a non-locked ZIP.
Target latency: SMS out within 60 seconds of the competitor event.

Event envelope (example):
	{
		"event_type": "competitor_acted_on_lead",
		"payload": {
			"competitor_event_id": "uuid",
			"lead_id": 12345,
			"zip_code": "33647",
			"vertical": "roofing",
			"competitor_subscriber_id": 55,
			"lead_tier": "Gold",
		},
	}

Flow (6 nodes):
	1. assemble_context         — look up target subscriber + live ZIP data
	2. decision_hierarchy_check — delegates to the shared subgraph
	3. build_compose_context    — render prompt templates into system+user
	4. compose_and_send         — delegates to the shared subgraph
	5. finalize                 — record final summary into state for tests

Returns a LifecycleState-shaped dict ready for assertion / logging.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from config.scoring import for_county
from src.agents.context_utils import build_personalization_fields
from src.agents.prompts.loader import render_fallback_body, render_for_subscriber_auto
from src.agents.subgraphs.compose_and_send import run_compose_and_send
from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.write_tools import log_decision
from src.agents.tools.read_tools import (
	get_competition_status,
	get_segment_and_score,
	get_subscriber_profile,
	get_zip_activity,
)
from src.services.feedback_ritual import publish_feedback_ritual_candidate
from src.services.kill_switch_service import get_cached_metric


# Phrase used in place of the tier label when the subscriber's county is
# flagged tier_visibility="internal" (Pinellas pre-retune). Reads naturally
# in every template that currently interpolates {lead_tier}.
_TIER_SUPPRESSED_PHRASE = "matching"


GRAPH_NAME = "fomo"
CAMPAIGN = "fomo_competitor_action"
CLAUDE_TASK_TYPE = "sms_copy"
KILL_SWITCH_FEATURE = "lock_conversion"
AB_TEST_NAME: Optional[str] = None   # no A/B on FOMO v1


class FOMOState(TypedDict, total=False):
	# ── Inputs (from event envelope) ─────────────────────────────────────────
	decision_id: str
	subscriber_id: int         # the next-best-fit target (resolved pre-graph)
	event_type: str
	event_payload: dict

	# ── Assembled context ────────────────────────────────────────────────────
	subscriber_profile: dict
	zip_activity: dict
	competition_status: dict
	segment_data: dict          # fa038 — from get_segment_and_score

	# ── Hierarchy outputs ────────────────────────────────────────────────────
	action_allowed: bool
	action_blocked_reason: str
	use_fallback: bool
	kill_switch_color: str
	revenue_signal_score: int

	# ── Intermediate compose inputs (must be declared — LangGraph drops undeclared keys) ──
	_system_prompt: str
	_user_prompt: str
	_fallback_body: str
	_render_context: dict
	_variant_id: Optional[str]

	# ── Compose/send outputs ─────────────────────────────────────────────────
	message_body: str
	sent: bool
	send_reason: str
	message_outcome_id: Optional[int]
	tokens_used: int
	cost_usd: float
	terminal_status: str
	failure_reason: str


def _build_review_capture(state: FOMOState) -> dict:
	payload = state.get("event_payload") or {}
	return {
		"raw_input_text": (
			"competitor_acted_on_lead "
			f"zip_code={payload.get('zip_code') or ''} "
			f"vertical={payload.get('vertical') or ''} "
			f"lead_tier={payload.get('lead_tier') or ''}"
		).strip(),
		"generated_output_text": state.get("message_body") or "",
		"confidence_score": None,
		"confidence_reason": None,
		"review_flag": True,
		"review_flag_reason": state.get("failure_reason") or "fomo_early_abort",
	}


# ──────────────────────────────────────────────────────────────────────────────
# Nodes
# ──────────────────────────────────────────────────────────────────────────────

def _node_assemble_context(state: FOMOState) -> FOMOState:
	payload = state.get("event_payload") or {}
	subscriber_id = state["subscriber_id"]
	zip_code = payload.get("zip_code")
	vertical = payload.get("vertical")

	profile = get_subscriber_profile(subscriber_id)
	if not profile:
		return {
			"terminal_status": "aborted",
			"failure_reason": "fomo:subscriber_not_found",
		}

	zip_activity = get_zip_activity(zip_code, vertical=vertical) if zip_code else {}
	competition = get_competition_status(zip_code, vertical=vertical) if zip_code else {}
	segment_data = get_segment_and_score(subscriber_id)

	# Refuse early if the ZIP is already locked by someone else — FOMO only
	# fires for non-locked ZIPs.
	if competition.get("is_locked"):
		return {
			"subscriber_profile": profile,
			"zip_activity": zip_activity,
			"competition_status": competition,
			"segment_data": segment_data,
			"terminal_status": "aborted",
			"failure_reason": "fomo:zip_already_locked",
		}

	return {
		"subscriber_profile": profile,
		"zip_activity": zip_activity,
		"competition_status": competition,
		"segment_data": segment_data,
	}


def _node_hierarchy_check(state: FOMOState) -> FOMOState:
	if state.get("terminal_status"):
		return {}

	# fa034: FOMO drives lock conversion. Wire the kill_switch gate to the
	# live lock_conversion metric so a sustained drop fails decisions safely
	# (red → block, yellow → fallback template). Pattern mirrors retention.py.
	hierarchy = run_decision_hierarchy({
		"subscriber_id": state["subscriber_id"],
		"graph_name": GRAPH_NAME,
		"kill_switch_feature": KILL_SWITCH_FEATURE,
		"kill_switch_observed_value": get_cached_metric(KILL_SWITCH_FEATURE),
		"learning_card_type": "message_perf",
	})

	if not hierarchy.get("action_allowed", True):
		return {
			"action_allowed": False,
			"action_blocked_reason": hierarchy.get("action_blocked_reason", "unknown"),
			"kill_switch_color": hierarchy.get("kill_switch_color"),
			"revenue_signal_score": hierarchy.get("revenue_signal_score", 0),
			"terminal_status": "aborted",
			"failure_reason": hierarchy.get("action_blocked_reason", "hierarchy_blocked"),
		}

	return {
		"action_allowed": True,
		"use_fallback": bool(hierarchy.get("use_fallback", False)),
		"kill_switch_color": hierarchy.get("kill_switch_color"),
		"revenue_signal_score": hierarchy.get("revenue_signal_score", 0),
	}


def _node_build_compose_context(state: FOMOState) -> Dict[str, Any]:
	"""
	Render prompt templates using the context assembled earlier. Stored in
	state under _rendered_system / _rendered_user / _rendered_fallback so the
	compose-and-send subgraph can consume them directly.
	"""
	if state.get("terminal_status"):
		return {}

	profile = state.get("subscriber_profile") or {}
	zip_activity = state.get("zip_activity") or {}
	competition = state.get("competition_status") or {}
	segment_data = state.get("segment_data") or {}
	payload = state.get("event_payload") or {}

	# Counties whose tier distribution isn't yet trustworthy substitute a
	# neutral phrase for {lead_tier} in every prompt template.
	_county_cfg = for_county(profile.get("county_id"))
	_tier_for_prompt = (
		_TIER_SUPPRESSED_PHRASE
		if _county_cfg.tier_visibility == "internal"
		else (payload.get("lead_tier") or "Gold")
	)

	raw_score = state.get("revenue_signal_score", 0)
	personalization = build_personalization_fields(profile, segment_data, raw_score)

	context = {
		"subscriber_first_name": (profile.get("name") or "there").split(" ")[0],
		"first_name": (profile.get("name") or "there").split(" ")[0],
		"vertical": profile.get("vertical") or payload.get("vertical") or "",
		"zip_code": payload.get("zip_code") or "",
		"lead_tier": _tier_for_prompt,
		"active_lead_count": zip_activity.get("active_viewers", 0),
		"competing_viewers": competition.get("active_wallet_users_in_vertical", 0),
		"competitor_signal": (
			f"a competitor just contacted a {_tier_for_prompt} lead in {payload.get('zip_code', '')}"
		),
		"lead_specific_detail": (
			f"{zip_activity.get('active_viewers', 0)} more viewers active in {payload.get('zip_code', '')}"
		),
		"unlock_link": f"https://app.forcedaction.io/feed/{profile.get('id')}",
		"prompt_version": "fomo_v2",
		# fa038 personalization fields
		**personalization,
	}

	system, user, variant, test_name = render_for_subscriber_auto(
		GRAPH_NAME, state["subscriber_id"], context
	)
	fallback = render_fallback_body(GRAPH_NAME, context)

	return {
		"_system_prompt": system,
		"_user_prompt": user,
		"_fallback_body": fallback,
		"_render_context": context,
		"_variant_id": _format_variant_id(test_name, variant),
	}


def _format_variant_id(test_name: Optional[str], variant: Optional[str]) -> Optional[str]:
	"""Compose 'test_name:variant' attribution id, or None when no test is active."""
	if not test_name or not variant:
		return None
	return f"{test_name}:{variant}"


def _node_compose_and_send(state: FOMOState) -> FOMOState:
	if state.get("terminal_status"):
		return {}

	result = run_compose_and_send({
		"decision_id": state["decision_id"],
		"graph_name": GRAPH_NAME,
		"subscriber_id": state["subscriber_id"],
		"campaign": CAMPAIGN,
		"claude_task_type": CLAUDE_TASK_TYPE,
		"system_prompt": state.get("_system_prompt", ""),
		"user_prompt": state.get("_user_prompt", ""),
		"cache_system": True,                      # same system prompt across FOMO calls
		"max_output_tokens": 160,
		"variant_id": state.get("_variant_id"),
		"message_type": "marketing",
		"use_fallback": state.get("use_fallback", False),
		"ab_fallback_body": state.get("_fallback_body"),
		"personalization_context": state.get("_render_context"),
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


def _node_finalize(state: FOMOState) -> FOMOState:
	final_status = state.get("terminal_status") or "completed"

	# Early-abort audit: compose_and_send writes its own log on the happy path;
	# if the graph never reached compose_and_send (e.g. ZIP already locked) we
	# still owe an agent_decisions row here.
	if final_status != "completed" or not state.get("sent"):
		try:
			log_decision(
				decision_id=state["decision_id"],
				graph_name=GRAPH_NAME,
				subscriber_id=state.get("subscriber_id"),
				event_type=state.get("event_type"),
				terminal_status=final_status,
				tokens_used=int(state.get("tokens_used", 0) or 0),
				cost_usd=float(state.get("cost_usd", 0.0) or 0.0),
				summary={
					"failure_reason": state.get("failure_reason"),
					"early_abort": True,
					"review_capture": _build_review_capture(state),
				},
			)
			publish_feedback_ritual_candidate(
				decision_id=state["decision_id"],
				graph_name=GRAPH_NAME,
				terminal_status=final_status,
			)
		except Exception:
			pass

	if not state.get("terminal_status"):
		return {"terminal_status": "completed"}
	return {}


# ──────────────────────────────────────────────────────────────────────────────
# Graph assembly
# ──────────────────────────────────────────────────────────────────────────────

def build_fomo_graph() -> StateGraph:
	g = StateGraph(FOMOState)
	g.add_node("assemble_context", _node_assemble_context)
	g.add_node("hierarchy_check", _node_hierarchy_check)
	g.add_node("build_compose_context", _node_build_compose_context)
	g.add_node("compose_and_send", _node_compose_and_send)
	g.add_node("finalize", _node_finalize)

	g.add_edge(START, "assemble_context")
	g.add_edge("assemble_context", "hierarchy_check")
	g.add_edge("hierarchy_check", "build_compose_context")
	g.add_edge("build_compose_context", "compose_and_send")
	g.add_edge("compose_and_send", "finalize")
	g.add_edge("finalize", END)
	return g


def run_fomo(event_payload: Dict[str, Any], subscriber_id: int,
			 decision_id: Optional[str] = None) -> Dict[str, Any]:
	"""Compile + invoke the FOMO graph for a single event. Convenience wrapper."""
	graph = build_fomo_graph().compile()
	event_type = (event_payload or {}).get("event_type", "competitor_acted_on_lead")
	final = graph.invoke({
		"decision_id": decision_id or str(uuid.uuid4()),
		"subscriber_id": subscriber_id,
		"event_type": event_type,
		"event_payload": event_payload,
	})
	return dict(final)
