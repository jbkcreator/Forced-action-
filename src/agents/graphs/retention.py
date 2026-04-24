"""
Retention Summaries graph.

Per-tier scheduled summary runs — Sonnet-composed "what you got, what you
would have missed" messages sent to active subscribers on a tier-specific
cadence. Batched fan-out: one cron trigger → one graph invocation per
matching subscriber.

Trigger shape:
	event_type: 'retention_summary_due'
	event_payload: {'tier': 'wallet' | 'lock' | 'autopilot', 'window_days': 7}

A scheduler produces one such event per subscriber in the target cohort.
The graph itself is single-subscriber; the fan-out is a concern of the
event producer (cron + query).

Flow (7 nodes):
	1. load_profile              — basic identity, gate out inactive users
	2. assemble_history          — wallet + deals + top ZIP
	3. assemble_opportunity_gap  — unclaimed Gold leads + competing viewers
	4. hierarchy_check           — shared subgraph
	5. build_context             — render prompt
	6. compose_and_send          — shared subgraph (Sonnet-tier)
	7. finalize
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Iterable, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.prompts.loader import render_fallback_body, render_system_and_user
from src.agents.subgraphs.compose_and_send import run_compose_and_send
from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import (
	get_deal_history,
	get_lead_pool,
	get_subscriber_profile,
	get_wallet_state,
	get_zip_activity,
)


GRAPH_NAME = "retention"
CAMPAIGN_PREFIX = "retention_summary"
KILL_SWITCH_FEATURE = "retention_30d"


class RetentionState(TypedDict, total=False):
	decision_id: str
	subscriber_id: int
	event_type: str
	event_payload: dict

	# Tier cohort (wallet | lock | autopilot)
	tier_cohort: str

	# Assembled
	subscriber_profile: dict
	wallet_state: dict
	deal_history: list
	top_zip: Optional[str]
	unclaimed_gold_count: int
	competing_viewers: int

	# Hierarchy
	action_allowed: bool
	action_blocked_reason: str
	use_fallback: bool

	# Compose
	message_body: str
	sent: bool
	tokens_used: int
	cost_usd: float
	terminal_status: str
	failure_reason: str


# ──────────────────────────────────────────────────────────────────────────────
# Nodes
# ──────────────────────────────────────────────────────────────────────────────

def _node_load_profile(state: RetentionState) -> RetentionState:
	profile = get_subscriber_profile(state["subscriber_id"])
	if not profile:
		return {"terminal_status": "aborted", "failure_reason": "retention:subscriber_not_found"}
	if profile.get("status") not in (None, "active"):
		# Retention only fires against active subscribers.
		return {
			"subscriber_profile": profile,
			"terminal_status": "aborted",
			"failure_reason": f"retention:status_{profile.get('status')}",
		}
	return {"subscriber_profile": profile}


def _node_assemble_history(state: RetentionState) -> RetentionState:
	if state.get("terminal_status"):
		return {}

	sub_id = state["subscriber_id"]
	wallet = get_wallet_state(sub_id)
	deals = get_deal_history(sub_id, limit=10)

	# Find a "top ZIP" from deal history if available. Fall back to the
	# subscriber's county in the profile.
	top_zip: Optional[str] = None
	for d in deals:
		lead_source = d.get("lead_source")
		if lead_source and lead_source.startswith("zip:"):
			top_zip = lead_source.split(":", 1)[1]
			break

	return {
		"wallet_state": wallet,
		"deal_history": deals,
		"top_zip": top_zip,
	}


def _node_assemble_opportunity_gap(state: RetentionState) -> RetentionState:
	if state.get("terminal_status"):
		return {}

	profile = state.get("subscriber_profile") or {}
	top_zip = state.get("top_zip")

	unclaimed = 0
	competing = 0
	if top_zip:
		pool = get_lead_pool(top_zip, vertical=profile.get("vertical"), min_score=60, limit=50)
		unclaimed = sum(1 for lead in pool if (lead.get("tier") or "").lower().endswith("gold"))
		activity = get_zip_activity(top_zip, vertical=profile.get("vertical"))
		competing = int(activity.get("active_viewers", 0))

	return {
		"unclaimed_gold_count": unclaimed,
		"competing_viewers": competing,
	}


def _node_hierarchy_check(state: RetentionState) -> RetentionState:
	if state.get("terminal_status"):
		return {}

	hierarchy = run_decision_hierarchy({
		"subscriber_id": state["subscriber_id"],
		"graph_name": GRAPH_NAME,
		"kill_switch_feature": None,          # priority-list scope: fail-open
		"learning_card_type": "churn_signal",
	})

	if not hierarchy.get("action_allowed", True):
		return {
			"terminal_status": "aborted",
			"failure_reason": hierarchy.get("action_blocked_reason", "hierarchy_blocked"),
		}

	return {
		"action_allowed": True,
		"use_fallback": bool(hierarchy.get("use_fallback", False)),
	}


def _node_build_context(state: RetentionState) -> RetentionState:
	if state.get("terminal_status"):
		return {}

	profile = state.get("subscriber_profile") or {}
	wallet = state.get("wallet_state") or {}
	deals = state.get("deal_history") or []

	largest_deal = 0
	for d in deals:
		amt = float(d.get("deal_amount") or 0)
		if amt > largest_deal:
			largest_deal = amt

	ctx = {
		"tier": state.get("tier_cohort", profile.get("tier", "wallet")),
		"subscriber_first_name": (profile.get("name") or "there").split(" ")[0],
		"first_name": (profile.get("name") or "there").split(" ")[0],
		"credits_used_total": wallet.get("credits_used_total", 0),
		"leads_unlocked_count": wallet.get("credits_used_total", 0),   # proxy
		"deals_reported_count": len(deals),
		"largest_deal_size": f"${largest_deal:,.0f}" if largest_deal else "none yet",
		"top_zip": state.get("top_zip") or "your area",
		"unclaimed_gold_count": state.get("unclaimed_gold_count", 0),
		"competing_viewers": state.get("competing_viewers", 0),
		"action_link": f"https://app.forcedaction.io/feed/{profile.get('id')}",
	}

	system, user = render_system_and_user(GRAPH_NAME, ctx)
	fallback = render_fallback_body(GRAPH_NAME, ctx)
	return {
		"_system_prompt": system,
		"_user_prompt": user,
		"_fallback_body": fallback,
	}


def _node_compose_and_send(state: RetentionState) -> RetentionState:
	if state.get("terminal_status"):
		return {}

	campaign = f"{CAMPAIGN_PREFIX}_{state.get('tier_cohort', 'wallet')}"
	result = run_compose_and_send({
		"decision_id": state["decision_id"],
		"graph_name": GRAPH_NAME,
		"subscriber_id": state["subscriber_id"],
		"campaign": campaign,
		"claude_task_type": "retention_copy",      # routes to Sonnet via claude_router
		"system_prompt": state.get("_system_prompt", ""),
		"user_prompt": state.get("_user_prompt", ""),
		"cache_system": True,
		"max_output_tokens": 400,
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


def _node_finalize(state: RetentionState) -> RetentionState:
	from src.agents.tools.write_tools import log_decision

	final_status = state.get("terminal_status") or "completed"

	# If compose_and_send did not run (early abort), we still owe an audit row.
	# compose_and_send writes its own log_decision on the happy path; here we
	# only write when the graph aborted before reaching compose_and_send.
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
				summary={"failure_reason": state.get("failure_reason"), "early_abort": True},
			)
		except Exception:
			# Audit logging must never mask the original outcome.
			pass

	if not state.get("terminal_status"):
		return {"terminal_status": "completed"}
	return {}


# ──────────────────────────────────────────────────────────────────────────────
# Assembly
# ──────────────────────────────────────────────────────────────────────────────

def build_retention_graph() -> StateGraph:
	g = StateGraph(RetentionState)
	g.add_node("load_profile", _node_load_profile)
	g.add_node("assemble_history", _node_assemble_history)
	g.add_node("assemble_opportunity_gap", _node_assemble_opportunity_gap)
	g.add_node("hierarchy_check", _node_hierarchy_check)
	g.add_node("build_context", _node_build_context)
	g.add_node("compose_and_send", _node_compose_and_send)
	g.add_node("finalize", _node_finalize)

	g.add_edge(START, "load_profile")
	g.add_edge("load_profile", "assemble_history")
	g.add_edge("assemble_history", "assemble_opportunity_gap")
	g.add_edge("assemble_opportunity_gap", "hierarchy_check")
	g.add_edge("hierarchy_check", "build_context")
	g.add_edge("build_context", "compose_and_send")
	g.add_edge("compose_and_send", "finalize")
	g.add_edge("finalize", END)
	return g


def run_retention(subscriber_id: int, tier_cohort: str,
				  decision_id: Optional[str] = None) -> Dict[str, Any]:
	graph = build_retention_graph().compile()
	final = graph.invoke({
		"decision_id": decision_id or str(uuid.uuid4()),
		"subscriber_id": subscriber_id,
		"event_type": "retention_summary_due",
		"event_payload": {"tier": tier_cohort},
		"tier_cohort": tier_cohort,
	})
	return dict(final)


def run_retention_batch(subscriber_ids: Iterable[int], tier_cohort: str) -> List[Dict[str, Any]]:
	"""
	Batched fan-out helper. The event producer (cron) calls this with a
	cohort list; each subscriber gets one graph invocation with its own
	decision_id. Runs sequentially — concurrency is handled by the outer
	supervisor worker pool when in production.
	"""
	results: List[Dict[str, Any]] = []
	for sid in subscriber_ids:
		results.append(run_retention(sid, tier_cohort))
	return results
