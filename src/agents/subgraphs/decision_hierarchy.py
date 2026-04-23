"""
decision_hierarchy_check — the 6-step gate every Cora action passes through.

Composed purely from gating + read tools. No Claude calls.

Step order (from LANGGRAPH-ARCHITECTURE.md §3):

  1. Hard guardrails          — guardrail_check on (decision_type, proposed_value)
  2. Learning card            — get_learning_card; inform fallback selection
  3. Live Redis state         — skip for priority-list scope; placeholder hook
  4. Subscriber segment+score — get_segment_and_score
  5. A/B variant              — ab_variant_assign (if test_name provided)
  6. Kill-switch colour       — kill_switch_status (if feature provided)

A 'no' at any step sets state['action_allowed']=False and short-circuits to
END. A 'yellow' at the kill-switch step sets state['use_fallback']=True and
allows — callers are expected to pick a simpler copy path. An 'unknown'
colour is treated as RED (fail-safe).

Only decision_type + proposed_value are required inputs for the guardrail
step; every other check has a sensible "not applicable → pass" behaviour so
graphs that don't use a particular signal skip it cleanly.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.tools.gating_tools import (
	ab_variant_assign,
	guardrail_check,
	kill_switch_status,
)
from src.agents.tools.read_tools import (
	get_learning_card,
	get_segment_and_score,
)


class DecisionHierarchyState(TypedDict, total=False):
	# ── Inputs (graphs populate what applies) ────────────────────────────────
	subscriber_id: int
	graph_name: str

	# Guardrail check (optional — skipped if decision_type is None)
	decision_type: Optional[str]
	proposed_value: Optional[float]

	# Kill-switch (optional — skipped if feature is None)
	kill_switch_feature: Optional[str]
	kill_switch_observed_value: Optional[float]

	# A/B (optional — skipped if ab_test_name is None)
	ab_test_name: Optional[str]

	# Learning card type to consult. Defaults to 'general' if missing.
	learning_card_type: Optional[str]

	# ── Outputs ───────────────────────────────────────────────────────────────
	action_allowed: bool
	action_blocked_reason: str
	use_fallback: bool
	hierarchy_path: list  # audit — which nodes fired + outcome
	kill_switch_color: str
	segment: str
	revenue_signal_score: int
	ab_variant: Optional[str]
	learning_card: dict
	guardrails_in_scope: dict


# ──────────────────────────────────────────────────────────────────────────────
# Node bodies — each appends to hierarchy_path and either short-circuits or passes
# ──────────────────────────────────────────────────────────────────────────────

def _append_path(state: DecisionHierarchyState, entry: str) -> list:
	path = list(state.get("hierarchy_path") or [])
	path.append(entry)
	return path


def _node_check_guardrail(state: DecisionHierarchyState) -> DecisionHierarchyState:
	decision_type = state.get("decision_type")
	proposed_value = state.get("proposed_value")

	# Not all decisions have a numeric bound to check; skip cleanly.
	if decision_type is None:
		return {"hierarchy_path": _append_path(state, "guardrail:skip")}

	if proposed_value is None:
		return {
			"action_allowed": False,
			"action_blocked_reason": "guardrail_check requested without proposed_value",
			"hierarchy_path": _append_path(state, "guardrail:missing_value"),
		}

	result = guardrail_check(decision_type, proposed_value)
	if not result["allowed"]:
		return {
			"action_allowed": False,
			"action_blocked_reason": f"guardrail:{result['reason']}",
			"guardrails_in_scope": result["bound"],
			"hierarchy_path": _append_path(state, f"guardrail:block:{result['reason']}"),
		}

	return {
		"guardrails_in_scope": result["bound"],
		"hierarchy_path": _append_path(state, "guardrail:pass"),
	}


def _node_consult_learning_card(state: DecisionHierarchyState) -> DecisionHierarchyState:
	card_type = state.get("learning_card_type") or "general"
	card = get_learning_card(card_type)

	# No card yet? That's fine — graphs fall back to default copy.
	if not card:
		return {
			"learning_card": {},
			"hierarchy_path": _append_path(state, "learning_card:absent"),
		}

	return {
		"learning_card": card,
		"hierarchy_path": _append_path(state, "learning_card:loaded"),
	}


def _node_read_segment_and_score(state: DecisionHierarchyState) -> DecisionHierarchyState:
	subscriber_id = state.get("subscriber_id")
	if subscriber_id is None:
		return {"hierarchy_path": _append_path(state, "segment:skip_no_subscriber")}

	result = get_segment_and_score(subscriber_id)
	return {
		"segment": result.get("segment", "new"),
		"revenue_signal_score": int(result.get("revenue_signal_score") or 0),
		"hierarchy_path": _append_path(state, f"segment:{result.get('segment', 'new')}"),
	}


def _node_assign_ab_variant(state: DecisionHierarchyState) -> DecisionHierarchyState:
	test_name = state.get("ab_test_name")
	subscriber_id = state.get("subscriber_id")
	if test_name is None or subscriber_id is None:
		return {
			"ab_variant": None,
			"hierarchy_path": _append_path(state, "ab:skip"),
		}

	result = ab_variant_assign(subscriber_id, test_name)
	return {
		"ab_variant": result.get("variant"),
		"hierarchy_path": _append_path(
			state,
			f"ab:{'capped' if result.get('traffic_capped') else result.get('variant')}",
		),
	}


def _node_check_kill_switch(state: DecisionHierarchyState) -> DecisionHierarchyState:
	feature = state.get("kill_switch_feature")
	if feature is None:
		# No feature declared — treat as green (action allowed without fallback).
		return {
			"action_allowed": True,
			"use_fallback": False,
			"kill_switch_color": "green",
			"hierarchy_path": _append_path(state, "kill_switch:skip"),
		}

	observed = state.get("kill_switch_observed_value")
	result = kill_switch_status(feature, observed)
	color = result["color"]

	if color == "red":
		return {
			"action_allowed": False,
			"action_blocked_reason": f"kill_switch:red:{feature}",
			"kill_switch_color": color,
			"hierarchy_path": _append_path(state, f"kill_switch:red:{feature}"),
		}

	if color == "unknown":
		# Fail-safe per architecture doc: treat unknown as RED for blocking decisions.
		return {
			"action_allowed": False,
			"action_blocked_reason": f"kill_switch:unknown:{feature}",
			"kill_switch_color": color,
			"hierarchy_path": _append_path(state, f"kill_switch:unknown:{feature}"),
		}

	use_fallback = color == "yellow"
	return {
		"action_allowed": True,
		"use_fallback": use_fallback,
		"kill_switch_color": color,
		"hierarchy_path": _append_path(
			state, f"kill_switch:{color}" + (":fallback" if use_fallback else "")
		),
	}


# ──────────────────────────────────────────────────────────────────────────────
# Conditional routing — short-circuit to END once action_allowed is False
# ──────────────────────────────────────────────────────────────────────────────

def _continue_if_allowed(
	state: DecisionHierarchyState,
	next_node: str,
) -> str:
	"""Return 'next_node' if allowed is still True-or-unset, else END."""
	if state.get("action_allowed") is False:
		return END
	return next_node


# ──────────────────────────────────────────────────────────────────────────────
# Graph assembly
# ──────────────────────────────────────────────────────────────────────────────

def build_decision_hierarchy_graph() -> StateGraph:
	"""
	Return a compilable StateGraph for the decision hierarchy check.

	Callers compile it with their own checkpointer (or none, since the
	subgraph is pure-function and cheap to re-run):

		graph = build_decision_hierarchy_graph().compile()
		final = graph.invoke(initial_state)
	"""
	g = StateGraph(DecisionHierarchyState)

	g.add_node("guardrail", _node_check_guardrail)
	g.add_node("learning_card", _node_consult_learning_card)
	g.add_node("segment", _node_read_segment_and_score)
	g.add_node("ab_variant", _node_assign_ab_variant)
	g.add_node("kill_switch", _node_check_kill_switch)

	g.add_edge(START, "guardrail")
	g.add_conditional_edges(
		"guardrail",
		lambda s: _continue_if_allowed(s, "learning_card"),
		{"learning_card": "learning_card", END: END},
	)
	g.add_edge("learning_card", "segment")
	g.add_edge("segment", "ab_variant")
	g.add_edge("ab_variant", "kill_switch")
	g.add_edge("kill_switch", END)

	return g


def run_decision_hierarchy(inputs: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Convenience wrapper: compile + invoke the hierarchy check in one call.

	Returns the final state as a dict. Caller inspects action_allowed,
	action_blocked_reason, use_fallback, kill_switch_color, etc.
	"""
	graph = build_decision_hierarchy_graph().compile()
	final = graph.invoke(inputs)
	return dict(final)
