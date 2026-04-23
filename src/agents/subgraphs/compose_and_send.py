"""
compose_and_send_compliant_sms — the standard "say something to a user" chain.

Four nodes executed in order:

  1. budget_precheck  — abort early if this decision already busted its budget
  2. compose          — Claude call to generate the message body
  3. compliance_gate  — subscriber-level TCPA/DNC check before dispatch
  4. send_and_log     — send_sms tool + log_decision finalize

If any node decides the action must not proceed, it sets
`terminal_status` in state and routing jumps to END. Compose and send both
track tokens_used + cost_usd so the rolling budget is enforced across
successive Claude calls inside one decision.

This subgraph is deliberately oblivious to the business reason for a
message — the caller passes a prompt template + context via state. That
lets FOMO, Abandonment, and Retention all reuse the exact same flow.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.tools.gating_tools import budget_check, compliance_check
from src.agents.tools.write_tools import log_decision, send_sms
from src.services.claude_router import call_claude_with_usage


class ComposeAndSendState(TypedDict, total=False):
	# ── Required inputs ──────────────────────────────────────────────────────
	decision_id: str               # UUID, primary key in agent_decisions
	graph_name: str                # e.g. 'fomo', 'abandonment', 'retention'
	subscriber_id: int
	campaign: str                  # short id — used for idempotency in send_sms

	# ── Compose inputs ───────────────────────────────────────────────────────
	claude_task_type: str          # routes model tier (haiku/sonnet/opus)
	system_prompt: str             # role/persona + live context
	user_prompt: str               # the specific ask ("write the SMS")
	cache_system: bool             # prompt caching hint
	max_output_tokens: int         # default 160 words ≈ 240 tokens

	# ── Optional inputs ──────────────────────────────────────────────────────
	variant_id: Optional[str]      # A/B variant attribution
	message_type: str              # 'marketing' | 'transactional'
	ab_fallback_body: Optional[str]  # literal body to send when use_fallback=True

	# Feature flags the caller may already have resolved.
	use_fallback: Optional[bool]   # from decision_hierarchy (yellow kill-switch)

	# ── Carried-in budget state (zero-ok if caller doesn't track it) ─────────
	tokens_used: int
	cost_usd: float

	# ── Outputs ──────────────────────────────────────────────────────────────
	message_body: str
	compliance_allowed: bool
	compliance_reason: str
	sent: bool
	send_reason: str
	message_outcome_id: Optional[int]
	terminal_status: str           # 'completed' | 'aborted' | 'escalated' | 'failed'
	failure_reason: str


# ──────────────────────────────────────────────────────────────────────────────
# Nodes
# ──────────────────────────────────────────────────────────────────────────────

def _node_budget_precheck(state: ComposeAndSendState) -> ComposeAndSendState:
	result = budget_check(
		tokens_used=int(state.get("tokens_used", 0) or 0),
		cost_usd=float(state.get("cost_usd", 0.0) or 0.0),
		graph_name=state.get("graph_name"),
	)
	if not result["allowed"]:
		return {
			"terminal_status": "aborted",
			"failure_reason": f"budget:{result['reason']}",
		}
	return {}


def _node_compose(state: ComposeAndSendState) -> ComposeAndSendState:
	# If caller already flagged a fallback path, skip Claude entirely and use
	# the static copy. This keeps RED/YELLOW kill-switch days from burning
	# tokens on graphs we've explicitly throttled.
	if state.get("use_fallback") and state.get("ab_fallback_body"):
		return {
			"message_body": state["ab_fallback_body"],
			"tokens_used": int(state.get("tokens_used", 0) or 0),
			"cost_usd": float(state.get("cost_usd", 0.0) or 0.0),
		}

	task_type = state.get("claude_task_type") or "sms_copy"
	system = state.get("system_prompt") or ""
	user = state.get("user_prompt") or ""
	max_tokens = int(state.get("max_output_tokens") or 240)
	cache_system = bool(state.get("cache_system", False))

	messages: List[Dict[str, Any]] = [{"role": "user", "content": user}]

	try:
		result = call_claude_with_usage(
			task_type=task_type,
			messages=messages,
			system=system or None,
			cache_system=cache_system,
			max_tokens=max_tokens,
			subscriber_id=state.get("subscriber_id"),
		)
	except Exception as exc:
		return {
			"terminal_status": "failed",
			"failure_reason": f"compose:{type(exc).__name__}:{exc}",
		}

	return {
		"message_body": result["text"].strip(),
		"tokens_used": int(state.get("tokens_used", 0) or 0)
			+ int(result["input_tokens"]) + int(result["output_tokens"]),
		"cost_usd": float(state.get("cost_usd", 0.0) or 0.0) + float(result["cost_usd"]),
	}


def _node_compliance_gate(state: ComposeAndSendState) -> ComposeAndSendState:
	if state.get("terminal_status"):
		return {}

	subscriber_id = state.get("subscriber_id")
	if subscriber_id is None:
		return {
			"compliance_allowed": False,
			"compliance_reason": "no_subscriber_id",
			"terminal_status": "aborted",
			"failure_reason": "compliance:no_subscriber_id",
		}

	result = compliance_check(
		subscriber_id=subscriber_id,
		message_type=state.get("message_type") or "marketing",
	)
	if not result["can_send"]:
		return {
			"compliance_allowed": False,
			"compliance_reason": result["reason"],
			"terminal_status": "aborted",
			"failure_reason": f"compliance:{result['reason']}",
		}
	return {"compliance_allowed": True, "compliance_reason": "ok"}


def _node_send_and_log(state: ComposeAndSendState) -> ComposeAndSendState:
	if state.get("terminal_status"):
		# A prior node already aborted — still write the audit row.
		log_decision(
			decision_id=state["decision_id"],
			graph_name=state.get("graph_name") or "unknown",
			subscriber_id=state.get("subscriber_id"),
			terminal_status=state["terminal_status"],
			tokens_used=int(state.get("tokens_used", 0) or 0),
			cost_usd=float(state.get("cost_usd", 0.0) or 0.0),
			summary={
				"failure_reason": state.get("failure_reason"),
				"compose_skipped": True,
			},
		)
		return {}

	body = state.get("message_body") or ""
	if not body:
		log_decision(
			decision_id=state["decision_id"],
			graph_name=state.get("graph_name") or "unknown",
			subscriber_id=state.get("subscriber_id"),
			terminal_status="failed",
			tokens_used=int(state.get("tokens_used", 0) or 0),
			cost_usd=float(state.get("cost_usd", 0.0) or 0.0),
			summary={"failure_reason": "empty_message_body"},
		)
		return {
			"sent": False,
			"send_reason": "empty_message_body",
			"terminal_status": "failed",
			"failure_reason": "empty_message_body",
		}

	send_result = send_sms(
		subscriber_id=state["subscriber_id"],
		body=body,
		campaign=state["campaign"],
		variant_id=state.get("variant_id"),
		decision_id=state.get("decision_id"),
		message_type=state.get("message_type") or "marketing",
	)

	if send_result["sent"]:
		final_status = "completed"
	elif send_result["reason"] == "duplicate":
		# Not a failure — idempotency working as intended.
		final_status = "completed"
	else:
		final_status = "aborted"

	log_decision(
		decision_id=state["decision_id"],
		graph_name=state.get("graph_name") or "unknown",
		subscriber_id=state.get("subscriber_id"),
		terminal_status=final_status,
		tokens_used=int(state.get("tokens_used", 0) or 0),
		cost_usd=float(state.get("cost_usd", 0.0) or 0.0),
		summary={
			"campaign": state.get("campaign"),
			"variant_id": state.get("variant_id"),
			"send_reason": send_result["reason"],
			"message_outcome_id": send_result.get("message_outcome_id"),
		},
	)

	return {
		"sent": send_result["sent"],
		"send_reason": send_result["reason"],
		"message_outcome_id": send_result.get("message_outcome_id"),
		"terminal_status": final_status,
	}


# ──────────────────────────────────────────────────────────────────────────────
# Routing
# ──────────────────────────────────────────────────────────────────────────────

def _after_budget(state: ComposeAndSendState) -> str:
	if state.get("terminal_status"):
		return "send_and_log"   # still write the audit row
	return "compose"


def _after_compose(state: ComposeAndSendState) -> str:
	if state.get("terminal_status"):
		return "send_and_log"
	return "compliance_gate"


def _after_compliance(state: ComposeAndSendState) -> str:
	# Always routes to send_and_log — it decides whether to dispatch based on
	# terminal_status. Keeping this a single exit point means the audit log
	# captures every decision path.
	return "send_and_log"


# ──────────────────────────────────────────────────────────────────────────────
# Graph assembly
# ──────────────────────────────────────────────────────────────────────────────

def build_compose_and_send_graph() -> StateGraph:
	g = StateGraph(ComposeAndSendState)

	g.add_node("budget_precheck", _node_budget_precheck)
	g.add_node("compose", _node_compose)
	g.add_node("compliance_gate", _node_compliance_gate)
	g.add_node("send_and_log", _node_send_and_log)

	g.add_edge(START, "budget_precheck")
	g.add_conditional_edges(
		"budget_precheck", _after_budget,
		{"compose": "compose", "send_and_log": "send_and_log"},
	)
	g.add_conditional_edges(
		"compose", _after_compose,
		{"compliance_gate": "compliance_gate", "send_and_log": "send_and_log"},
	)
	g.add_conditional_edges(
		"compliance_gate", _after_compliance,
		{"send_and_log": "send_and_log"},
	)
	g.add_edge("send_and_log", END)

	return g


def run_compose_and_send(inputs: Dict[str, Any]) -> Dict[str, Any]:
	"""Convenience wrapper: compile + invoke the compose-and-send chain."""
	graph = build_compose_and_send_graph().compile()
	final = graph.invoke(inputs)
	return dict(final)
