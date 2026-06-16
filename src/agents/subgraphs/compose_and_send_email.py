"""
compose_and_send_email — the standard "send an email to a user" chain.

Three nodes executed in order:

  1. budget_precheck  — abort early if this decision already busted its budget
  2. compose          — Claude call to generate the email body
  3. send_and_log     — send_email write tool + log_decision finalize

Mirrors compose_and_send.py (SMS) but for email via Mailchimp SMTP relay.
No TCPA compliance gate — that is SMS-specific. Suppression and dedup are
enforced inside the send_email write tool.

Caller is responsible for passing a rendered subject line — the subgraph
does not generate the subject, only the body.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.tools.gating_tools import budget_check
from src.agents.tools.write_tools import log_decision, send_email
from src.core.database import Database
from src.services.claude_router import call_claude_with_usage

logger = logging.getLogger(__name__)


class ComposeAndSendEmailState(TypedDict, total=False):
    # ── Required inputs ───────────────────────────────────────────────────────
    decision_id: str               # UUID, primary key in agent_decisions
    graph_name: str                # e.g. 'reactivation'
    subscriber_id: int
    campaign: str                  # short id — used for dedup in send_email
    recipient_email: str           # destination address
    subject: str                   # email subject line (caller renders this)

    # ── Compose inputs ────────────────────────────────────────────────────────
    claude_task_type: str          # routes model tier (haiku/sonnet/opus)
    system_prompt: str
    user_prompt: str
    cache_system: bool
    max_output_tokens: int         # default 400 — email bodies are longer than SMS
    force_tier: Optional[str]      # override routing

    # ── Optional inputs ───────────────────────────────────────────────────────
    variant_id: Optional[str]
    ab_fallback_body: Optional[str]
    personalization_context: Optional[dict]
    use_fallback: Optional[bool]   # from decision_hierarchy (yellow kill-switch)

    # ── Carried-in budget state ───────────────────────────────────────────────
    tokens_used: int
    cost_usd: float

    # ── Outputs ───────────────────────────────────────────────────────────────
    message_body: str
    sent: bool
    send_reason: str
    message_outcome_id: Optional[int]
    terminal_status: str           # 'completed' | 'aborted' | 'escalated' | 'failed'
    failure_reason: str


# ─────────────────────────────────────────────────────────────────────────────
# Nodes
# ─────────────────────────────────────────────────────────────────────────────

def _node_budget_precheck(state: ComposeAndSendEmailState) -> ComposeAndSendEmailState:
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


def _node_compose(state: ComposeAndSendEmailState) -> ComposeAndSendEmailState:
    if state.get("use_fallback") and state.get("ab_fallback_body"):
        return {
            "message_body": state["ab_fallback_body"],
            "tokens_used": int(state.get("tokens_used", 0) or 0),
            "cost_usd": float(state.get("cost_usd", 0.0) or 0.0),
        }

    task_type = state.get("claude_task_type") or "email_copy"
    system = state.get("system_prompt") or ""
    user = state.get("user_prompt") or ""
    max_tokens = int(state.get("max_output_tokens") or 400)
    cache_system = bool(state.get("cache_system", False))

    if not user.strip():
        return {
            "terminal_status": "failed",
            "failure_reason": "compose:empty_user_prompt",
        }

    messages: List[Dict[str, Any]] = [{"role": "user", "content": user}]

    try:
        with Database().session_scope() as session:
            result = call_claude_with_usage(
                task_type=task_type,
                messages=messages,
                system=system or None,
                cache_system=cache_system,
                max_tokens=max_tokens,
                subscriber_id=state.get("subscriber_id"),
                graph_name=state.get("graph_name"),
                force_tier=state.get("force_tier"),
                db=session,
            )
    except Exception as exc:
        fallback = state.get("ab_fallback_body")
        if fallback:
            logger.warning(
                "compose_email: Claude call failed (%s: %s) — using static fallback body",
                type(exc).__name__, exc,
            )
            return {
                "message_body": fallback,
                "tokens_used": int(state.get("tokens_used", 0) or 0),
                "cost_usd": float(state.get("cost_usd", 0.0) or 0.0),
            }
        return {
            "terminal_status": "failed",
            "failure_reason": f"compose:{type(exc).__name__}:{exc}",
        }

    if result["text"].startswith("[BLOCKED]"):
        fallback = state.get("ab_fallback_body")
        if fallback:
            return {
                "message_body": fallback,
                "tokens_used": int(state.get("tokens_used", 0) or 0),
                "cost_usd": float(state.get("cost_usd", 0.0) or 0.0),
            }
        return {
            "terminal_status": "aborted",
            "failure_reason": "compose:vendor_pause",
        }

    return {
        "message_body": result["text"].strip(),
        "tokens_used": int(state.get("tokens_used", 0) or 0)
            + int(result["input_tokens"]) + int(result["output_tokens"]),
        "cost_usd": float(state.get("cost_usd", 0.0) or 0.0) + float(result["cost_usd"]),
    }


def _node_send_and_log(state: ComposeAndSendEmailState) -> ComposeAndSendEmailState:
    if state.get("terminal_status"):
        log_decision(
            decision_id=state["decision_id"],
            graph_name=state.get("graph_name") or "unknown",
            subscriber_id=state.get("subscriber_id"),
            terminal_status=state["terminal_status"],
            tokens_used=int(state.get("tokens_used", 0) or 0),
            cost_usd=float(state.get("cost_usd", 0.0) or 0.0),
            variant_id=state.get("variant_id"),
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
            variant_id=state.get("variant_id"),
            summary={"failure_reason": "empty_message_body"},
        )
        return {
            "sent": False,
            "send_reason": "empty_message_body",
            "terminal_status": "failed",
            "failure_reason": "empty_message_body",
        }

    send_result = send_email(
        subscriber_id=state["subscriber_id"],
        recipient_email=state["recipient_email"],
        subject=state["subject"],
        body=body,
        campaign=state["campaign"],
        variant_id=state.get("variant_id"),
        decision_id=state.get("decision_id"),
        personalization_context=state.get("personalization_context"),
    )

    if send_result["sent"]:
        final_status = "completed"
    elif send_result["reason"] == "duplicate":
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
        variant_id=state.get("variant_id"),
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


# ─────────────────────────────────────────────────────────────────────────────
# Routing
# ─────────────────────────────────────────────────────────────────────────────

def _after_budget(state: ComposeAndSendEmailState) -> str:
    if state.get("terminal_status"):
        return "send_and_log"
    return "compose"


def _after_compose(state: ComposeAndSendEmailState) -> str:
    if state.get("terminal_status"):
        return "send_and_log"
    return "send_and_log"


# ─────────────────────────────────────────────────────────────────────────────
# Graph assembly
# ─────────────────────────────────────────────────────────────────────────────

def build_compose_and_send_email_graph() -> StateGraph:
    g = StateGraph(ComposeAndSendEmailState)

    g.add_node("budget_precheck", _node_budget_precheck)
    g.add_node("compose", _node_compose)
    g.add_node("send_and_log", _node_send_and_log)

    g.add_edge(START, "budget_precheck")
    g.add_conditional_edges(
        "budget_precheck", _after_budget,
        {"compose": "compose", "send_and_log": "send_and_log"},
    )
    g.add_conditional_edges(
        "compose", _after_compose,
        {"send_and_log": "send_and_log"},
    )
    g.add_edge("send_and_log", END)

    return g


def run_compose_and_send_email(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience wrapper: compile + invoke the compose-and-send-email chain."""
    graph = build_compose_and_send_email_graph().compile()
    final = graph.invoke(inputs)
    return dict(final)
