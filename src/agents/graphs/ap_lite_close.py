"""
AutoPilot Lite close graph.

Triggered when a Territory Lock subscriber exceeds the manual-action
threshold (subscriber_crossed_ap_lite_threshold from ap_lite_sweep).

Sends an SMS CTA to upgrade to AutoPilot Lite.

Event envelope (example):
    {
        "event_type": "subscriber_crossed_ap_lite_threshold",
        "payload": {
            "weekly_actions": 14,
            "threshold": 10,
            "week": "2026-W18",
            "cta_url": "https://app.forcedaction.io/dashboard/<uuid>?upgrade=autopilot_lite",
        },
    }
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.prompts.loader import render_fallback_body, render_system_and_user
from src.agents.subgraphs.compose_and_send import run_compose_and_send
from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import get_subscriber_profile


GRAPH_NAME = "ap_lite_close"
CAMPAIGN = "ap_lite_conversion"
CLAUDE_TASK_TYPE = "sms_copy"
KILL_SWITCH_FEATURE = "ap_lite_conversion"


class ApLiteState(TypedDict, total=False):
    decision_id: str
    subscriber_id: int
    event_type: str
    event_payload: dict

    subscriber_profile: dict

    action_allowed: bool
    action_blocked_reason: str
    use_fallback: bool
    kill_switch_color: str
    revenue_signal_score: int

    # Intermediate compose inputs (must be declared — LangGraph drops undeclared keys)
    _system_prompt: str
    _user_prompt: str
    _fallback_body: str
    _render_context: dict
    _variant_id: Optional[str]

    message_body: str
    sent: bool
    send_reason: str
    message_outcome_id: Optional[int]
    tokens_used: int
    cost_usd: float
    terminal_status: str
    failure_reason: str


def _node_assemble_context(state: ApLiteState) -> ApLiteState:
    profile = get_subscriber_profile(state["subscriber_id"])
    if not profile:
        return {
            "terminal_status": "aborted",
            "failure_reason": "ap_lite_close:subscriber_not_found",
        }
    return {"subscriber_profile": profile}


def _node_hierarchy_check(state: ApLiteState) -> ApLiteState:
    if state.get("terminal_status"):
        return {}

    hierarchy = run_decision_hierarchy({
        "subscriber_id": state["subscriber_id"],
        "graph_name": GRAPH_NAME,
        "kill_switch_feature": KILL_SWITCH_FEATURE,
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


def _node_build_compose_context(state: ApLiteState) -> Dict[str, Any]:
    if state.get("terminal_status"):
        return {}

    profile = state.get("subscriber_profile") or {}
    payload = state.get("event_payload") or {}
    weekly_actions = payload.get("weekly_actions") or 0

    context = {
        "first_name": (profile.get("name") or "there").split(" ")[0],
        "subscriber_first_name": (profile.get("name") or "there").split(" ")[0],
        "weekly_actions": weekly_actions,
        "threshold": payload.get("threshold") or 10,
        "cta_url": payload.get("cta_url") or "",
        "tier": profile.get("tier") or "annual_lock",
        "revenue_signal_score": state.get("revenue_signal_score", 0),
        "action_signal": f"you did {weekly_actions} manual lead actions last week",
    }

    system, user = render_system_and_user(GRAPH_NAME, context)
    fallback = render_fallback_body(GRAPH_NAME, context)

    return {
        "_system_prompt": system,
        "_user_prompt": user,
        "_fallback_body": fallback,
        "_render_context": context,
    }


def _node_compose_and_send(state: ApLiteState) -> ApLiteState:
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
        "cache_system": True,
        "max_output_tokens": 160,
        "variant_id": None,
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


def _node_finalize(state: ApLiteState) -> ApLiteState:
    from src.agents.tools.write_tools import log_decision

    final_status = state.get("terminal_status") or "completed"

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
            pass

    if not state.get("terminal_status"):
        return {"terminal_status": "completed"}
    return {}


def build_ap_lite_graph() -> StateGraph:
    g = StateGraph(ApLiteState)
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


def run_ap_lite_close(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    graph = build_ap_lite_graph().compile()
    final = graph.invoke({
        "decision_id": decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type": "subscriber_crossed_ap_lite_threshold",
        "event_payload": event_payload,
    })
    return dict(final)
