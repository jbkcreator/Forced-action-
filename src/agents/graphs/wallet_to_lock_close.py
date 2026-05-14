"""
Wallet-to-Lock close graph.

Triggered when a Wallet subscriber crosses the spend threshold in a ZIP
(subscriber_crossed_lock_threshold event from wallet_to_lock_sweep).

Sends an SMS CTA to upgrade to Territory Lock before a competitor does.

Event envelope (example):
    {
        "event_type": "subscriber_crossed_lock_threshold",
        "payload": {
            "zip_code": "33647",
            "credits_spent": 42,
            "lock_threshold": 40,
            "cta_url": "https://app.forcedaction.io/checkout?...",
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


GRAPH_NAME = "wallet_to_lock_close"
CAMPAIGN = "wallet_lock_conversion"
CLAUDE_TASK_TYPE = "sms_copy"
KILL_SWITCH_FEATURE = "lock_conversion"


class WalletToLockState(TypedDict, total=False):
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


def _node_assemble_context(state: WalletToLockState) -> WalletToLockState:
    profile = get_subscriber_profile(state["subscriber_id"])
    if not profile:
        return {
            "terminal_status": "aborted",
            "failure_reason": "wallet_to_lock:subscriber_not_found",
        }
    return {"subscriber_profile": profile}


def _node_hierarchy_check(state: WalletToLockState) -> WalletToLockState:
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


def _node_build_compose_context(state: WalletToLockState) -> Dict[str, Any]:
    if state.get("terminal_status"):
        return {}

    profile = state.get("subscriber_profile") or {}
    payload = state.get("event_payload") or {}

    credits_spent = payload.get("credits_spent") or 0
    zip_code = payload.get("zip_code") or ""
    uncontacted_count = payload.get("uncontacted_count", 0)
    tier_breakdown = payload.get("tier_breakdown") or {}
    spend_rate = round(credits_spent / 4, 1) if credits_spent else 0

    try:
        from src.services.urgency_engine import get_active_count as _active_count
        competing_viewers = _active_count(zip_code)
    except Exception:
        competing_viewers = 0

    context = {
        "first_name": (profile.get("name") or "there").split(" ")[0],
        "subscriber_first_name": (profile.get("name") or "there").split(" ")[0],
        "zip_code": zip_code,
        "credits_spent": credits_spent,
        "spend_rate_per_week": spend_rate,
        "uncontacted_count": uncontacted_count,
        "gold_count": tier_breakdown.get("gold", 0),
        "silver_count": tier_breakdown.get("silver", 0),
        "bronze_count": tier_breakdown.get("bronze", 0),
        "competing_viewers": competing_viewers,
        "lock_threshold": payload.get("lock_threshold") or 40,
        "cta_url": payload.get("cta_url") or "",
        "tier": profile.get("tier") or "wallet",
        "revenue_signal_score": state.get("revenue_signal_score", 0),
        "lock_signal": (
            f"you've spent {credits_spent} credits in "
            f"{zip_code or 'this ZIP'} this month"
        ),
    }

    system, user = render_system_and_user(GRAPH_NAME, context)
    fallback = render_fallback_body(GRAPH_NAME, context)

    return {
        "_system_prompt": system,
        "_user_prompt": user,
        "_fallback_body": fallback,
        "_render_context": context,
    }


def _node_compose_and_send(state: WalletToLockState) -> WalletToLockState:
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


def _node_finalize(state: WalletToLockState) -> WalletToLockState:
    import json as _json
    from src.agents.tools.write_tools import log_decision

    final_status = state.get("terminal_status") or "completed"
    payload = state.get("event_payload") or {}
    uncontacted_count = payload.get("uncontacted_count", 0)
    tier_breakdown = payload.get("tier_breakdown") or {}

    if final_status == "completed" and state.get("sent"):
        # Set pending offer so YES reply routes to checkout (24h TTL)
        try:
            from src.core.redis_client import redis_available, rset as _rset
            if redis_available():
                offer = _json.dumps({
                    "type": "lock_close",
                    "zip_code": payload.get("zip_code", ""),
                })
                _rset(f"fa:pending_offer:{state['subscriber_id']}", offer, ttl_seconds=86400)
        except Exception:
            pass
    else:
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
                    "uncontacted_count": uncontacted_count,
                    "tier_breakdown": tier_breakdown,
                },
            )
        except Exception:
            pass

    if not state.get("terminal_status"):
        return {"terminal_status": "completed"}
    return {}


def build_wallet_to_lock_graph() -> StateGraph:
    g = StateGraph(WalletToLockState)
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


def run_wallet_to_lock_close(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    graph = build_wallet_to_lock_graph().compile()
    final = graph.invoke({
        "decision_id": decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type": "subscriber_crossed_lock_threshold",
        "event_payload": event_payload,
    })
    return dict(final)
