"""
NWS Storm Urgency graph.

Triggered when a qualifying NWS alert covers ZIPs where a subscriber holds
locked territory AND the subscriber has Gold+ leads in those ZIPs.

Event payload:
    {
        "alert_id":     str,   # NWS @id
        "event":        str,   # "Severe Thunderstorm Warning"
        "headline":     str,   # official NWS headline (used verbatim — no invention)
        "area_desc":    str,   # NWS areaDesc
        "expires":      str,   # ISO timestamp
        "affected_zips": list, # ZIPs in subscriber's territory covered by alert
        "lead_count":   int,   # Gold+ leads in those ZIPs
    }

Flow (5 nodes — mirrors fomo.py):
    1. assemble_context      — subscriber profile + lead context
    2. hierarchy_check       — shared decision_hierarchy subgraph
    3. build_compose_context — render prompt templates
    4. compose_and_send      — shared compose_and_send subgraph
    5. finalize              — agent_decisions log + mark nws_alerts.cora_urgency_sent
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.prompts.loader import render_fallback_body, render_for_subscriber_auto
from src.agents.subgraphs.compose_and_send import run_compose_and_send
from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import get_subscriber_profile


GRAPH_NAME = "nws_urgency"
CAMPAIGN = "nws_storm_urgency"
CLAUDE_TASK_TYPE = "sms_copy"
AB_TEST_NAME: Optional[str] = None


class NWSUrgencyState(TypedDict, total=False):
    # ── Inputs ───────────────────────────────────────────────────────────────
    decision_id: str
    subscriber_id: int
    event_type: str
    event_payload: dict

    # ── Assembled context ────────────────────────────────────────────────────
    subscriber_profile: dict

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


# ──────────────────────────────────────────────────────────────────────────────
# Nodes
# ──────────────────────────────────────────────────────────────────────────────

def _node_assemble_context(state: NWSUrgencyState) -> NWSUrgencyState:
    subscriber_id = state["subscriber_id"]

    profile = get_subscriber_profile(subscriber_id)
    if not profile:
        return {
            "terminal_status": "aborted",
            "failure_reason": "nws_urgency:subscriber_not_found",
        }

    return {"subscriber_profile": profile}


def _node_hierarchy_check(state: NWSUrgencyState) -> NWSUrgencyState:
    if state.get("terminal_status"):
        return {}

    hierarchy = run_decision_hierarchy({
        "subscriber_id": state["subscriber_id"],
        "graph_name": GRAPH_NAME,
        "kill_switch_feature": None,
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


def _node_build_compose_context(state: NWSUrgencyState) -> Dict[str, Any]:
    if state.get("terminal_status"):
        return {}

    profile = state.get("subscriber_profile") or {}
    payload = state.get("event_payload") or {}

    affected_zips = payload.get("affected_zips") or []
    zips_display = ", ".join(affected_zips[:4])
    if len(affected_zips) > 4:
        zips_display += f" (+{len(affected_zips) - 4} more)"

    context = {
        "subscriber_first_name": (profile.get("name") or "there").split(" ")[0],
        "first_name": (profile.get("name") or "there").split(" ")[0],
        "vertical": profile.get("vertical") or "",
        "affected_zips_display": zips_display,
        "lead_count": payload.get("lead_count", 0),
        "alert_event": payload.get("event", "Weather Alert"),
        "alert_headline": payload.get("headline", payload.get("event", "")),
        "area_desc": payload.get("area_desc", "")[:300],
        "alert_expires": payload.get("expires", ""),
        "unlock_link": f"https://app.forcedaction.io/feed/{profile.get('id')}",
        "revenue_signal_score": state.get("revenue_signal_score", 0),
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
        "_variant_id": f"{test_name}:{variant}" if test_name and variant else None,
    }


def _node_compose_and_send(state: NWSUrgencyState) -> NWSUrgencyState:
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
        "variant_id": state.get("_variant_id"),
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


def _node_finalize(state: NWSUrgencyState) -> NWSUrgencyState:
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

    # Mark nws_alerts.cora_urgency_sent + log URGENCY_MESSAGE_SENT
    if state.get("sent"):
        payload = state.get("event_payload") or {}
        alert_id = payload.get("alert_id")
        if alert_id:
            try:
                from src.core.database import get_db_context
                from src.core.models import NWSAlert
                from sqlalchemy import select, update
                from src.services.nws_webhook import _log_event
                with get_db_context() as db:
                    db.execute(
                        update(NWSAlert)
                        .where(NWSAlert.alert_id == alert_id)
                        .values(cora_urgency_sent=True)
                    )
                    _log_event(db, "URGENCY_MESSAGE_SENT", {
                        "alert_id": alert_id,
                        "subscriber_id": state.get("subscriber_id"),
                        "lead_count": payload.get("lead_count"),
                    })
                    db.commit()
            except Exception:
                pass

    if not state.get("terminal_status"):
        return {"terminal_status": "completed"}
    return {}


# ──────────────────────────────────────────────────────────────────────────────
# Graph assembly
# ──────────────────────────────────────────────────────────────────────────────

def build_nws_urgency_graph() -> StateGraph:
    g = StateGraph(NWSUrgencyState)
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


def run_nws_urgency(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Compile + invoke the NWS Urgency graph for a single event."""
    graph = build_nws_urgency_graph().compile()
    final = graph.invoke({
        "decision_id": decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type": "nws_storm_alert_active",
        "event_payload": event_payload,
    })
    return dict(final)
