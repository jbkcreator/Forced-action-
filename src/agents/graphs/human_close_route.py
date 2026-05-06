"""
Human close routing graph.

Triggered when escalate_to_human_closer fires (inline from Cora when
terminal_status='escalated', or via HumanCloseSweep cron weekday 1 PM UTC).

Does NOT send an SMS. Routes to human via Slack and logs to agent_decisions.

Event envelope (example):
    {
        "event_type": "escalate_to_human_closer",
        "subscriber_id": 123,
        "payload": {
            "revenue_signal_score": 92,
            "interactions_count": 5,
            "target_tier": "autopilot_pro",
            "last_decision_id": "uuid",
        },
    }
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.tools.read_tools import get_subscriber_profile


GRAPH_NAME = "human_close_route"


class HumanCloseState(TypedDict, total=False):
    decision_id: str
    subscriber_id: int
    event_type: str
    event_payload: dict

    subscriber_profile: dict
    candidate: object
    routed: bool

    terminal_status: str
    failure_reason: str


def _node_assemble_context(state: HumanCloseState) -> HumanCloseState:
    profile = get_subscriber_profile(state["subscriber_id"])
    if not profile:
        return {
            "terminal_status": "aborted",
            "failure_reason": "human_close:subscriber_not_found",
        }
    return {"subscriber_profile": profile}


def _node_route_to_human(state: HumanCloseState) -> HumanCloseState:
    if state.get("terminal_status"):
        return {}

    from src.core.database import db
    from src.services.human_close_routing import (
        HumanCloseCandidate,
        find_candidates,
        route_candidate,
    )

    payload = state.get("event_payload") or {}
    profile = state.get("subscriber_profile") or {}
    subscriber_id = state["subscriber_id"]

    with db.session_scope() as session:
        # Check if already covered by sweep (avoid double-routing)
        from src.core.models import HumanCloseEscalation
        from datetime import datetime, timedelta, timezone
        from sqlalchemy import select

        dedup_cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        existing = session.execute(
            select(HumanCloseEscalation).where(
                HumanCloseEscalation.subscriber_id == subscriber_id,
                HumanCloseEscalation.routed_at >= dedup_cutoff,
            ).limit(1)
        ).scalar_one_or_none()

        if existing:
            return {
                "routed": False,
                "terminal_status": "skipped",
                "failure_reason": "human_close:already_routed_this_week",
            }

        candidate = HumanCloseCandidate(
            subscriber_id=subscriber_id,
            revenue_signal_score=int(payload.get("revenue_signal_score") or 0),
            interactions_count=int(payload.get("interactions_count") or 0),
            target_tier=payload.get("target_tier") or "annual_lock",
            last_decision_id=payload.get("last_decision_id") or state["decision_id"],
            subscriber=session.get(
                __import__("src.core.models", fromlist=["Subscriber"]).Subscriber,
                subscriber_id,
            ),
        )

        if not candidate.subscriber:
            return {
                "terminal_status": "aborted",
                "failure_reason": "human_close:subscriber_orm_not_found",
            }

        success = route_candidate(session, candidate)

    return {
        "routed": success,
        "terminal_status": "completed" if success else "failed",
        "failure_reason": None if success else "human_close:slack_post_failed",
    }


def _node_finalize(state: HumanCloseState) -> HumanCloseState:
    from src.agents.tools.write_tools import log_decision

    final_status = state.get("terminal_status") or "completed"
    try:
        log_decision(
            decision_id=state["decision_id"],
            graph_name=GRAPH_NAME,
            subscriber_id=state.get("subscriber_id"),
            event_type=state.get("event_type"),
            terminal_status=final_status,
            tokens_used=0,
            cost_usd=0.0,
            summary={
                "routed": state.get("routed"),
                "failure_reason": state.get("failure_reason"),
            },
        )
    except Exception:
        pass

    if not state.get("terminal_status"):
        return {"terminal_status": "completed"}
    return {}


def build_human_close_graph() -> StateGraph:
    g = StateGraph(HumanCloseState)
    g.add_node("assemble_context", _node_assemble_context)
    g.add_node("route_to_human", _node_route_to_human)
    g.add_node("finalize", _node_finalize)

    g.add_edge(START, "assemble_context")
    g.add_edge("assemble_context", "route_to_human")
    g.add_edge("route_to_human", "finalize")
    g.add_edge("finalize", END)
    return g


def run_human_close_route(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    graph = build_human_close_graph().compile()
    final = graph.invoke({
        "decision_id": decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type": "escalate_to_human_closer",
        "event_payload": event_payload,
    })
    return dict(final)
