"""
Cora's main graph — deterministic routing only, no LLM.

    route (plain dict lookup on event_type)
        target.ready    -> Outreach subgraph
        reply.received  -> Reply subgraph
        call.booked     -> Pre-call subgraph
        call.completed  -> Post-call recap subgraph (THROUGH-v2.2 T3)

Compiled with checkpointer=PostgresSaver, thread_id=opportunity_thread_id
(via src.agents.cora.checkpointer.run_with_checkpoint) — see that module's
docstring for why this reuses the already-migrated LangGraph checkpoint
tables with zero new migration.

Subgraphs are invoked as plain function calls (run_outreach/run_reply/
run_pre_call), not as nested StateGraph composition — mirrors the existing
codebase's own convention (e.g. src/agents/graphs/reactivation.py calling
run_compose_and_send_email(...) as a function from within a node) rather
than introducing a different pattern for Cora alone.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.cora.subgraphs.outreach import run_outreach
from src.agents.cora.subgraphs.post_call_recap import run_post_call_recap
from src.agents.cora.subgraphs.pre_call import run_pre_call
from src.agents.cora.subgraphs.reply import run_reply

logger = logging.getLogger(__name__)


class MainState(TypedDict, total=False):
    event_type: str
    payload: Dict[str, Any]
    result: Dict[str, Any]
    terminal_status: str
    reject_reason: str


# Deterministic dict lookup — the entirety of "routing logic". No LLM.
EVENT_ROUTES: Dict[str, Callable[..., Dict[str, Any]]] = {
    "target.ready": run_outreach,
    "reply.received": run_reply,
    "call.booked": run_pre_call,
    "call.completed": run_post_call_recap,
}


def _node_route(state: MainState) -> MainState:
    # IMPORTANT: graph state flows through the LangGraph Postgres checkpointer
    # when run via run_cora_main() — it must stay msgpack-serializable. A live
    # SQLAlchemy Session is not serializable, so "payload" (part of state)
    # never carries one. It's also not enough to strip `db` from this node's
    # own state alone: LangGraph propagates an ancestor graph's checkpointer
    # to any StateGraph.invoke() called underneath it in the same call stack,
    # so a subgraph's OWN internal state must never carry a Session either.
    # Each subgraph therefore takes `db` as a plain function argument (closed
    # over into its nodes at build time), never as a state field — see
    # subgraphs/outreach.py's build_outreach_graph docstring-comment. Verified
    # directly: a Session anywhere in checkpointed state raises "TypeError:
    # Type is not msgpack serializable: Session" from
    # langgraph.checkpoint.serde.jsonplus at checkpoint-write time.
    event_type = state.get("event_type", "")
    handler = EVENT_ROUTES.get(event_type)
    if handler is None:
        logger.warning("main_graph: unrecognized event_type=%r — no route", event_type)
        return {"terminal_status": "failed", "reject_reason": "unrecognized_event_type"}

    payload = dict(state.get("payload", {}))
    payload.pop("db", None)

    from src.core.database import get_db_context

    with get_db_context() as session:
        result = handler(payload, db=session)

    return {
        "result": result,
        "terminal_status": result.get("terminal_status", "failed"),
        "reject_reason": result.get("reject_reason"),
    }


def build_cora_main_graph() -> StateGraph:
    g = StateGraph(MainState)
    g.add_node("route", _node_route)
    g.add_edge(START, "route")
    g.add_edge("route", END)
    return g


def run_cora_main(event_type: str, payload: Dict[str, Any], thread_id: str) -> Dict[str, Any]:
    """Compiles with the LangGraph Postgres checkpointer, keyed by opportunity_thread_id."""
    from src.agents.cora.checkpointer import run_with_checkpoint

    builder = build_cora_main_graph()
    return run_with_checkpoint(builder, thread_id, {"event_type": event_type, "payload": payload})


def run_cora_main_no_checkpoint(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Checkpointer-free variant for tests/CLI use where a live Postgres checkpoint connection isn't wanted."""
    graph = build_cora_main_graph().compile()
    final = graph.invoke({"event_type": event_type, "payload": payload})
    return dict(final)
