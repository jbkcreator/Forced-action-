"""
Command Center LangGraph graph.

Standalone graph — completely separate from the existing Cora main_graph.
No shared state, no shared checkpointer, no shared worker.

    START
      ↓
    guard_node     ← haiku intent check + injection detection
      ↓ (blocked)
      ↓ (valid)
    loop_node      ← multi-turn agentic tool-use loop
      ↓
    persist_node   ← write answer to cc_answers, touch session
      ↓
    emit_node      ← post answer back to Slack (stub until Slack is wired)
      ↓
    END

No checkpointer — conversation history is managed explicitly through the
cc_messages table and loaded into state at the start of each request.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.cora.command_center.guard import _make_node_guard
from src.agents.cora.command_center.loop import _make_node_loop

logger = logging.getLogger(__name__)


class CommandCenterState(TypedDict, total=False):
    # ── Input ────────────────────────────────────────────────────────────────
    session_id: str
    question: str
    slack_user_id: Optional[str]
    slack_channel: Optional[str]
    slack_thread_ts: Optional[str]

    # ── Conversation history (loaded from cc_messages before graph entry) ───
    messages: List[Dict[str, Any]]
    history_length: int    # len(messages) before this request's new turns
    turn_number: int
    total_tokens: int

    # ── Guard outcome ─────────────────────────────────────────────────────────
    blocked: bool
    block_reason: Optional[str]

    # ── Loop / answer ─────────────────────────────────────────────────────────
    answer: Optional[str]
    answer_id: Optional[int]

    # ── LangGraph standard ────────────────────────────────────────────────────
    terminal_status: str
    reject_reason: Optional[str]

    # ── Streaming UX ──────────────────────────────────────────────────────────
    placeholder_ts: Optional[str]   # ts of the "thinking..." message to edit

    # ── Profiling ─────────────────────────────────────────────────────────────
    _timings: Optional[Dict[str, int]]
    _cost_usd: Optional[float]

    # ── Runtime (not serialized — no checkpointer) ────────────────────────────
    _db: Optional[Any]   # SQLAlchemy Session passed through state so the graph
                         # can be compiled once and reused across requests


def _node_persist(state: CommandCenterState) -> CommandCenterState:
    from src.agents.cora.command_center import store

    session_id = state.get("session_id", "")
    answer = state.get("answer") or ""
    question = state.get("question", "")
    db = state.get("_db")

    if not session_id or not answer:
        return {"terminal_status": "completed"}

    answer_id: Optional[int] = None
    if db is not None:
        try:
            answer_id = store.write_answer(db, session_id, question, answer, status="pending")
            store.touch_session(db, session_id)
        except Exception as exc:
            logger.warning("persist: failed to write answer for session=%s: %s", session_id, exc)

    return {
        "answer_id": answer_id,
        "terminal_status": "completed",
    }


_CC_FALLBACK_CHANNEL = "C0BLD6BG6TS"  # shared test channel (relay/lifecycle/learning-hygiene)


def _node_emit(state: CommandCenterState) -> CommandCenterState:
    """Deliver the answer back to the user via Slack."""
    answer = state.get("answer") or ""
    answer_id = state.get("answer_id")
    slack_channel = state.get("slack_channel") or _CC_FALLBACK_CHANNEL
    slack_thread_ts = state.get("slack_thread_ts")
    db = state.get("_db")

    if not answer:
        return {}

    logger.info(
        "command_center.emit: session=%s channel=%s thread=%s answer=%r",
        state.get("session_id"), slack_channel, slack_thread_ts, answer[:80],
    )

    try:
        from config.settings import get_settings
        from slack_sdk import WebClient

        settings = get_settings()
        token = settings.slack_bot_token
        if token:
            client = WebClient(token=token.get_secret_value())
            placeholder_ts = state.get("placeholder_ts")
            if placeholder_ts:
                client.chat_update(
                    channel=slack_channel,
                    ts=placeholder_ts,
                    text=answer,
                )
                logger.info("command_center.emit: updated placeholder ts=%s channel=%s", placeholder_ts, slack_channel)
            else:
                kwargs: dict = {"channel": slack_channel, "text": answer}
                if slack_thread_ts:
                    kwargs["thread_ts"] = slack_thread_ts
                client.chat_postMessage(**kwargs)
                logger.info("command_center.emit: posted to channel=%s", slack_channel)
        else:
            logger.warning("command_center.emit: SLACK_BOT_TOKEN not set — answer not posted")
    except Exception as exc:
        logger.warning("command_center.emit: Slack post failed: %s", exc)

    if db is not None and answer_id is not None:
        from src.agents.cora.command_center import store
        try:
            store.mark_answer_delivered(db, answer_id)
        except Exception as exc:
            logger.warning("emit: failed to mark answer delivered: %s", exc)

    return {}


def _after_guard(state: CommandCenterState) -> str:
    return "emit" if state.get("blocked") else "loop"


def _timed(name: str, fn):
    """Wrap a graph node to log wall-clock duration and accumulated cost."""
    import time

    def _wrapper(state):
        t0 = time.monotonic()
        result = fn(state)
        ms = int((time.monotonic() - t0) * 1000)
        session = state.get("session_id", "?")
        out = result or {}
        # Accumulate cost across nodes (guard sets it, loop adds to it).
        prior_cost = float(state.get("_cost_usd") or 0)
        node_cost = float(out.get("_cost_usd") or 0)
        # If the node returned a cost that's already cumulative (loop), keep it;
        # otherwise add the node's incremental cost to the running total.
        cumulative = node_cost if node_cost >= prior_cost else prior_cost + node_cost
        out["_cost_usd"] = cumulative
        timings = dict(state.get("_timings") or {})
        timings[name] = ms
        out["_timings"] = timings
        logger.info(
            "PROFILE node=%-8s session=%s duration_ms=%d cost_usd=%.6f",
            name, session, ms, cumulative,
        )
        return out

    return _wrapper


def _build_graph() -> StateGraph:
    g = StateGraph(CommandCenterState)
    g.add_node("guard",   _timed("guard",   _make_node_guard()))
    g.add_node("loop",    _timed("loop",    _make_node_loop()))
    g.add_node("persist", _timed("persist", _node_persist))
    g.add_node("emit",    _timed("emit",    _node_emit))
    g.add_edge(START, "guard")
    g.add_conditional_edges("guard", _after_guard, {"loop": "loop", "emit": "emit"})
    g.add_edge("loop", "persist")
    g.add_edge("persist", "emit")
    g.add_edge("emit", END)
    return g


# Compiled once at first use — reused for every subsequent request.
_GRAPH_LOCK = threading.Lock()
_COMPILED_GRAPH = None


def _get_compiled_graph():
    global _COMPILED_GRAPH
    if _COMPILED_GRAPH is None:
        with _GRAPH_LOCK:
            if _COMPILED_GRAPH is None:
                logger.info("command_center: compiling graph (one-time)")
                _COMPILED_GRAPH = _build_graph().compile()
    return _COMPILED_GRAPH


def run_command_center(
    payload: Dict[str, Any],
    db=None,
) -> Dict[str, Any]:
    """
    Entry point called by the Command Center worker.

    Loads session message history from DB, runs the graph, returns final state.
    """
    from src.agents.cora.command_center import store

    session_id: str = payload["session_id"]
    question: str = payload.get("question", "").strip()

    # Ensure session row exists.
    if db is not None and not store.session_exists(db, session_id):
        store.create_session(
            db, session_id,
            slack_user_id=payload.get("slack_user_id"),
            slack_channel=payload.get("slack_channel"),
            slack_thread_ts=payload.get("slack_thread_ts"),
        )

    # Load prior conversation history.
    history: List[Dict[str, Any]] = []
    if db is not None:
        try:
            history = store.load_session_messages(db, session_id, limit=40)
        except Exception as exc:
            logger.warning("run_command_center: failed to load history: %s", exc)

    initial_state: CommandCenterState = {
        "session_id": session_id,
        "question": question,
        "slack_user_id": payload.get("slack_user_id"),
        "slack_channel": payload.get("slack_channel"),
        "slack_thread_ts": payload.get("slack_thread_ts"),
        "placeholder_ts": payload.get("placeholder_ts"),
        "messages": history,
        "history_length": len(history),
        "turn_number": len(history) // 2,
        "total_tokens": 0,
        "blocked": False,
        "block_reason": None,
        "answer": None,
        "answer_id": None,
        "terminal_status": "failed",
        "reject_reason": None,
        "_db": db,
    }

    final = _get_compiled_graph().invoke(initial_state)
    return dict(final)
