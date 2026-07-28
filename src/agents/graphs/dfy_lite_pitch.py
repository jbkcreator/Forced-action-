"""
DFY-Lite Pitch graph — Lifecycle runtime fulfillment for subscriber-triggered pitch generation.

Triggered by : dfy_lite_pitch_requested
Event payload : {"order_id": <int>}  — the dfy_lite_orders row to process

Flow:
  setup → check_kill_switch → compile_signals → generate_pitch → finalize
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import sqlalchemy as sa
from langgraph.graph import END, START, StateGraph
from typing import TypedDict

from src.agents.pitch_builder import (
    build_property_pitch_context,
    generate_pitch_with_claude,
)
from src.agents.tools.write_tools import log_decision
from src.core.database import db
from src.services.kill_switch_service import get_cached_metric, get_kill_switch_status

logger = logging.getLogger(__name__)

GRAPH_NAME = "dfy_lite_pitch"
KILL_SWITCH_FEATURE = "dfy_lite_pitch"


class DfyLitePitchState(TypedDict, total=False):
    # Standard Lifecycle fields
    decision_id: str
    subscriber_id: int
    event_type: str
    event_payload: dict

    # Resolved from DB order row in setup
    order_id: int
    property_id: int
    request_options: dict

    # Kill switch outcome
    kill_switch_color: str
    action_allowed: bool
    action_blocked_reason: str

    # Intermediate results
    pitch_context: dict
    generated_outputs: dict

    # Cost accumulation
    tokens_used: int
    cost_usd: float

    # Terminal
    terminal_status: str
    failure_reason: str


# ── Nodes ──────────────────────────────────────────────────────────────────────

def _node_setup(state: DfyLitePitchState) -> Dict[str, Any]:
    """Extract order_id from event payload and load order details from DB."""
    payload = state.get("event_payload") or {}
    order_id = payload.get("order_id")

    if not order_id:
        return {
            "terminal_status": "aborted",
            "failure_reason": "dfy_lite_pitch:missing_order_id",
        }

    try:
        with db.session_scope() as session:
            row = session.execute(
                sa.text("""
                    SELECT
                        id, subscriber_id, property_id, pitch_type, offer_angle,
                        target_vertical, selected_output_formats, custom_instructions
                    FROM dfy_lite_orders
                    WHERE id = :order_id
                """),
                {"order_id": order_id},
            ).mappings().first()

        if row is None:
            return {
                "terminal_status": "aborted",
                "failure_reason": f"dfy_lite_pitch:order_{order_id}_not_found",
            }

        if row["subscriber_id"] != state["subscriber_id"]:
            return {
                "terminal_status": "aborted",
                "failure_reason": f"dfy_lite_pitch:order_{order_id}_subscriber_mismatch",
            }

        request_options = {
            "property_id":              row["property_id"],
            "target_vertical":          row["target_vertical"],
            "pitch_type":               row["pitch_type"],
            "offer_angle":              row["offer_angle"],
            "selected_output_formats":  row["selected_output_formats"] or [],
            "custom_instructions":      row["custom_instructions"],
        }

        return {
            "order_id":       order_id,
            "property_id":    row["property_id"],
            "request_options": request_options,
            "tokens_used":    0,
            "cost_usd":       0.0,
        }

    except Exception as exc:
        logger.error("dfy_lite_pitch setup failed for order %s: %s", order_id, exc)
        return {
            "terminal_status": "failed",
            "failure_reason":  f"dfy_lite_pitch:setup_error:{exc}",
        }


def _node_check_kill_switch(state: DfyLitePitchState) -> Dict[str, Any]:
    if state.get("terminal_status"):
        return {}

    observed = get_cached_metric(KILL_SWITCH_FEATURE)
    ks = get_kill_switch_status(KILL_SWITCH_FEATURE, observed)
    color = ks.get("color", "green")

    if color == "red":
        _mark_order(state["order_id"], "Cancelled", "Kill switch is red — pitch generation blocked")
        return {
            "kill_switch_color":    color,
            "action_allowed":       False,
            "action_blocked_reason": "kill_switch_red",
            "terminal_status":      "aborted",
            "failure_reason":       "dfy_lite_pitch:kill_switch_red",
        }

    return {
        "kill_switch_color": color,
        "action_allowed":    True,
    }


def _node_compile_signals(state: DfyLitePitchState) -> Dict[str, Any]:
    if state.get("terminal_status"):
        return {}

    order_id = state["order_id"]
    property_id = state["property_id"]

    try:
        with db.session_scope() as session:
            context = build_property_pitch_context(session, property_id)
            property_snap = {k: v for k, v in (context.get("property") or {}).items() if v is not None}
            now = datetime.now(timezone.utc)
            session.execute(
                sa.text("""
                    UPDATE dfy_lite_orders
                    SET status                 = 'Signal_Compiled',
                        distress_stack_json    = CAST(:stack AS JSONB),
                        property_snapshot_json = CAST(:snap AS JSONB),
                        updated_at             = :now
                    WHERE id = :order_id
                """),
                {
                    "stack":    json.dumps(context, default=str),
                    "snap":     json.dumps(property_snap, default=str),
                    "now":      now,
                    "order_id": order_id,
                },
            )
            session.commit()

        return {"pitch_context": context}

    except Exception as exc:
        logger.warning("DFY-Lite signal compilation failed for order %s: %s", order_id, exc)
        _mark_order(order_id, "Signal_Failed", str(exc)[:1000])
        return {
            "terminal_status": "failed",
            "failure_reason":  f"dfy_lite_pitch:signal_failed:{exc}",
        }


def _node_generate_pitch(state: DfyLitePitchState) -> Dict[str, Any]:
    if state.get("terminal_status"):
        return {}

    order_id       = state["order_id"]
    context        = state.get("pitch_context") or {}
    request_options = state.get("request_options") or {}
    subscriber_id  = state["subscriber_id"]

    try:
        with db.session_scope() as session:
            generated = generate_pitch_with_claude(
                context=context,
                request_options=request_options,
                subscriber_id=subscriber_id,
                db=session,
            )
            now = datetime.now(timezone.utc)
            session.execute(
                sa.text("""
                    UPDATE dfy_lite_orders
                    SET status                 = 'Needs_Review',
                        generated_outputs_json = CAST(:outputs AS JSONB),
                        updated_at             = :now
                    WHERE id = :order_id
                """),
                {
                    "outputs":  json.dumps(generated, default=str),
                    "now":      now,
                    "order_id": order_id,
                },
            )
            session.commit()

        meta = generated.get("metadata") or {}
        return {
            "generated_outputs": generated,
            "tokens_used": (
                int(meta.get("input_tokens", 0) or 0)
                + int(meta.get("output_tokens", 0) or 0)
            ),
            "cost_usd": float(meta.get("cost_usd", 0.0) or 0.0),
        }

    except Exception as exc:
        logger.warning("DFY-Lite pitch generation failed for order %s: %s", order_id, exc)
        _mark_order(order_id, "Pitch_Failed", str(exc)[:1000])
        return {
            "terminal_status": "failed",
            "failure_reason":  f"dfy_lite_pitch:pitch_failed:{exc}",
        }


def _node_finalize(state: DfyLitePitchState) -> Dict[str, Any]:
    final_status = state.get("terminal_status") or "completed"

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
                "order_id":          state.get("order_id"),
                "property_id":       state.get("property_id"),
                "failure_reason":    state.get("failure_reason"),
                "kill_switch_color": state.get("kill_switch_color"),
            },
        )
    except Exception:
        pass

    if not state.get("terminal_status"):
        return {"terminal_status": "completed"}
    return {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mark_order(order_id: int, status: str, reason: str) -> None:
    """Best-effort status update — swallows errors so they don't mask the original failure."""
    try:
        with db.session_scope() as session:
            session.execute(
                sa.text("""
                    UPDATE dfy_lite_orders
                    SET status       = :status,
                        error_reason = :reason,
                        updated_at   = :now
                    WHERE id = :order_id
                """),
                {
                    "status":   status,
                    "reason":   reason[:1000],
                    "now":      datetime.now(timezone.utc),
                    "order_id": order_id,
                },
            )
            session.commit()
    except Exception as exc:
        logger.error("_mark_order: failed to set order %s → %s: %s", order_id, status, exc)


# ── Graph assembly ────────────────────────────────────────────────────────────

def _build_graph() -> StateGraph:
    g = StateGraph(DfyLitePitchState)
    g.add_node("setup",              _node_setup)
    g.add_node("check_kill_switch",  _node_check_kill_switch)
    g.add_node("compile_signals",    _node_compile_signals)
    g.add_node("generate_pitch",     _node_generate_pitch)
    g.add_node("finalize",           _node_finalize)

    g.add_edge(START,             "setup")
    g.add_edge("setup",           "check_kill_switch")
    g.add_edge("check_kill_switch", "compile_signals")
    g.add_edge("compile_signals", "generate_pitch")
    g.add_edge("generate_pitch",  "finalize")
    g.add_edge("finalize",        END)
    return g


def run_dfy_lite_pitch(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Router entry point — builds and invokes the DFY-Lite pitch graph."""
    graph = _build_graph().compile()
    final = graph.invoke({
        "decision_id":   decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type":    "dfy_lite_pitch_requested",
        "event_payload": event_payload,
    })
    return dict(final)
