"""
Synthflow Voice Drop graph.

Triggered by high_intent_no_convert event — subscriber has score≥70,
hasn't converted in 48h, and hasn't received a voice drop in 7 days.

Flow:
  1. assemble_context  — load subscriber + lead context, run qualification checks
  2. hierarchy_check   — standard decision hierarchy gate
  3. initiate_drop     — call Synthflow API, log ManualActionLog row
  4. finalize          — record terminal status
"""

from __future__ import annotations

import uuid
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import get_subscriber_profile

logger = logging.getLogger(__name__)

GRAPH_NAME = "synthflow_voice_drop"
KILL_SWITCH_FEATURE = "synthflow_voice_drop"
_VOICE_DROP_ACTION_TYPE = "voice_drop"

_OFFER_LABELS: Dict[str, str] = {
    "annual_lock":        "Charter Annual Plan",
    "territory_lock":     "Territory Lock",
    "data_only":          "Data-Only Plan",
    "autopilot_upgrade":  "Autopilot Upgrade",
}


class VoiceDropState(TypedDict, total=False):
    decision_id: str
    subscriber_id: int
    event_type: str
    event_payload: dict

    subscriber_profile: dict
    phone: str
    vertical: str
    agent_id: str
    offer_type: str

    action_allowed: bool
    action_blocked_reason: str
    kill_switch_color: str

    call_id: Optional[str]
    sent: bool
    terminal_status: str
    failure_reason: str


def _node_assemble_context(state: VoiceDropState) -> VoiceDropState:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.core.models import Subscriber, ManualActionLog
    from sqlalchemy import text

    subscriber_id = state["subscriber_id"]
    profile = get_subscriber_profile(subscriber_id)
    if not profile:
        return {"terminal_status": "aborted", "failure_reason": "voice_drop:subscriber_not_found"}

    phone = profile.get("phone")
    if not phone:
        return {"terminal_status": "aborted", "failure_reason": "voice_drop:no_phone"}

    event_payload = state.get("event_payload") or {}
    vertical = profile.get("vertical") or event_payload.get("vertical") or "roofing"
    offer_type = event_payload.get("offer_type", "")

    settings = get_settings()
    # Revenue recovery calls use a dedicated agent; fall back to vertical-specific then roofing
    if offer_type and settings.synthflow_outbound_agent_revenue_recovery:
        agent_id = settings.synthflow_outbound_agent_revenue_recovery
    else:
        agent_id = (
            getattr(settings, f"synthflow_outbound_agent_{vertical}", None)
            or settings.synthflow_outbound_agent_roofing
        )
    if not agent_id:
        return {"terminal_status": "aborted", "failure_reason": "voice_drop:no_agent_configured"}

    # Dedup: skip if voice drop logged within last 7 days
    try:
        with get_db_context() as db:
            from sqlalchemy import select
            cutoff = datetime.now(timezone.utc).timestamp() - 7 * 86400
            recent = db.execute(
                text(
                    "SELECT 1 FROM manual_action_log "
                    "WHERE subscriber_id = :sid AND action_type = 'voice_drop' "
                    "AND created_at > to_timestamp(:cutoff) LIMIT 1"
                ),
                {"sid": subscriber_id, "cutoff": cutoff},
            ).first()
        if recent:
            return {"terminal_status": "aborted", "failure_reason": "voice_drop:dedup_7d"}
    except Exception as exc:
        logger.warning("voice_drop dedup check failed: %s", exc)

    return {
        "subscriber_profile": profile,
        "phone": phone,
        "vertical": vertical,
        "agent_id": agent_id,
        "offer_type": offer_type,
    }


def _node_hierarchy_check(state: VoiceDropState) -> VoiceDropState:
    if state.get("terminal_status"):
        return {}

    hierarchy = run_decision_hierarchy({
        "subscriber_id": state["subscriber_id"],
        "graph_name": GRAPH_NAME,
        "kill_switch_feature": KILL_SWITCH_FEATURE,
        "learning_card_type": "call_perf",
    })

    if not hierarchy.get("action_allowed", True):
        return {
            "action_allowed": False,
            "action_blocked_reason": hierarchy.get("action_blocked_reason", "unknown"),
            "kill_switch_color": hierarchy.get("kill_switch_color"),
            "terminal_status": "aborted",
            "failure_reason": hierarchy.get("action_blocked_reason", "hierarchy_blocked"),
        }

    return {
        "action_allowed": True,
        "kill_switch_color": hierarchy.get("kill_switch_color"),
    }


def _node_initiate_drop(state: VoiceDropState) -> VoiceDropState:
    if state.get("terminal_status"):
        return {}

    from src.services.synthflow_client import initiate_call
    from src.core.database import get_db_context
    from src.core.models import ManualActionLog

    profile = state.get("subscriber_profile") or {}
    offer_type = state.get("offer_type", "")
    created_at = profile.get("created_at")
    days_on_platform = (
        (datetime.now(timezone.utc) - created_at).days
        if created_at else 0
    )

    context = {
        "subscriber_id": state["subscriber_id"],
        "subscriber_name": profile.get("name"),
        "vertical": state.get("vertical"),
        "decision_id": state["decision_id"],
        # Revenue recovery variables — empty strings when not a recovery call
        "offer_type": offer_type,
        "offer_label": _OFFER_LABELS.get(offer_type, ""),
        "current_plan": profile.get("tier", ""),
        "days_on_platform": str(days_on_platform),
        "zip_code": profile.get("territory_zip", ""),
    }

    call_id = initiate_call(
        phone=state["phone"],
        agent_id=state["agent_id"],
        context=context,
    )

    sent = call_id is not None

    if sent:
        try:
            today = date.today()
            week_start = today - timedelta(days=today.weekday())
            with get_db_context() as db:
                log_row = ManualActionLog(
                    subscriber_id=state["subscriber_id"],
                    action_type=_VOICE_DROP_ACTION_TYPE,
                    week_start=week_start,
                )
                db.add(log_row)
                db.commit()
        except Exception as exc:
            logger.warning("voice_drop log write failed: %s", exc)

    return {
        "call_id": call_id,
        "sent": sent,
        "failure_reason": None if sent else "voice_drop:initiate_failed",
    }


def _node_finalize(state: VoiceDropState) -> VoiceDropState:
    final_status = state.get("terminal_status") or ("completed" if state.get("sent") else "failed")
    return {"terminal_status": final_status}


def build_synthflow_voice_drop_graph() -> StateGraph:
    g = StateGraph(VoiceDropState)
    g.add_node("assemble_context", _node_assemble_context)
    g.add_node("hierarchy_check", _node_hierarchy_check)
    g.add_node("initiate_drop", _node_initiate_drop)
    g.add_node("finalize", _node_finalize)

    g.add_edge(START, "assemble_context")
    g.add_edge("assemble_context", "hierarchy_check")
    g.add_edge("hierarchy_check", "initiate_drop")
    g.add_edge("initiate_drop", "finalize")
    g.add_edge("finalize", END)
    return g


def run_synthflow_voice_drop(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    graph = build_synthflow_voice_drop_graph().compile()
    final = graph.invoke({
        "decision_id": decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type": "high_intent_no_convert",
        "event_payload": event_payload,
    })
    return dict(final)
