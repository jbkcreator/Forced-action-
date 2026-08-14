"""
Synthflow Voice Drop graph.

Triggered by high_intent_no_convert event - subscriber has score>=70,
hasn't converted in 48h, and hasn't received a voice drop in 7 days.

Flow:
  1. assemble_context  - load subscriber + lead context, run qualification checks
  2. hierarchy_check   - standard decision hierarchy gate
  3. initiate_drop     - block AI cold dial, route to human queue in GHL
  4. followup_sms      - skipped because no AI dispatch occurred
  5. finalize          - record terminal status
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import get_subscriber_profile
from src.core.database import get_db_context
from src.services.compliance_gator import has_voice_consent, validate_outbound
from src.services.kill_switch_service import get_cached_metric

logger = logging.getLogger(__name__)

GRAPH_NAME = "synthflow_voice_drop"
KILL_SWITCH_FEATURE = "lock_conversion"


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
    followup_sent: bool
    followup_skipped_reason: str
    terminal_status: str
    failure_reason: Optional[str]


def _node_assemble_context(state: VoiceDropState) -> VoiceDropState:
    from config.settings import get_settings
    from sqlalchemy import text

    subscriber_id = state.get("subscriber_id")
    if not subscriber_id:
        return {"terminal_status": "aborted", "failure_reason": "voice_drop:missing_subscriber_id"}
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
    if offer_type and settings.synthflow_outbound_agent_revenue_recovery:
        agent_id = settings.synthflow_outbound_agent_revenue_recovery
    else:
        agent_id = (
            getattr(settings, f"synthflow_outbound_agent_{vertical}", None)
            or settings.synthflow_outbound_agent_roofing
        )
    if not agent_id:
        return {"terminal_status": "aborted", "failure_reason": "voice_drop:no_agent_configured"}

    try:
        with get_db_context() as db:
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
        "subscriber_id": state.get("subscriber_id"),
        "graph_name": GRAPH_NAME,
        "kill_switch_feature": KILL_SWITCH_FEATURE,
        "kill_switch_observed_value": get_cached_metric(KILL_SWITCH_FEATURE),
        "learning_card_type": "call_perf",
    })

    if not hierarchy.get("action_allowed", True):
        return {
            "action_allowed": False,
            "action_blocked_reason": hierarchy.get("action_blocked_reason", "unknown"),
            "kill_switch_color": hierarchy.get("kill_switch_color") or "",
            "terminal_status": "aborted",
            "failure_reason": hierarchy.get("action_blocked_reason", "hierarchy_blocked"),
        }

    return {
        "action_allowed": True,
        "kill_switch_color": hierarchy.get("kill_switch_color") or "",
    }


def _node_initiate_drop(state: VoiceDropState) -> VoiceDropState:
    if state.get("terminal_status"):
        return {}

    from src.services.synthflow_service import _apply_tags_to_contact

    profile = state.get("subscriber_profile") or {}
    subscriber_id: int = state.get("subscriber_id")
    phone: str = state.get("phone", "")
    ghl_contact_id = profile.get("ghl_contact_id")

    try:
        with get_db_context() as db:
            compliance = validate_outbound(
                phone=phone,
                channel="voice",
                db=db,
                zip_code=profile.get("territory_zip") or None,
            )
            if not compliance.allowed:
                logger.info(
                    "voice_drop blocked by compliance gate: subscriber=%s reason=%s",
                    subscriber_id,
                    compliance.reason,
                )
                return {
                    "call_id": None,
                    "sent": False,
                    "terminal_status": "aborted",
                    "failure_reason": f"compliance:{compliance.reason}",
                }

            if not has_voice_consent(subscriber_id, db):
                logger.info(
                    "voice_drop blocked by missing voice consent: subscriber=%s",
                    subscriber_id,
                )
                return {
                    "call_id": None,
                    "sent": False,
                    "terminal_status": "aborted",
                    "failure_reason": "voice_consent_required",
                }

            if ghl_contact_id:
                _apply_tags_to_contact(ghl_contact_id, ["cold_dial_human_required"])
            else:
                logger.warning(
                    "voice_drop missing ghl_contact_id for subscriber=%s; human queue tag skipped",
                    subscriber_id,
                )
    except (KeyError, AttributeError):
        raise

    return {
        "call_id": None,
        "sent": False,
        "terminal_status": "aborted",
        "failure_reason": "compliance:cold_dial_human_only",
    }


def _node_followup_sms(state: VoiceDropState) -> VoiceDropState:
    if state.get("terminal_status"):
        return {}
    if not state.get("sent"):
        return {"followup_sent": False, "followup_skipped_reason": "voice_drop_not_sent"}
    return {"followup_sent": False, "followup_skipped_reason": "voice_drop_human_routed"}


def _node_finalize(state: VoiceDropState) -> VoiceDropState:
    from src.agents.tools.write_tools import log_decision

    final_status = state.get("terminal_status") or ("completed" if state.get("sent") else "failed")

    try:
        log_decision(
            decision_id=state.get("decision_id", ""),
            graph_name=GRAPH_NAME,
            subscriber_id=state.get("subscriber_id"),
            event_type=state.get("event_type"),
            terminal_status=final_status,
            summary={
                "sent": bool(state.get("sent")),
                "call_id": state.get("call_id"),
                "kill_switch_color": state.get("kill_switch_color"),
                "failure_reason": state.get("failure_reason"),
                "followup_sent": state.get("followup_sent"),
            },
        )
    except Exception as exc:
        logger.warning("voice_drop log_decision failed sub=%s: %s", state.get("subscriber_id"), exc)

    return {"terminal_status": final_status}


def build_synthflow_voice_drop_graph() -> StateGraph:
    g = StateGraph(VoiceDropState)
    g.add_node("assemble_context", _node_assemble_context)
    g.add_node("hierarchy_check", _node_hierarchy_check)
    g.add_node("initiate_drop", _node_initiate_drop)
    g.add_node("followup_sms", _node_followup_sms)
    g.add_node("finalize", _node_finalize)

    g.add_edge(START, "assemble_context")
    g.add_edge("assemble_context", "hierarchy_check")
    g.add_edge("hierarchy_check", "initiate_drop")
    g.add_edge("initiate_drop", "followup_sms")
    g.add_edge("followup_sms", "finalize")
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
