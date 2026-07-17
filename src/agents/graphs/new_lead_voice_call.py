"""
New-Lead Voice Call graph.

Triggered by new_lead_signup event — a brand-new free-signup lead, fired
once at signup time (src/services/signup_engine.py). Distinct from
synthflow_voice_drop.py, which targets EXISTING stalled subscribers
(score>=70, 48h no-convert) on a daily sweep — this graph has no score/dedup
gating since a brand-new lead has no history to gate on, and it does not
touch that graph or its sweep at all.

Flow:
  1. assemble_context — load subscriber, require phone, resolve Synthflow agent
  2. hierarchy_check  — standard decision hierarchy gate (own kill-switch key)
  3. initiate_call    — voice-consent (PEWC) gate + compliance gate + call Synthflow API
  4. finalize         — record terminal status, every path

VOICE CONSENT (ADR 0030 / B0-06): before any dispatch, has_voice_consent()
must confirm a stored PEWC voice-consent record for the subscriber. An
AI-generated Synthflow voice is an "artificial voice" robocall under the TCPA
(47 CFR 64.1200(f)(9)) and needs prior express written consent distinct from
the generic marketing consent. Absent it, the run aborts with
failure_reason="compliance:voice_consent_required" — fail closed.

KNOWN GAP, by design, not an oversight: compliance_gator.validate_outbound()
requires a dnc_phone_checks row (populated by the property-owner
scraping/DNC-scrub pipeline) or it blocks with "dnc_check_required" — a
self-submitted signup phone number never has one, so calling it unmodified
here means every new-lead call is currently blocked at the compliance gate
until a real DNC-check mechanism exists for self-submitted numbers. That
mechanism is out of scope for this task — reusing validate_outbound()
unmodified (same call the existing synthflow_voice_drop graph makes) was a
deliberate choice over building a reduced compliance check, since weakening
a TCPA-relevant gate needs an explicit legal sign-off this task does not
have. Every call will abort with failure_reason="compliance:dnc_check_required"
until that follow-up work lands — this is visible in agent_decisions, not
silent.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import get_subscriber_profile
from src.core.database import get_db_context
from src.services.compliance_gator import has_voice_consent, validate_outbound
from src.services.kill_switch_service import get_cached_metric
from src.services.synthflow_client import initiate_call

logger = logging.getLogger(__name__)

GRAPH_NAME = "new_lead_voice_call"
EVENT_TYPE = "new_lead_signup"
KILL_SWITCH_FEATURE = "speed_to_lead_call"


class NewLeadCallState(TypedDict, total=False):
    decision_id: str
    subscriber_id: int
    event_type: str
    event_payload: dict

    subscriber_profile: dict
    phone: str
    vertical: str
    agent_id: str

    action_allowed: bool
    action_blocked_reason: str
    kill_switch_color: str

    call_id: Optional[str]
    sent: bool
    terminal_status: str
    failure_reason: Optional[str]


def _node_assemble_context(state: NewLeadCallState) -> NewLeadCallState:
    from config.settings import get_settings

    subscriber_id = state.get("subscriber_id")
    if not subscriber_id:
        return {"terminal_status": "aborted", "failure_reason": "new_lead_call:missing_subscriber_id"}
    profile = get_subscriber_profile(subscriber_id)
    if not profile:
        return {"terminal_status": "aborted", "failure_reason": "new_lead_call:subscriber_not_found"}

    phone = profile.get("phone")
    if not phone:
        return {"terminal_status": "aborted", "failure_reason": "new_lead_call:no_phone"}

    event_payload = state.get("event_payload") or {}
    vertical = profile.get("vertical") or event_payload.get("vertical") or "roofing"

    settings = get_settings()
    agent_id = (
        getattr(settings, f"synthflow_outbound_agent_{vertical}", None)
        or settings.synthflow_outbound_agent_roofing
    )
    if not agent_id:
        return {"terminal_status": "aborted", "failure_reason": "new_lead_call:no_agent_configured"}

    return {
        "subscriber_profile": profile,
        "phone": phone,
        "vertical": vertical,
        "agent_id": agent_id,
    }


def _node_hierarchy_check(state: NewLeadCallState) -> NewLeadCallState:
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


def _node_initiate_call(state: NewLeadCallState) -> NewLeadCallState:
    if state.get("terminal_status"):
        return {}

    profile = state.get("subscriber_profile") or {}
    subscriber_id: int = state.get("subscriber_id")
    decision_id: str = state.get("decision_id", "")
    phone: str = state.get("phone", "")
    agent_id: str = state.get("agent_id", "")

    context = {
        "subscriber_id": subscriber_id,
        "subscriber_name": profile.get("name"),
        "vertical": state.get("vertical"),
        "decision_id": decision_id,
        "zip_code": profile.get("territory_zip", ""),
    }

    try:
        with get_db_context() as db:
            # PEWC gate (ADR 0030 / B0-06) — an AI voice call is a robocall
            # under the TCPA and needs prior express written consent, distinct
            # from generic marketing consent. Fail closed when it is absent.
            if not has_voice_consent(subscriber_id, db):
                logger.info(
                    "new_lead_call blocked — no voice consent on file: subscriber=%s",
                    subscriber_id,
                )
                return {
                    "call_id": None,
                    "sent": False,
                    "terminal_status": "aborted",
                    "failure_reason": "compliance:voice_consent_required",
                }

            compliance = validate_outbound(
                phone=phone,
                channel="voice",
                db=db,
                zip_code=profile.get("territory_zip") or None,
            )
    except Exception:
        logger.error("new_lead_call compliance check raised unexpectedly: subscriber=%s", subscriber_id, exc_info=True)
        raise

    if not compliance.allowed:
        logger.info(
            "new_lead_call blocked by compliance gate: subscriber=%s reason=%s",
            subscriber_id, compliance.reason,
        )
        return {
            "call_id": None,
            "sent": False,
            "terminal_status": "aborted",
            "failure_reason": f"compliance:{compliance.reason}",
        }

    call_id = initiate_call(phone=phone, agent_id=agent_id, context=context)
    sent = call_id is not None

    return {
        "call_id": call_id,
        "sent": sent,
        "failure_reason": None if sent else "new_lead_call:initiate_failed",
    }


def _node_finalize(state: NewLeadCallState) -> NewLeadCallState:
    from src.agents.tools.write_tools import log_decision

    final_status = state.get("terminal_status") or ("completed" if state.get("sent") else "failed")

    # No node in this flow logs on the happy path — own the agent_decisions
    # row on EVERY path, same convention as synthflow_voice_drop's finalize.
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
            },
        )
    except Exception:
        logger.warning("new_lead_call log_decision failed sub=%s", state.get("subscriber_id"), exc_info=True)

    return {"terminal_status": final_status}


def build_new_lead_voice_call_graph() -> StateGraph:
    g = StateGraph(NewLeadCallState)
    g.add_node("assemble_context", _node_assemble_context)
    g.add_node("hierarchy_check", _node_hierarchy_check)
    g.add_node("initiate_call", _node_initiate_call)
    g.add_node("finalize", _node_finalize)

    g.add_edge(START, "assemble_context")
    g.add_edge("assemble_context", "hierarchy_check")
    g.add_edge("hierarchy_check", "initiate_call")
    g.add_edge("initiate_call", "finalize")
    g.add_edge("finalize", END)
    return g


def run_new_lead_voice_call(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    graph = build_new_lead_voice_call_graph().compile()
    final = graph.invoke({
        "decision_id": decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type": EVENT_TYPE,
        "event_payload": event_payload,
    })
    return dict(final)
