"""
Synthflow Voice Drop graph.

Triggered by high_intent_no_convert event — subscriber has score≥70,
hasn't converted in 48h, and hasn't received a voice drop in 7 days.

Flow:
  1. assemble_context  — load subscriber + lead context, run qualification checks
  2. hierarchy_check   — standard decision hierarchy gate
  3. initiate_drop     — call Synthflow API, log ManualActionLog row
  4. followup_sms      — send SMS reinforcement within ~60s, fires for ALL
                         eligible outcomes per PDF requirement (voicemail,
                         no_answer, completed, sample/demo_requested). Goes
                         through sms_compliance.send_sms so TCPA opt-out,
                         opt-in, and quiet-hour gates still apply.
  5. finalize          — record terminal status

Voicemail duration (20 seconds, per client PDF) is enforced by the Synthflow
agent script itself, not in this code path. **Duration must be confirmed in
Synthflow agent config** for each `synthflow_outbound_agent_*` env var.
"""

from __future__ import annotations

import uuid
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import get_subscriber_profile
from src.core.database import get_db_context
from src.services.allotment_engine import consume as allotment_consume
from src.services.compliance_gator import validate_outbound
from src.services.synthflow_client import initiate_call
from src.services.kill_switch_service import get_cached_metric

logger = logging.getLogger(__name__)

GRAPH_NAME = "synthflow_voice_drop"
# Gate on lock_conversion: this voice drop is a high-intent conversion-recovery
# call, and lock_conversion's documented remediation is "live-data close, voice
# drop, urgency" (config/cora_guardrails.py) — the same metric fomo gates on.
# The prior value "synthflow_voice_drop" was neither in the KILL_SWITCH config
# nor computed by kill_switch_metric_ingest, so the gate always resolved to
# 'unknown' → fail-safe RED → every dispatch aborted before initiating a call.
KILL_SWITCH_FEATURE = "lock_conversion"
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
    followup_sent: bool
    followup_skipped_reason: str
    terminal_status: str
    failure_reason: Optional[str]


def _node_assemble_context(state: VoiceDropState) -> VoiceDropState:
    from config.settings import get_settings
    from sqlalchemy import text

    subscriber_id: int = state.get("subscriber_id", 0)
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
        "subscriber_id": state.get("subscriber_id", 0),
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

    from src.core.models import ManualActionLog

    profile = state.get("subscriber_profile") or {}
    offer_type = state.get("offer_type", "")
    # get_subscriber_profile serializes created_at to an ISO string; parse it
    # back before arithmetic. Tolerate datetime, ISO string, or missing.
    created_at = profile.get("created_at")
    if isinstance(created_at, str):
        try:
            created_at = datetime.fromisoformat(created_at)
        except ValueError:
            created_at = None
    if created_at is not None and created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    days_on_platform = (
        (datetime.now(timezone.utc) - created_at).days
        if created_at else 0
    )

    subscriber_id: int = state.get("subscriber_id", 0)
    decision_id: str = state.get("decision_id", "")
    phone: str = state.get("phone", "")
    agent_id: str = state.get("agent_id", "")

    context = {
        "subscriber_id": subscriber_id,
        "subscriber_name": profile.get("name"),
        "vertical": state.get("vertical"),
        "decision_id": decision_id,
        # Revenue recovery variables — empty strings when not a recovery call
        "offer_type": offer_type,
        "offer_label": _OFFER_LABELS.get(offer_type, ""),
        "current_plan": profile.get("tier", ""),
        "days_on_platform": str(days_on_platform),
        "zip_code": profile.get("territory_zip", ""),
    }

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

            # Allotment gate — wallet holders are unlimited; free-tier subscribers
            # are capped at 1 voicemail/week. consume() handles both cases.
            if not allotment_consume(subscriber_id, "voicemail", db):
                logger.info(
                    "voice_drop blocked by allotment cap: subscriber=%s",
                    subscriber_id,
                )
                return {
                    "call_id": None,
                    "sent": False,
                    "terminal_status": "aborted",
                    "failure_reason": "allotment:voicemail_weekly_cap",
                }
    except (KeyError, AttributeError):
        # compliance or allotment raised unexpectedly — propagate to LangGraph
        raise

    call_id = initiate_call(
        phone=phone,
        agent_id=agent_id,
        context=context,
    )

    sent = call_id is not None

    if sent:
        try:
            today = date.today()
            week_start = today - timedelta(days=today.weekday())
            with get_db_context() as db:
                log_row = ManualActionLog(
                    subscriber_id=subscriber_id,
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


def _compose_followup_body(profile: dict, vertical: str) -> str:
    """One-liner SMS reinforcement copy. Capped at 160 chars (single segment)."""
    from config.settings import settings

    name = (profile.get("name") or "").strip().split(" ", 1)[0] or "there"
    zip_code = profile.get("territory_zip") or ""
    feed_uuid = profile.get("event_feed_uuid") or ""
    base_url = getattr(settings, "app_base_url", "") or ""
    feed_url = f"{base_url}/dashboard?uuid={feed_uuid}" if feed_uuid and base_url else base_url
    in_zip = f" in {zip_code}" if zip_code else ""
    msg = (
        f"Hey {name}, just left you a voicemail — fresh {vertical} leads"
        f"{in_zip} are piling up. Tap: {feed_url}"
    )
    return msg[:160]


def _node_followup_sms(state: VoiceDropState) -> VoiceDropState:
    """
    Send reinforcement SMS within ~60s of voicemail per PDF requirement.

    Fires for every eligible voice-drop dispatch regardless of post-call
    outcome (voicemail / no_answer / completed / sample_requested / etc.),
    because we don't know the outcome at this point — Synthflow reports it
    later via webhook. Compliance (opt-out, opt-in, quiet hours) is enforced
    inside sms_compliance.send_sms so we don't duplicate those checks here.

    Idempotency: the 7-day dedup in assemble_context blocks re-runs for the
    same subscriber, so a successful path through this node can fire at most
    once per 7d per subscriber.
    """
    if state.get("terminal_status"):
        return {}
    if not state.get("sent"):
        return {"followup_sent": False, "followup_skipped_reason": "voice_drop_not_sent"}
    if state.get("kill_switch_color") == "red":
        return {"followup_sent": False, "followup_skipped_reason": "kill_switch_red"}

    profile = state.get("subscriber_profile") or {}
    phone = profile.get("phone") or state.get("phone")
    if not phone:
        return {"followup_sent": False, "followup_skipped_reason": "no_phone"}

    body = _compose_followup_body(profile, state.get("vertical") or "roofing")

    try:
        from src.services.sms_compliance import send_sms

        with get_db_context() as db:
            ok = send_sms(
                to=phone,
                body=body,
                db=db,
                message_type="marketing",
                subscriber_id=state.get("subscriber_id"),
                task_type="synthflow_voice_drop_followup",
                campaign=GRAPH_NAME,
                decision_id=state.get("decision_id"),
            )
    except Exception as exc:
        logger.warning("voice_drop followup_sms failed sub=%s: %s", state.get("subscriber_id"), exc)
        return {"followup_sent": False, "followup_skipped_reason": f"error:{exc.__class__.__name__}"}

    if not ok:
        return {"followup_sent": False, "followup_skipped_reason": "compliance_suppressed"}
    return {"followup_sent": True}


def _node_finalize(state: VoiceDropState) -> VoiceDropState:
    from src.agents.tools.write_tools import log_decision

    final_status = state.get("terminal_status") or ("completed" if state.get("sent") else "failed")

    # Unlike the other Cora graphs, this flow has no compose_and_send node that
    # logs on the happy path — so we own the agent_decisions row on EVERY path.
    # Without it there is no audit trail and the DoD SLA query (agent_decisions
    # → synthflow_calls) can't see the dispatch.
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
        logger.warning("voice_drop log_decision failed sub=%s: %s",
                       state.get("subscriber_id"), exc)

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
