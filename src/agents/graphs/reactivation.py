"""
Reactivation graph — Sprint S0 dormant/churned subscriber outreach.

Triggered when reactivation_scheduler publishes a reactivation_outreach event
after identifying an eligible subscriber via geo + lifecycle gates.

Event envelope:
    {
        "event_type": "reactivation_outreach",
        "subscriber_id": 123,
        "payload": {
            "cohort":    "county_live" | "sold_out",
            "county_id": "hillsborough",
            "zip_code":  "33601",       # sold_out only
            "vertical":  "roofing",     # sold_out only
        },
    }

Flow (5 nodes):
    1. assemble_context     — load subscriber profile, determine channel (sms/email)
    2. hierarchy_check      — kill switch, A/B, guardrails
    3. build_compose_context — render prompts from cohort + subscriber context
    4. compose_and_send     — SMS path (run_compose_and_send) or
                              email path (run_compose_and_send_email)
    5. finalize             — stamp last_reactivation_attempt_at, audit log on abort
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy import text

from src.agents.subgraphs.compose_and_send import run_compose_and_send
from src.agents.subgraphs.compose_and_send_email import run_compose_and_send_email
from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import get_segment_and_score, get_subscriber_profile
from src.agents.tools.write_tools import log_decision
from src.core.database import db
from src.services.feedback_ritual import publish_feedback_ritual_candidate
from src.services.kill_switch_service import get_cached_metric

logger = logging.getLogger(__name__)

GRAPH_NAME = "reactivation"
CAMPAIGN_COUNTY_LIVE = "reactivation_county_live"
CAMPAIGN_SOLD_OUT = "reactivation_sold_out_zip"
CLAUDE_TASK_TYPE = "sms_copy"
KILL_SWITCH_FEATURE = "reactivation"


class ReactivationState(TypedDict, total=False):
    # ── Inputs (from event envelope) ─────────────────────────────────────────
    decision_id: str
    subscriber_id: int
    event_type: str
    event_payload: dict

    # ── Assembled context ─────────────────────────────────────────────────────
    subscriber_profile: dict
    segment_data: dict

    # ── Hierarchy outputs ─────────────────────────────────────────────────────
    action_allowed: bool
    action_blocked_reason: str
    use_fallback: bool
    kill_switch_color: str
    revenue_signal_score: int

    # ── Intermediate compose inputs (must be declared — LangGraph drops undeclared keys) ──
    _channel: str              # 'sms' | 'email'
    _campaign: str
    _recipient_email: str      # email channel only
    _subject: str              # email channel only
    _system_prompt: str
    _user_prompt: str
    _fallback_body: str
    _render_context: dict
    _variant_id: Optional[str]

    # ── Compose/send outputs ──────────────────────────────────────────────────
    message_body: str
    sent: bool
    send_reason: str
    message_outcome_id: Optional[int]
    tokens_used: int
    cost_usd: float
    terminal_status: str
    failure_reason: str


def _build_review_capture(state: ReactivationState) -> dict:
    payload = state.get("event_payload") or {}
    cohort = payload.get("cohort", "county_live")
    county_id = payload.get("county_id") or ""
    zip_code = payload.get("zip_code") or ""

    raw_input_text = f"reactivation_outreach cohort={cohort} county_id={county_id}"
    if zip_code:
        raw_input_text = f"{raw_input_text} zip_code={zip_code}"

    return {
        "raw_input_text": raw_input_text,
        "generated_output_text": state.get("message_body") or "",
        "confidence_score": None,
        "confidence_reason": None,
        "review_flag": True,
        "review_flag_reason": state.get("failure_reason") or "reactivation_early_abort",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Nodes
# ─────────────────────────────────────────────────────────────────────────────

def _node_assemble_context(state: ReactivationState) -> ReactivationState:
    subscriber_id = state["subscriber_id"]

    profile = get_subscriber_profile(subscriber_id)
    if not profile:
        return {
            "terminal_status": "aborted",
            "failure_reason": "reactivation:subscriber_not_found",
        }

    if not profile.get("phone") and not profile.get("email"):
        return {
            "subscriber_profile": profile,
            "terminal_status": "aborted",
            "failure_reason": "reactivation:no_contact_info",
        }

    segment_data = get_segment_and_score(subscriber_id)
    channel = "sms" if profile.get("phone") else "email"

    return {
        "subscriber_profile": profile,
        "segment_data": segment_data,
        "_channel": channel,
    }


def _node_hierarchy_check(state: ReactivationState) -> ReactivationState:
    if state.get("terminal_status"):
        return {}

    # Default to a healthy value when no metric data exists yet so the graph
    # runs from launch day without requiring metric bootstrapping.
    observed = get_cached_metric(KILL_SWITCH_FEATURE)
    if observed is None:
        observed = 10.0

    hierarchy = run_decision_hierarchy({
        "subscriber_id": state["subscriber_id"],
        "graph_name": GRAPH_NAME,
        "kill_switch_feature": KILL_SWITCH_FEATURE,
        "kill_switch_observed_value": observed,
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


def _node_build_compose_context(state: ReactivationState) -> ReactivationState:
    if state.get("terminal_status"):
        return {}

    profile = state.get("subscriber_profile") or {}
    payload = state.get("event_payload") or {}

    cohort = payload.get("cohort", "county_live")
    county_id = payload.get("county_id") or profile.get("county_id") or ""
    zip_code = payload.get("zip_code") or ""
    vertical = payload.get("vertical") or profile.get("vertical") or ""
    first_name = (profile.get("name") or "there").split()[0]
    channel = state.get("_channel", "sms")
    campaign = CAMPAIGN_COUNTY_LIVE if cohort == "county_live" else CAMPAIGN_SOLD_OUT

    render_context = {
        "first_name": first_name,
        "cohort": cohort,
        "county_id": county_id,
        "zip_code": zip_code,
        "vertical": vertical,
        "channel": channel,
    }

    if cohort == "county_live":
        system_prompt = (
            "You are Cora, a concise outbound copywriter for Forced Action — "
            "a distressed property intelligence platform. "
            "Write a short, urgent, professional reactivation message. "
            "Never use emojis. Never use all-caps. Always include 'Reply STOP to opt out' for SMS."
        )
        if channel == "sms":
            user_prompt = (
                f"Write a reactivation SMS for {first_name}. "
                f"The {county_id} county territory just launched on Forced Action. "
                f"They previously subscribed but are no longer active. "
                f"Remind them new distressed leads are live and invite them back. "
                f"Include this link: https://forcedactionleads.com?county={county_id} "
                f"Keep it under 160 characters. End with 'Reply STOP to opt out.'"
            )
            fallback_body = (
                f"{first_name}, {county_id} just launched on Forced Action — "
                f"new distressed leads are live. Claim your spot: "
                f"https://forcedactionleads.com?county={county_id}  Reply STOP to opt out."
            )
            subject = ""
        else:
            user_prompt = (
                f"Write a reactivation email body for {first_name}. "
                f"The {county_id} county territory just launched on Forced Action. "
                f"They previously subscribed but are no longer active. "
                f"Remind them new distressed leads are live and invite them back. "
                f"Include this link: https://forcedactionleads.com?county={county_id} "
                f"Keep it under 200 words. Be direct and professional."
            )
            fallback_body = (
                f"Hi {first_name},\n\n"
                f"The {county_id} territory just launched on Forced Action. "
                f"New distressed property leads are live and available now.\n\n"
                f"Claim your territory: https://forcedactionleads.com?county={county_id}\n\n"
                f"— Forced Action Team"
            )
            subject = f"{county_id} just launched — new leads are live on Forced Action"
    else:
        system_prompt = (
            "You are Cora, a concise outbound copywriter for Forced Action — "
            "a distressed property intelligence platform. "
            "Write a short, urgent, scarcity-driven reactivation message. "
            "Never use emojis. Never use all-caps. Always include 'Reply STOP to opt out' for SMS."
        )
        if channel == "sms":
            user_prompt = (
                f"Write a reactivation SMS for {first_name}. "
                f"A slot just opened in ZIP {zip_code} for {vertical} leads in {county_id}. "
                f"They previously subscribed but are no longer active. "
                f"Create urgency — the slot may not last. "
                f"Include this link: https://forcedactionleads.com?zip={zip_code} "
                f"Keep it under 160 characters. End with 'Reply STOP to opt out.'"
            )
            fallback_body = (
                f"{first_name}, a slot opened in {zip_code} for {vertical} leads in {county_id}. "
                f"Lock it: https://forcedactionleads.com?zip={zip_code}  Reply STOP to opt out."
            )
            subject = ""
        else:
            user_prompt = (
                f"Write a reactivation email body for {first_name}. "
                f"A Gold+ lead slot just opened in ZIP {zip_code} for {vertical} in {county_id}. "
                f"They previously subscribed but are no longer active. "
                f"Create urgency — slots are limited and this may not last. "
                f"Include this link: https://forcedactionleads.com?zip={zip_code} "
                f"Keep it under 200 words. Be direct and professional."
            )
            fallback_body = (
                f"Hi {first_name},\n\n"
                f"A slot just opened in {zip_code} for {vertical} leads in {county_id}. "
                f"Gold+ distressed properties are available now — unassigned and unlocked.\n\n"
                f"Lock your slot: https://forcedactionleads.com?zip={zip_code}\n\n"
                f"— Forced Action Team"
            )
            subject = f"Slot open in {zip_code} — lock it now on Forced Action"

    return {
        "_campaign": campaign,
        "_recipient_email": profile.get("email") or "",
        "_subject": subject,
        "_system_prompt": system_prompt,
        "_user_prompt": user_prompt,
        "_fallback_body": fallback_body,
        "_render_context": render_context,
        "_variant_id": None,
    }


def _node_compose_and_send(state: ReactivationState) -> ReactivationState:
    if state.get("terminal_status"):
        return {}

    channel = state.get("_channel", "sms")
    base = {
        "decision_id": state["decision_id"],
        "graph_name": GRAPH_NAME,
        "subscriber_id": state["subscriber_id"],
        "campaign": state.get("_campaign"),
        "claude_task_type": CLAUDE_TASK_TYPE,
        "system_prompt": state.get("_system_prompt", ""),
        "user_prompt": state.get("_user_prompt", ""),
        "cache_system": False,
        "variant_id": state.get("_variant_id"),
        "use_fallback": state.get("use_fallback", False),
        "ab_fallback_body": state.get("_fallback_body"),
        "personalization_context": state.get("_render_context"),
        "tokens_used": int(state.get("tokens_used", 0) or 0),
        "cost_usd": float(state.get("cost_usd", 0.0) or 0.0),
    }

    if channel == "email":
        result = run_compose_and_send_email({
            **base,
            "recipient_email": state.get("_recipient_email", ""),
            "subject": state.get("_subject", ""),
            "max_output_tokens": 400,
        })
    else:
        result = run_compose_and_send({
            **base,
            "message_type": "marketing",
            "max_output_tokens": 160,
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


def _node_finalize(state: ReactivationState) -> ReactivationState:
    final_status = state.get("terminal_status") or "completed"

    if state.get("sent"):
        try:
            with db.session_scope() as s:
                s.execute(
                    text(
                        "UPDATE subscribers SET last_reactivation_attempt_at = :now "
                        "WHERE id = :id"
                    ),
                    {"now": datetime.now(timezone.utc), "id": state["subscriber_id"]},
                )
        except Exception:
            logger.exception(
                "reactivation: failed to stamp last_reactivation_attempt_at sub_id=%s",
                state.get("subscriber_id"),
            )

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
                summary={
                    "failure_reason": state.get("failure_reason"),
                    "early_abort": True,
                    "review_capture": _build_review_capture(state),
                },
            )
            publish_feedback_ritual_candidate(
                decision_id=state["decision_id"],
                graph_name=GRAPH_NAME,
                terminal_status=final_status,
            )
        except Exception:
            pass

    if not state.get("terminal_status"):
        return {"terminal_status": "completed"}
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Graph assembly
# ─────────────────────────────────────────────────────────────────────────────

def build_reactivation_graph() -> StateGraph:
    g = StateGraph(ReactivationState)
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


def run_reactivation(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Compile + invoke the reactivation graph for a single event."""
    graph = build_reactivation_graph().compile()
    final = graph.invoke({
        "decision_id": decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type": "reactivation_outreach",
        "event_payload": event_payload,
    })
    return dict(final)
