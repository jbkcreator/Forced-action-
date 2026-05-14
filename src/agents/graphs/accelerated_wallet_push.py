"""
Accelerated Wallet Push graph (fa016).

Triggered when a saved-card subscriber records their first paid debit and is
not already enrolled in a wallet. Sends an SMS offering to activate the
starter wallet against the card they already saved.

Event envelope (example):
    {
        "event_type": "accelerated_wallet_push_eligible",
        "subscriber_id": 42,
        "payload": {
            "tier":             "starter_wallet",
            "credits_in_offer": 20,
            "price_cents":      4900,
            "missed_leads":     3,
            "reason":           "saved_card_paid_intent",
            "cta_url":          "https://app.forcedaction.io/dashboard/<uuid>?wallet_offer=accept",
        },
    }

On a successful send the graph:
  - inserts a WalletPushOffer(status='offered') row,
  - sets Redis `fa:pending_offer:{sub}` = {"type":"wallet_push", ...} TTL 24h,
  - logs the decision via the compose_and_send subgraph (agent_decisions /
    message_outcomes are written there).
"""

from __future__ import annotations

import json as _json
import logging
import uuid
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.prompts.loader import (
    render_fallback_body,
    render_for_subscriber_auto,
)
from src.agents.subgraphs.compose_and_send import run_compose_and_send
from src.agents.subgraphs.decision_hierarchy import run_decision_hierarchy
from src.agents.tools.read_tools import get_subscriber_profile, get_wallet_state

logger = logging.getLogger(__name__)


GRAPH_NAME = "accelerated_wallet_push"
CAMPAIGN = "accelerated_wallet_push"
CLAUDE_TASK_TYPE = "sms_copy"
KILL_SWITCH_FEATURE = "wallet_adoption"


class AcceleratedWalletPushState(TypedDict, total=False):
    decision_id: str
    subscriber_id: int
    event_type: str
    event_payload: dict

    subscriber_profile: dict
    wallet_state: dict

    action_allowed: bool
    action_blocked_reason: str
    use_fallback: bool
    kill_switch_color: str
    revenue_signal_score: int

    framing_variant: str
    ab_variant: Optional[str]

    # Intermediate prompt artefacts produced by build_compose_context and
    # consumed by compose_and_send. Declared so LangGraph's TypedDict schema
    # doesn't drop them between nodes.
    _system_prompt: str
    _user_prompt: str
    _fallback_body: str
    _render_context: dict

    message_body: str
    sent: bool
    send_reason: str
    message_outcome_id: Optional[int]
    tokens_used: int
    cost_usd: float
    terminal_status: str
    failure_reason: str

    offer_id: Optional[int]


def _node_assemble_context(state: AcceleratedWalletPushState) -> AcceleratedWalletPushState:
    profile = get_subscriber_profile(state["subscriber_id"])
    if not profile:
        return {
            "terminal_status": "aborted",
            "failure_reason": "accelerated_wallet_push:subscriber_not_found",
        }
    if profile.get("wallet_opt_out"):
        return {
            "terminal_status": "aborted",
            "failure_reason": "accelerated_wallet_push:wallet_opt_out",
        }
    if not profile.get("has_saved_card"):
        return {
            "terminal_status": "aborted",
            "failure_reason": "accelerated_wallet_push:no_saved_card",
        }

    wallet = get_wallet_state(state["subscriber_id"])
    if wallet.get("enrolled"):
        return {
            "terminal_status": "aborted",
            "failure_reason": "accelerated_wallet_push:already_enrolled",
        }

    return {"subscriber_profile": profile, "wallet_state": wallet}


def _node_hierarchy_check(state: AcceleratedWalletPushState) -> AcceleratedWalletPushState:
    if state.get("terminal_status"):
        return {}

    # Feed the cached `wallet_adoption` metric into the kill-switch gate so it
    # can compute green/yellow/red instead of returning "unknown" (which the
    # hierarchy treats as red).
    try:
        from src.tasks.kill_switch_metric_ingest import get_cached_metric
        observed = get_cached_metric(KILL_SWITCH_FEATURE)
    except Exception:
        observed = None

    hierarchy = run_decision_hierarchy({
        "subscriber_id": state["subscriber_id"],
        "graph_name": GRAPH_NAME,
        "kill_switch_feature": KILL_SWITCH_FEATURE,
        "kill_switch_observed_value": observed,
        "ab_test_name": "accelerated_wallet_push_framing",
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


def _node_build_compose_context(state: AcceleratedWalletPushState) -> Dict[str, Any]:
    if state.get("terminal_status"):
        return {}

    profile = state.get("subscriber_profile") or {}
    payload = state.get("event_payload") or {}

    # Prefer the payload value (set by the detector). Fall back to profile
    # only when the payload did not specify a value at all. Note: `or` would
    # incorrectly fall through on 0.
    if "missed_leads" in payload and payload["missed_leads"] is not None:
        missed_leads = int(payload["missed_leads"])
    else:
        missed_leads = int(profile.get("missed_lead_count") or 0)
    credits = int(payload.get("credits_in_offer") or 20)
    price_cents = int(payload.get("price_cents") or 4900)
    framing = "missing_leads" if missed_leads > 0 else "credits_ready"
    zip_code = (
        payload.get("zip_code")
        or profile.get("zip_code")
        or profile.get("primary_zip")
        or ""
    )

    context = {
        "first_name": (profile.get("name") or "there").split(" ")[0],
        "zip_code": zip_code,
        "missed_leads": missed_leads,
        "credits_in_offer": credits,
        "price_cents": price_cents,
        "price_dollars_formatted": f"${price_cents // 100}",
        "cta_url": payload.get("cta_url") or "",
        "framing_variant": framing,
        "tier": payload.get("tier") or "starter_wallet",
        "revenue_signal_score": state.get("revenue_signal_score", 0),
    }

    # Use render_for_subscriber_auto so A/B variant assignment (deterministic)
    # is honoured. Falls back to base system.yaml if no test is enabled.
    try:
        system, user, variant, _test_name = render_for_subscriber_auto(
            GRAPH_NAME, state["subscriber_id"], context
        )
    except Exception as exc:  # never block a send on prompt loading
        logger.warning("render_for_subscriber_auto failed: %s", exc)
        system, user = "", ""
        variant = None

    try:
        fallback = render_fallback_body(GRAPH_NAME, context)
    except Exception:
        fallback = (
            f"{context['first_name']}, your Wallet is ready on the card you saved. "
            f"{credits} credits at {context['price_dollars_formatted']}/mo. "
            f"Reply WALLET to activate. {context['cta_url']}"
        )

    return {
        "framing_variant": framing,
        "ab_variant": variant,
        "_system_prompt": system,
        "_user_prompt": user,
        "_fallback_body": fallback,
        "_render_context": context,
    }


def _node_compose_and_send(state: AcceleratedWalletPushState) -> AcceleratedWalletPushState:
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
        "variant_id": state.get("ab_variant"),
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


def _node_finalize(state: AcceleratedWalletPushState) -> AcceleratedWalletPushState:
    from src.agents.tools.write_tools import log_decision

    final_status = state.get("terminal_status") or "completed"
    payload = state.get("event_payload") or {}

    offer_id: Optional[int] = None

    if final_status == "completed" and state.get("sent"):
        # Reuse the offer row created upstream by ensure_offer_row() (called
        # from the webhook handler). If missing (legacy path), create it.
        try:
            from src.core.database import Database
            from src.core.models import WalletPushOffer
            from src.services import wallet_engine
            with Database().session_scope() as session:
                offer = wallet_engine.ensure_offer_row(
                    state["subscriber_id"],
                    {"tier": payload.get("tier") or "starter_wallet",
                     "reason": "agent_sms_sent"},
                    session,
                )
                # Stamp agent-side metadata if this is the row we just promoted.
                if not offer.decision_id:
                    offer.decision_id = state.get("decision_id")
                if not offer.framing_variant or offer.framing_variant == "credits_ready":
                    offer.framing_variant = state.get("framing_variant") or "credits_ready"
                if not offer.ab_variant and state.get("ab_variant"):
                    offer.ab_variant = state.get("ab_variant")
                session.flush()
                offer_id = offer.id
        except Exception as exc:
            logger.warning("wallet_push_offer ensure failed: %s", exc)

        # fa017: business event audit (offer sent successfully)
        try:
            from src.services.business_events import log_business_event
            log_business_event(
                "WALLET_OFFER_SENT", subscriber_id=state["subscriber_id"],
                payload={
                    "offer_id": offer_id,
                    "framing_variant": state.get("framing_variant"),
                    "ab_variant": state.get("ab_variant"),
                    "tier": payload.get("tier") or "starter_wallet",
                    "message_outcome_id": state.get("message_outcome_id"),
                },
            )
        except Exception:
            pass

        # Set pending offer for SMS reply routing (24h TTL)
        try:
            from src.core.redis_client import redis_available, rset as _rset
            if redis_available():
                offer_envelope = _json.dumps({
                    "type": "wallet_push",
                    "offer_id": offer_id,
                    "tier": payload.get("tier") or "starter_wallet",
                })
                _rset(
                    f"fa:pending_offer:{state['subscriber_id']}",
                    offer_envelope,
                    ttl_seconds=86400,
                )
        except Exception:
            pass
    else:
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
                    "framing_variant": state.get("framing_variant"),
                },
            )
        except Exception:
            pass

    out: AcceleratedWalletPushState = {}
    if offer_id is not None:
        out["offer_id"] = offer_id
    if not state.get("terminal_status"):
        out["terminal_status"] = "completed"
    return out


def build_accelerated_wallet_push_graph() -> StateGraph:
    g = StateGraph(AcceleratedWalletPushState)
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


def run_accelerated_wallet_push(
    event_payload: Dict[str, Any],
    subscriber_id: int,
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    graph = build_accelerated_wallet_push_graph().compile()
    final = graph.invoke({
        "decision_id": decision_id or str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "event_type": "accelerated_wallet_push_eligible",
        "event_payload": event_payload,
    })
    return dict(final)
