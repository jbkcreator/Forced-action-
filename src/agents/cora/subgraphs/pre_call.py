"""
Pre-call subgraph — C4.

    gather_context -> compose_brief -> persist

Produces a PreCallBrief for Throughput's closing cockpit to consume later —
this subgraph does not build that UI, only the brief + booking-link config
(config/cora_offer_links.py doubles as the "BookingLink model").
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy.orm import Session

from src.agents.cora import contracts, offer_links, opportunity_state, store
from src.services.claude_router import call_claude_with_usage

logger = logging.getLogger(__name__)


class PreCallState(TypedDict, total=False):
    # ── Inputs (shape of contracts.CallBookedStubPayload) ───────────────────
    opportunity_thread_id: str
    call_booked_at: str
    rep: Optional[str]
    scheduled_for: Optional[str]
    buyer_entity: Dict[str, Any]

    # ── Derived ──────────────────────────────────────────────────────────────
    conversation: List[Dict[str, Any]]
    current_intent: Optional[str]
    recommended_offer: str
    likely_objections: List[Dict[str, str]]
    booking_link: Optional[str]
    payment_link: Optional[str]

    # ── Output ────────────────────────────────────────────────────────────────
    brief_id: Optional[str]
    brief_content: Dict[str, Any]
    terminal_status: str


def _make_node_gather_context(db: Optional[Session]):
    def _node_gather_context(state: PreCallState) -> PreCallState:
        from config.cora_objection_library import get_objections_for_avenue

        conversation = store.read_conversation(state["opportunity_thread_id"])
        replies = store.read_replies(state["opportunity_thread_id"])
        current_intent = replies[-1].get("intent") if replies else None

        recommendation = contracts.recommend_offer_stub(state["buyer_entity"])
        avenue = next((d["record"].get("avenue") for d in conversation if d["kind"] == "draft"), None)
        likely_objections = get_objections_for_avenue(avenue or "")
        # buyer_entity + db required here too — same reason as offer_links.py's
        # own docstring: founder_tier's real Stripe checkout only resolves when
        # both are supplied; omitting them (as this used to) silently falls
        # through to the "unbuilt" branch for every offer, founder_tier included.
        resolved = offer_links.resolve_offer_link(
            recommendation["offer"], buyer_entity=state["buyer_entity"], db=db,
        )

        return {
            "conversation": conversation,
            "current_intent": current_intent,
            "recommended_offer": recommendation["offer"],
            "likely_objections": likely_objections,
            "booking_link": resolved.booking_link,
            "payment_link": resolved.payment_link,
        }

    return _node_gather_context


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def _extract_json(text: str) -> Dict[str, Any]:
    """Claude sometimes wraps JSON output in ```json fences despite instructions not to. Strip if present."""
    match = _JSON_FENCE_RE.search(text)
    candidate = match.group(1) if match else text
    return json.loads(candidate)


def _make_node_compose_brief(db: Optional[Session]):
    def _node_compose_brief(state: PreCallState) -> PreCallState:
        buyer_entity = state["buyer_entity"]
        system = (
            "Compose a pre-call brief for a founder about to call a real-estate buyer prospect. Ground every "
            "claim ONLY in the provided context — never invent facts. Output a JSON object with keys: "
            "suggested_opening (string), call_objective (string, one sentence)."
        )
        user = (
            f"Prospect: {buyer_entity.get('canonical_name')}\n"
            f"Portfolio: {buyer_entity.get('total_purchase_count')} purchases, "
            f"${buyer_entity.get('total_cash_volume')} total cash volume\n"
            f"Current reply intent: {state.get('current_intent')}\n"
            f"Recommended offer: {state['recommended_offer']}\n"
            f"Conversation so far (JSON): {json.dumps(state.get('conversation', []), default=str)[:2000]}"
        )

        try:
            result = call_claude_with_usage(
                task_type="cora_pre_call_brief",
                messages=[{"role": "user", "content": user}],
                system=system,
                max_tokens=300,
                graph_name="cora_pre_call",
                db=db,
            )
            parsed = _extract_json(result["text"])
            suggested_opening = parsed.get("suggested_opening", "")
            call_objective = parsed.get("call_objective", "")
        except Exception as exc:  # noqa: BLE001
            logger.warning("pre_call.compose_brief: Claude call/parse failed: %s", exc)
            suggested_opening = ""
            call_objective = ""

        content = _build_brief_content(state, buyer_entity, suggested_opening, call_objective)
        return {"brief_content": content}

    return _node_compose_brief


def _build_brief_content(
    state: "PreCallState", buyer_entity: Dict[str, Any], suggested_opening: str, call_objective: str
) -> Dict[str, Any]:
    return {
        "prospect_identity": {
            "canonical_name": buyer_entity.get("canonical_name"),
            "entity_type": buyer_entity.get("entity_type"),
        },
        "hunter_facts": {
            "total_purchase_count": buyer_entity.get("total_purchase_count"),
            "total_cash_volume": buyer_entity.get("total_cash_volume"),
            "is_whale": buyer_entity.get("is_whale"),
            "county_id": buyer_entity.get("county_id"),
        },
        "why_now_catalyst": buyer_entity.get("whale_flagged_at"),
        "prior_messages_and_replies": state.get("conversation", []),
        "current_reply_intent": state.get("current_intent"),
        "likely_objections": state.get("likely_objections", []),
        "recommended_offer": state["recommended_offer"],
        "pricing_context": None,  # available only once REVINT/price-band data exists
        "suggested_opening": suggested_opening,
        "call_objective": call_objective,
        "relevant_links": {"booking_link": state.get("booking_link"), "payment_link": state.get("payment_link")},
    }


def _node_persist(state: PreCallState) -> PreCallState:
    brief_id = store.new_brief_id()
    record = store.PreCallBriefRecord(
        brief_id=brief_id,
        opportunity_thread_id=state["opportunity_thread_id"],
        call_booked_at=state["call_booked_at"],
        content=state["brief_content"],
    )
    store.append_pre_call_brief(record)
    opportunity_state.mark_call(state["opportunity_thread_id"], reason="call_booked")

    fleet_event = contracts.make_fleet_event("brief.ready", state["opportunity_thread_id"], brief_id=brief_id)
    contracts.emit_fleet_event_stub(fleet_event)
    return {"brief_id": brief_id, "terminal_status": "completed"}


def build_pre_call_graph(db: Optional[Session] = None) -> StateGraph:
    # `db` is captured by closure into gather_context and compose_brief,
    # never placed in graph state — see outreach.py's build_outreach_graph for why.
    g = StateGraph(PreCallState)
    g.add_node("gather_context", _make_node_gather_context(db))
    g.add_node("compose_brief", _make_node_compose_brief(db))
    g.add_node("persist", _node_persist)

    g.add_edge(START, "gather_context")
    g.add_edge("gather_context", "compose_brief")
    g.add_edge("compose_brief", "persist")
    g.add_edge("persist", END)
    return g


def run_pre_call(inputs: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    inputs = dict(inputs)
    inputs.pop("db", None)
    graph = build_pre_call_graph(db).compile()
    final = graph.invoke(inputs)
    return dict(final)
