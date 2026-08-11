"""
Post-call recap subgraph — THROUGH-v2.2 T3's auto-drafter.

    gate -> compose -> resolve_links -> persist

Same shape as outreach.py (C1/C2), triggered by a different event
(call.completed instead of target.ready): a completed call is drafting
material just like a fresh lead is, so it reuses the same validate_can_draft
gate (kill switch, suppression, duplicate-actionable-draft) and the same
offer_links resolution — only the compose prompt and the synthetic
"fact" fed into validation differ. Persists a normal OutboundDraft
(cell_id="post_call_recap") — no new approval path; THROUGH's builder.py
picks it up exactly like any other draft.

Input is the vendor-agnostic stub shape (contracts.CallCompletedStubPayload)
— see src.agents.cora.ingestion.post_call_producer for why the actual
trigger source (Synthflow vs. Aircall vs. other) is deliberately deferred.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy.orm import Session

from src.agents.cora import contracts, offer_links, store
from src.agents.cora.subgraphs.outreach import _parse_compose_output
from src.agents.cora.validation import validate_can_draft
from src.services.claude_router import call_claude_with_usage

logger = logging.getLogger(__name__)

POST_CALL_CELL_ID = "post_call_recap"


class PostCallRecapState(TypedDict, total=False):
    # ── Inputs (shape of contracts.CallCompletedStubPayload) ────────────────
    opportunity_thread_id: str
    transcript_text: Optional[str]
    call_outcome: Optional[str]
    duration_seconds: Optional[int]
    completed_at: str

    # ── Derived ──────────────────────────────────────────────────────────────
    buyer_entity: Dict[str, Any]
    venture_key: str  # derived in _node_gate from buyer_entity["county_id"]
    conversation: List[Dict[str, Any]]
    contact_email: Optional[str]
    contact_phone: Optional[str]
    offer: str
    avenue: str
    angle: str
    recommended_channel: str
    facts_used: List[Dict[str, Any]]

    # ── Compose outputs ──────────────────────────────────────────────────────
    subject: str
    body: str
    tokens_used: int
    cost_usd: float

    # ── Link resolution ──────────────────────────────────────────────────────
    booking_link: Optional[str]
    payment_link: Optional[str]

    # ── Outcome ──────────────────────────────────────────────────────────────
    draft_id: Optional[str]
    terminal_status: str
    reject_reason: Optional[str]


def _make_node_gate(db: Optional[Session]):
    def _node_gate(state: PostCallRecapState) -> PostCallRecapState:
        from config.cora_cell_grid import get_cell
        from src.agents.cora.tools.read_tools import get_buyer_entity_by_opportunity_thread_id, get_contact_channel

        cell = get_cell(POST_CALL_CELL_ID)
        if cell is None:
            return {"terminal_status": "failed", "reject_reason": "invalid_cell_id"}

        buyer_entity = get_buyer_entity_by_opportunity_thread_id(db, state["opportunity_thread_id"])
        if buyer_entity is None:
            return {"terminal_status": "failed", "reject_reason": "unresolvable_buyer_entity"}

        contact = get_contact_channel(db, buyer_entity["id"])
        channel = "email" if contact.get("email") else "sms" if contact.get("phone") else "email"

        # The call itself is the "fact" grounding this draft — a completed
        # call is fresher evidence than anything Hunter's public-record
        # pipeline could supply, and validate_can_draft requires at least
        # one fact with an observed_at to pass its staleness check.
        facts_used = [{
            "fact_key": "call_outcome",
            "value": state.get("call_outcome") or "completed",
            "source_ref": "post_call_recap",
            "observed_at": state["completed_at"],
        }]

        result = validate_can_draft(
            buyer_entity=buyer_entity,
            cell_id=POST_CALL_CELL_ID,
            facts_used=facts_used,
            recommended_channel=channel,
            db=db,
            email=contact.get("email"),
            phone=contact.get("phone"),
        )
        if not result.allowed:
            return {"terminal_status": "rejected", "reject_reason": result.reject_reason}

        return {
            "buyer_entity": buyer_entity,
            "venture_key": store.venture_key_for_county(db, buyer_entity.get("county_id")),
            "conversation": store.read_conversation(db, state["opportunity_thread_id"]),
            "contact_email": contact.get("email"),
            "contact_phone": contact.get("phone"),
            "offer": cell["offer"],
            "avenue": cell["avenue"],
            "angle": cell["angle"],
            "recommended_channel": channel,
            "facts_used": facts_used,
        }

    return _node_gate


def _make_node_compose(db: Optional[Session]):
    def _node_compose(state: PostCallRecapState) -> PostCallRecapState:
        if state.get("terminal_status"):
            return {}

        buyer_entity = state["buyer_entity"]
        system = (
            "Draft a short follow-up email to a real-estate buyer prospect after a phone call just "
            "ended. Ground every claim ONLY in the call outcome and prior conversation provided — never "
            "invent details not present there. Reference that you just spoke, restate the agreed next "
            "step if the outcome implies one, and include a clear single ask. Under 100 words. Output "
            "exactly two lines: 'SUBJECT: <subject>' then 'BODY: <body>'."
        )
        import json
        user = (
            f"Recipient: {buyer_entity.get('canonical_name')}\n"
            f"Call outcome: {state.get('call_outcome') or 'unknown'}\n"
            f"Call transcript excerpt: {(state.get('transcript_text') or '')[:1500]}\n"
            f"Prior conversation (JSON): {json.dumps(state.get('conversation', []), default=str)[:1500]}"
        )

        try:
            result = call_claude_with_usage(
                task_type="cora_post_call_recap",
                messages=[{"role": "user", "content": user}],
                system=system,
                max_tokens=300,
                graph_name="cora_post_call_recap",
                db=db,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("post_call_recap.compose: Claude call failed: %s", exc)
            return {"terminal_status": "failed", "reject_reason": f"compose_error:{type(exc).__name__}"}

        subject, body = _parse_compose_output(result["text"])
        if not subject or not body:
            return {"terminal_status": "failed", "reject_reason": "compose_parse_failed"}

        return {
            "subject": subject,
            "body": body,
            "tokens_used": int(result.get("input_tokens", 0)) + int(result.get("output_tokens", 0)),
            "cost_usd": float(result.get("cost_usd", 0.0)),
        }

    return _node_compose


def _make_node_resolve_links(db: Optional[Session]):
    def _node_resolve_links(state: PostCallRecapState) -> PostCallRecapState:
        if state.get("terminal_status"):
            return {}
        resolved = offer_links.resolve_offer_link(
            state["offer"], buyer_entity=state["buyer_entity"], db=db,
            customer_email=state.get("contact_email"),
        )
        return {"booking_link": resolved.booking_link, "payment_link": resolved.payment_link}

    return _node_resolve_links


def _make_node_persist(db: Optional[Session]):
    def _node_persist(state: PostCallRecapState) -> PostCallRecapState:
        if state.get("terminal_status"):
            return {"terminal_status": state.get("terminal_status", "rejected")}

        buyer_entity = state["buyer_entity"]
        channel = state.get("recommended_channel", "email")
        contact_email = state.get("contact_email")
        contact_phone = state.get("contact_phone")
        if channel == "email" and not contact_email:
            logger.warning(
                "[cora.post_call_recap] draft skipped — no contact_email for thread %s channel=email",
                state.get("opportunity_thread_id"),
            )
            return {"terminal_status": "no_recipient"}
        if channel in ("sms", "voice") and not contact_phone:
            logger.warning(
                "[cora.post_call_recap] draft skipped — no contact_phone for thread %s channel=%s",
                state.get("opportunity_thread_id"), channel,
            )
            return {"terminal_status": "no_recipient"}

        draft_id = store.new_draft_id()
        record = store.OutboundDraftRecord(
            draft_id=draft_id,
            opportunity_thread_id=state["opportunity_thread_id"],
            buyer_entity_id=buyer_entity["id"],
            cell_id=POST_CALL_CELL_ID,
            offer=state["offer"],
            avenue=state["avenue"],
            angle=state["angle"],
            subject=state["subject"],
            body=state["body"],
            facts_used=state.get("facts_used", []),
            source_refs=[f.get("source_ref") for f in state.get("facts_used", [])],
            recommended_channel=channel,
            confidence_score=int(buyer_entity.get("confidence_score", 0) or 0),
            booking_link=state.get("booking_link"),
            payment_link=state.get("payment_link"),
            contact_email=contact_email,
            contact_phone=contact_phone,
            venture_key=state.get("venture_key") or store.venture_key_for_county(
                db, buyer_entity.get("county_id")
            ),
        )
        store.append_draft(db, record)
        store.index_contact_email(state.get("contact_email"), state["opportunity_thread_id"])

        fleet_event = contracts.make_fleet_event(
            "action.ready", state["opportunity_thread_id"], draft_id=draft_id,
        )
        contracts.emit_fleet_event_stub(fleet_event)
        store.mark_draft_published(db, draft_id)

        return {"draft_id": draft_id, "terminal_status": "completed"}

    return _node_persist


def _after_gate(state: PostCallRecapState) -> str:
    return "persist" if state.get("terminal_status") else "compose"


def _after_compose(state: PostCallRecapState) -> str:
    return "persist" if state.get("terminal_status") else "resolve_links"


def build_post_call_recap_graph(db: Optional[Session] = None) -> StateGraph:
    g = StateGraph(PostCallRecapState)
    g.add_node("gate", _make_node_gate(db))
    g.add_node("compose", _make_node_compose(db))
    g.add_node("resolve_links", _make_node_resolve_links(db))
    g.add_node("persist", _make_node_persist(db))

    g.add_edge(START, "gate")
    g.add_conditional_edges("gate", _after_gate, {"compose": "compose", "persist": "persist"})
    g.add_conditional_edges("compose", _after_compose, {"resolve_links": "resolve_links", "persist": "persist"})
    g.add_edge("resolve_links", "persist")
    g.add_edge("persist", END)
    return g


def run_post_call_recap(inputs: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    inputs = dict(inputs)
    inputs.pop("db", None)
    graph = build_post_call_recap_graph(db).compile()
    final = graph.invoke(inputs)
    return dict(final)
