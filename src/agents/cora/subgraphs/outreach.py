"""
Outreach subgraph — C1 (cold-draft engine) + C2 (target-to-draft pipeline).

Node shape mirrors src/agents/subgraphs/compose_and_send_email.py's
budget_precheck -> compose -> send_and_log chain, minus the send node:

    gate -> compose -> resolve_links -> persist

LLM reasoning happens only in `compose` (angle selection/explanation +
draft copy). Routing, validation, persistence are all plain code. This
subgraph never imports src.agents.tools.write_tools — no send capability
anywhere in this file.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy.orm import Session

from src.agents.cora import contracts, offer_links, opportunity_state, store
from src.agents.cora.validation import validate_can_draft
from src.services.claude_router import call_claude_with_usage
from src.services.playbook_retrieval import fetch_lessons, format_lessons_for_prompt

logger = logging.getLogger(__name__)


class OutreachState(TypedDict, total=False):
    # ── Inputs ───────────────────────────────────────────────────────────────
    buyer_entity: Dict[str, Any]          # from tools.read_tools.get_buyer_entity_by_opportunity_thread_id
    cell_id: str
    facts_used: List[Dict[str, Any]]
    contact_email: Optional[str]
    contact_phone: Optional[str]
    is_followup: bool                      # set by followup_scheduler.py; see validate_can_draft
    followup_sequence: Optional[int]       # 1 (day-2), 2 (day-5), ... — None for the original touch
    # Which venture this target belongs to (CLONE-v2.2 / CL4). Producers set
    # this from the target's county (see ingestion/target_producer.py); if a
    # caller omits it, _node_persist derives it from buyer_entity["county_id"]
    # rather than silently falling through to OutboundDraftRecord's default —
    # see that field's docstring for why a wrong value here is a production bug,
    # not a cosmetic one.
    venture_key: str

    # ── Derived ──────────────────────────────────────────────────────────────
    offer: str
    avenue: str
    angle: str
    recommended_channel: str
    price_cents: Optional[int]              # None when the offer has no price band (RESPA-excluded/unconfigured)
    experiment_assignment_id: Optional[int]

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
    terminal_status: str  # 'completed' | 'rejected' | 'failed'
    reject_reason: Optional[str]


def _make_node_gate(db: Optional[Session]):
    def _node_gate(state: OutreachState) -> OutreachState:
        from config.cora_cell_grid import get_cell

        cell = get_cell(state["cell_id"])
        if cell is None:
            return {"terminal_status": "failed", "reject_reason": "invalid_cell_id"}

        buyer_entity = state["buyer_entity"]
        channel = "email" if state.get("contact_email") else "sms" if state.get("contact_phone") else "email"

        result = validate_can_draft(
            buyer_entity=buyer_entity,
            cell_id=state["cell_id"],
            facts_used=state.get("facts_used", []),
            recommended_channel=channel,
            db=db,
            email=state.get("contact_email"),
            phone=state.get("contact_phone"),
            is_followup=bool(state.get("is_followup", False)),
        )
        if not result.allowed:
            return {"terminal_status": "rejected", "reject_reason": result.reject_reason}

        return {
            "offer": cell["offer"],
            "avenue": cell["avenue"],
            "angle": cell["angle"],
            "recommended_channel": channel,
        }

    return _node_gate


def _make_node_price_variant(db: Optional[Session]):
    def _node_price_variant(state: OutreachState) -> OutreachState:
        if state.get("terminal_status"):
            return {}

        from src.services.agent_lane_experiment_engine import (
            ensure_price_band_experiment,
            get_price_variant,
            record_decision_snapshot,
        )

        buyer_entity = state["buyer_entity"]
        offer = state["offer"]
        experiment = ensure_price_band_experiment(offer, db)
        if experiment is None:
            # No price band for this offer (RESPA-excluded/unconfigured) —
            # no price fact for compose, nothing to attribute later.
            return {"price_cents": None, "experiment_assignment_id": None}

        thread_id = buyer_entity["opportunity_thread_id"]
        variant = get_price_variant(offer, experiment.id, thread_id, db)
        record_decision_snapshot(
            thread_id, experiment.test_name, db,
            message_angle=state.get("angle"),
            offer=offer,
            chosen_action=f"draft_{offer}_cell_{state['cell_id']}",
        )
        return {
            "price_cents": variant["price_cents"],
            "experiment_assignment_id": variant["experiment_assignment_id"],
        }

    return _node_price_variant


def _build_prompt(state: OutreachState, db=None) -> tuple[str, str]:
    buyer_entity = state["buyer_entity"]
    facts_lines = "\n".join(
        f"- {f.get('fact_key')}: {f.get('value')} (source: {f.get('source_ref')})"
        for f in state.get("facts_used", [])
    )

    lesson_block = ""
    if db is not None:
        context = {
            k: v for k, v in {
                "offer": state.get("offer"),
                "avenue": state.get("avenue"),
                "angle": state.get("angle"),
            }.items() if v is not None
        }
        lessons = fetch_lessons(db, agent_domain="cora", context=context)
        lesson_block = format_lessons_for_prompt(lessons)

    system = (
        "You are drafting a single cold outreach email for Forced Action, a distressed-property "
        "intelligence platform, to a real-estate buyer entity. Ground every claim ONLY in the facts "
        "listed below — never invent a number, date, name, or detail not present in the facts, "
        "INCLUDING seat numbers, slot counts, deadlines, or any other specific not explicitly listed. "
        "If the angle implies a specific (e.g. a numbered seat) and no fact supplies one, write the "
        "framing generically (e.g. 'a founding seat') rather than inventing a number. If you cannot "
        "support a sentence with a listed fact, do not write it. Keep the tone like one sharp Florida "
        "investor talking to another: specific, respectful of time, one clear ask. Under 120 words unless "
        "the angle genuinely needs more. Output exactly two lines: 'SUBJECT: <subject>' then "
        "'BODY: <body>'."
    )
    if lesson_block:
        system = system + "\n\n" + lesson_block

    price_cents = state.get("price_cents")
    price_line = f"Price: ${price_cents / 100:,.0f}/mo\n" if price_cents is not None else ""
    user = (
        f"Recipient: {buyer_entity.get('canonical_name')}\n"
        f"Offer: {state['offer']}\n"
        f"{price_line}"
        f"Avenue: {state['avenue']}\n"
        f"Angle: {state['angle']}\n"
        f"Facts you may use:\n{facts_lines or '(none)'}\n"
    )
    return system, user


def _parse_compose_output(text: str) -> tuple[str, str]:
    subject, body = "", ""
    for line in text.splitlines():
        if line.upper().startswith("SUBJECT:"):
            subject = line.split(":", 1)[1].strip()
        elif line.upper().startswith("BODY:"):
            body = line.split(":", 1)[1].strip()
        elif body:
            body += "\n" + line
    return subject, body


def _make_node_compose(db: Optional[Session]):
    def _node_compose(state: OutreachState) -> OutreachState:
        if state.get("terminal_status"):
            return {}

        system, user = _build_prompt(state, db=db)
        try:
            result = call_claude_with_usage(
                task_type="cora_outreach_draft",
                messages=[{"role": "user", "content": user}],
                system=system,
                max_tokens=400,
                graph_name="cora_outreach",
                db=db,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("outreach.compose: Claude call failed: %s", exc)
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
    def _node_resolve_links(state: OutreachState) -> OutreachState:
        if state.get("terminal_status"):
            return {}
        resolved = offer_links.resolve_offer_link(
            state["offer"], buyer_entity=state["buyer_entity"], db=db,
            customer_email=state.get("contact_email"),
        )
        return {"booking_link": resolved.booking_link, "payment_link": resolved.payment_link}

    return _node_resolve_links


def _make_node_persist(db: Optional[Session]):
    def _node_persist(state: OutreachState) -> OutreachState:
        if state.get("terminal_status"):
            return {"terminal_status": state.get("terminal_status", "rejected")}

        buyer_entity = state["buyer_entity"]
        venture_key = state.get("venture_key") or store.venture_key_for_county(
            db, buyer_entity.get("county_id")
        )
        draft_id = store.new_draft_id()
        record = store.OutboundDraftRecord(
            draft_id=draft_id,
            opportunity_thread_id=buyer_entity["opportunity_thread_id"],
            buyer_entity_id=buyer_entity["id"],
            cell_id=state["cell_id"],
            offer=state["offer"],
            avenue=state["avenue"],
            angle=state["angle"],
            subject=state["subject"],
            body=state["body"],
            facts_used=state.get("facts_used", []),
            source_refs=[f.get("source_ref") for f in state.get("facts_used", [])],
            recommended_channel=state["recommended_channel"],
            confidence_score=int(buyer_entity.get("confidence_score", 0) or 0),
            booking_link=state.get("booking_link"),
            payment_link=state.get("payment_link"),
            is_followup=bool(state.get("is_followup", False)),
            followup_sequence=state.get("followup_sequence"),
            contact_email=state.get("contact_email"),
            contact_phone=state.get("contact_phone"),
            venture_key=venture_key,
            price_cents=state.get("price_cents"),
            experiment_assignment_id=state.get("experiment_assignment_id"),
        )
        store.append_draft(db, record)
        store.index_contact_email(state.get("contact_email"), buyer_entity["opportunity_thread_id"])
        opportunity_state.mark_targeted(buyer_entity["opportunity_thread_id"], reason="draft_created")

        fleet_event = contracts.make_fleet_event(
            "action.ready", buyer_entity["opportunity_thread_id"], draft_id=draft_id,
        )
        contracts.emit_fleet_event_stub(fleet_event)
        store.mark_draft_published(db, draft_id)

        return {"draft_id": draft_id, "terminal_status": "completed"}

    return _node_persist


def _after_gate(state: OutreachState) -> str:
    return "persist" if state.get("terminal_status") else "price_variant"


def _after_price_variant(state: OutreachState) -> str:
    return "persist" if state.get("terminal_status") else "compose"


def _after_compose(state: OutreachState) -> str:
    return "persist" if state.get("terminal_status") else "resolve_links"


def build_outreach_graph(db: Optional[Session] = None) -> StateGraph:
    # `db` is captured by closure into the two nodes that need it, never placed
    # in graph state — state must stay msgpack-serializable end-to-end since a
    # parent graph's checkpointer can apply to nodes invoked underneath it.
    g = StateGraph(OutreachState)
    g.add_node("gate", _make_node_gate(db))
    g.add_node("price_variant", _make_node_price_variant(db))
    g.add_node("compose", _make_node_compose(db))
    g.add_node("resolve_links", _make_node_resolve_links(db))
    g.add_node("persist", _make_node_persist(db))

    g.add_edge(START, "gate")
    g.add_conditional_edges("gate", _after_gate, {"price_variant": "price_variant", "persist": "persist"})
    g.add_conditional_edges("price_variant", _after_price_variant, {"compose": "compose", "persist": "persist"})
    g.add_conditional_edges("compose", _after_compose, {"resolve_links": "resolve_links", "persist": "persist"})
    g.add_edge("resolve_links", "persist")
    g.add_edge("persist", END)
    return g


def run_outreach(inputs: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    """Convenience wrapper: compile + invoke (no checkpointer — called directly by tests/CLI)."""
    inputs = dict(inputs)
    inputs.pop("db", None)
    graph = build_outreach_graph(db).compile()
    final = graph.invoke(inputs)
    return dict(final)
