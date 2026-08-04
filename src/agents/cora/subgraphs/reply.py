"""
Reply subgraph — C3.

    match_thread -> load_conversation -> classify_intent -> handle_or_compose -> persist

Top-level intents: INTERESTED, OBJECTION, TIMING, REFERRAL, HOSTILE.
Subtypes: PRICING_QUESTION, PRODUCT_QUESTION, NOT_NOW, WRONG_CONTACT,
BOOKING_REQUEST, UNSUBSCRIBE.

UNSUBSCRIBE triggers a real suppression write (src.services.email_suppression
.suppress_contact / the SMS-side opt-out path) and never receives a sales
response — this is the one place in this subgraph that performs a real
write beyond Cora's own store, and it's a compliance action, not a send.

Unmatched replies (no resolvable opportunity_thread_id) go straight to
manual_review — never guessed.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy.orm import Session

from src.agents.cora import contracts, opportunity_state, store
from src.services.claude_router import call_claude_with_usage
from src.services.playbook_retrieval import fetch_lessons, format_lessons_for_prompt

logger = logging.getLogger(__name__)

TOP_LEVEL_INTENTS = ("INTERESTED", "OBJECTION", "TIMING", "REFERRAL", "HOSTILE")
SUBTYPES = ("PRICING_QUESTION", "PRODUCT_QUESTION", "NOT_NOW", "WRONG_CONTACT", "BOOKING_REQUEST", "UNSUBSCRIBE")


class ReplyState(TypedDict, total=False):
    # ── Inputs (shape of contracts.ReplyStubPayload) ────────────────────────
    opportunity_thread_id: Optional[str]
    from_address: str
    subject: str
    body_text: str
    received_at: str
    avenue: Optional[str]  # for objection-library lookup, if known

    # ── Derived ──────────────────────────────────────────────────────────────
    reply_id: str
    conversation: List[Dict[str, Any]]
    intent: Optional[str]
    subtype: Optional[str]
    response_subject: Optional[str]
    response_body: Optional[str]
    status: str  # 'pending_approval' | 'manual_review' | 'suppressed'
    tokens_used: int
    cost_usd: float

    # ── Outcome (main_graph reads these — see main_graph._node_route) ────────
    terminal_status: str
    reject_reason: Optional[str]


def _make_node_match_thread(db: Optional[Session]):
    def _node_match_thread(state: ReplyState) -> ReplyState:
        """
        A real inbound reply arrives with only a from_address — the producer
        doesn't know which opportunity_thread_id it belongs to. Resolves it by
        matching from_address against every draft's contact_email
        (store.find_opportunity_thread_id_by_email); unmatched goes to
        manual_review, never guessed. A caller that already knows the
        opportunity_thread_id (tests, backfill, the seeded-reply fixtures) can
        still supply it directly and skip this lookup.
        """
        reply_id = store.new_reply_id()
        thread_id = state.get("opportunity_thread_id")
        if not thread_id:
            thread_id = store.find_opportunity_thread_id_by_email(db, state.get("from_address", ""))
            if not thread_id:
                return {"reply_id": reply_id, "status": "manual_review"}
            return {"reply_id": reply_id, "opportunity_thread_id": thread_id}
        return {"reply_id": reply_id}

    return _node_match_thread


def _make_node_load_conversation(db: Optional[Session]):
    def _node_load_conversation(state: ReplyState) -> ReplyState:
        if state.get("status") == "manual_review":
            return {}
        conversation = store.read_conversation(db, state["opportunity_thread_id"])
        if not conversation:
            # A thread_id was supplied but Cora has no record of ever drafting to
            # it — treat as unmatched rather than guessing.
            return {"status": "manual_review"}
        return {"conversation": conversation}

    return _node_load_conversation


_CLASSIFY_SCHEMA = {
    "name": "classify_reply",
    "description": "Classify a prospect's reply intent.",
    "input_schema": {
        "type": "object",
        "properties": {
            "intent": {"type": "string", "enum": list(TOP_LEVEL_INTENTS)},
            "subtype": {"type": ["string", "null"], "enum": list(SUBTYPES) + [None]},
        },
        "required": ["intent"],
    },
}


def _make_node_classify_intent(db: Optional[Session]):
    def _node_classify_intent(state: ReplyState) -> ReplyState:
        if state.get("status") == "manual_review":
            return {}

        system = (
            "Classify the intent of this reply to a cold-outreach email. Top-level intent must be exactly "
            "one of: INTERESTED, OBJECTION, TIMING, REFERRAL, HOSTILE. If a more specific subtype applies, "
            "supply it: PRICING_QUESTION, PRODUCT_QUESTION, NOT_NOW, WRONG_CONTACT, BOOKING_REQUEST, "
            "UNSUBSCRIBE. UNSUBSCRIBE takes priority over any other subtype if the reply asks to stop "
            "being contacted, opt out, or unsubscribe in any form."
        )
        user = f"Subject: {state.get('subject', '')}\n\nBody:\n{state.get('body_text', '')}"

        try:
            result = call_claude_with_usage(
                task_type="cora_reply_classify",
                messages=[{"role": "user", "content": user}],
                system=system,
                max_tokens=100,
                graph_name="cora_reply",
                db=db,
                tools=[_CLASSIFY_SCHEMA],
                tool_choice={"type": "tool", "name": "classify_reply"},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("reply.classify: Claude call failed: %s", exc)
            return {"status": "manual_review"}

        tool_input = result.get("tool_input") or {}
        intent = tool_input.get("intent")
        if intent not in TOP_LEVEL_INTENTS:
            return {"status": "manual_review"}

        return {
            "intent": intent,
            "subtype": tool_input.get("subtype"),
            "tokens_used": int(result.get("input_tokens", 0)) + int(result.get("output_tokens", 0)),
            "cost_usd": float(result.get("cost_usd", 0.0)),
        }

    return _node_classify_intent


def _make_handle_unsubscribe(db: Optional[Session]):
    def _handle_unsubscribe(state: ReplyState) -> ReplyState:
        # Deliberately no try/except here: suppress_contact() is a compliance
        # write, not a best-effort side-effect. If it fails, the exception
        # must propagate all the way to worker.py's _process_one, which
        # leaves the event unacked for Redis Streams to redeliver — a failed
        # opt-out must be retried, never silently treated as done.
        if db is not None:
            from src.services.email_suppression import suppress_contact
            suppress_contact(db, email=state.get("from_address"), source="cora_reply_unsubscribe")
        if state.get("opportunity_thread_id"):
            opportunity_state.mark_closed(state["opportunity_thread_id"], reason="unsubscribed")
        return {"status": "suppressed"}

    return _handle_unsubscribe


def _make_node_compose_response(db: Optional[Session]):
    handle_unsubscribe = _make_handle_unsubscribe(db)

    def _node_compose_response(state: ReplyState) -> ReplyState:
        if state.get("status") in ("manual_review",):
            return {}
        if state.get("subtype") == "UNSUBSCRIBE":
            return handle_unsubscribe(state)

        from config.cora_objection_library import get_objections_for_avenue

        objections = get_objections_for_avenue(state.get("avenue") or "") if state.get("intent") == "OBJECTION" else []
        system = (
            "Draft a short reply to a prospect who has responded to a cold outreach email. Never invent "
            "facts not present in the prior conversation. Never mention pricing or fee mechanics for "
            "hard-money/lender offers unless already present in the prior conversation. If intent is "
            "INTERESTED, include both a calendar-booking ask and next step in the same message — never "
            "'let me send details'. Under 100 words. Output exactly two lines: 'SUBJECT: <subject>' then "
            "'BODY: <body>'."
        )
        objection_hint = ""
        if objections:
            objection_hint = "\n\nKnown objection-response strategies for this avenue:\n" + "\n".join(
                f"- {o['objection']}: {o['response_strategy']}" for o in objections
            )

        lesson_block = ""
        if db is not None:
            context = {
                k: v for k, v in {
                    "offer": state.get("offer"),
                    "avenue": state.get("avenue"),
                }.items() if v is not None
            }
            lessons = fetch_lessons(db, agent_domain="cora", context=context)
            lesson_block = format_lessons_for_prompt(lessons)

        if lesson_block:
            system = system + "\n\n" + lesson_block

        user = (
            f"Intent: {state.get('intent')} (subtype: {state.get('subtype')})\n"
            f"Their reply: {state.get('body_text', '')}\n"
            f"Prior conversation (JSON): {json.dumps(state.get('conversation', []), default=str)[:2000]}"
            f"{objection_hint}"
        )

        try:
            result = call_claude_with_usage(
                task_type="cora_reply_compose",
                messages=[{"role": "user", "content": user}],
                system=system,
                max_tokens=300,
                graph_name="cora_reply",
                db=db,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("reply.compose: Claude call failed: %s", exc)
            return {"status": "manual_review"}

        from src.agents.cora.subgraphs.outreach import _parse_compose_output
        subject, body = _parse_compose_output(result["text"])
        if not body:
            return {"status": "manual_review"}

        return {
            "response_subject": subject,
            "response_body": body,
            "status": "pending_approval",
            "tokens_used": state.get("tokens_used", 0) + int(result.get("input_tokens", 0)) + int(result.get("output_tokens", 0)),
            "cost_usd": state.get("cost_usd", 0.0) + float(result.get("cost_usd", 0.0)),
        }

    return _node_compose_response


def _publish_call_booked(opportunity_thread_id: str, db: Optional[Session], call_booked_at: str) -> bool:
    """
    Real trigger for C4's call.booked event — a prospect replying with
    BOOKING_REQUEST *is* Cora's own booking signal, since Cora has no real
    calendar-confirmation webhook of its own (its booking_link is a static
    Calendly URL, not something that reports back a scheduled_for time).
    Deliberately NOT wired to src.api.main.py's Synthflow demo_requested
    webhook — that fires for property-owner/lead calls in the OLD Lifecycle
    GHL pipeline, a different population than Cora's Hunter buyer_entities,
    with no path back to an opportunity_thread_id at all.
    scheduled_for is left None for the same reason — there is no real
    calendar confirmation to read one from yet.

    call_booked_at is passed in by the caller (the triggering reply's own
    received_at) rather than generated fresh here — a retry of a previously
    failed publish must reuse the exact same idempotency_key, not mint a new
    one every attempt, or the worker's dedup can never recognize it.

    Returns True only on a confirmed successful publish. Callers must persist
    this outcome (ReplyRecord.published / store.mark_reply_published) instead
    of treating "no exception" as success — PR #180 finding: this used to
    swallow both a failed queue.publish() (returns None, doesn't raise) and
    any other exception behind a bare log line, permanently dropping the
    booking signal with nothing to retry it.
    """
    if db is None:
        logger.warning("reply.persist: no db session available — cannot publish call.booked for thread_id=%s", opportunity_thread_id)
        return False

    from src.agents.cora import queue
    from src.agents.cora.tools.read_tools import get_buyer_entity_by_opportunity_thread_id

    try:
        buyer_entity = get_buyer_entity_by_opportunity_thread_id(db, opportunity_thread_id)
        if buyer_entity is None:
            logger.warning("reply.persist: opportunity_thread_id=%s not resolvable — skipping call.booked", opportunity_thread_id)
            return False

        payload = {
            "opportunity_thread_id": opportunity_thread_id,
            "call_booked_at": call_booked_at,
            "rep": None,
            "scheduled_for": None,
            "buyer_entity": buyer_entity,
        }
        idempotency_key = queue.make_idempotency_key("call.booked", opportunity_thread_id, call_booked_at)
        message_id = queue.publish("call.booked", payload, idempotency_key=idempotency_key)
        if message_id is None:
            logger.warning(
                "reply.persist: call.booked publish failed (queue unavailable) for thread_id=%s — will retry via sweep",
                opportunity_thread_id,
            )
            return False
        logger.info("reply.persist: published call.booked for thread_id=%s (BOOKING_REQUEST reply)", opportunity_thread_id)
        return True
    except Exception:
        logger.exception("reply.persist: failed to publish call.booked for thread_id=%s — will retry via sweep", opportunity_thread_id)
        return False


def retry_unpublished_call_booked(db: Optional[Session]) -> int:
    """
    Periodic sweep (wired into __main__.py --serve) — retries call.booked
    publishes that failed at persist time, without re-running the reply
    subgraph's classify/compose steps or minting a duplicate ReplyRecord.
    Only BOOKING_REQUEST replies that reached pending_approval and never got
    a confirmed publish are eligible; received_at is reused as call_booked_at
    so the idempotency_key is identical to whatever the original attempt used.
    """
    retried = 0
    for record in store.read_replies():
        if (
            record.get("subtype") == "BOOKING_REQUEST"
            and record.get("status") == "pending_approval"
            and not record.get("published")
            and record.get("opportunity_thread_id")
        ):
            if _publish_call_booked(record["opportunity_thread_id"], db, call_booked_at=record.get("received_at", "")):
                store.mark_reply_published(record["reply_id"])
                retried += 1
    return retried


def _make_node_persist(db: Optional[Session]):
    def _node_persist(state: ReplyState) -> ReplyState:
        # Attempted BEFORE the record is built/appended so a successful
        # publish is reflected in the very first (and normally only) write —
        # avoids a second corrective append in the common case. A failure
        # here is captured in `published=False` and picked up later by
        # retry_unpublished_call_booked, never by re-running this graph.
        call_booked_published = False
        if (
            state.get("status") == "pending_approval"
            and state.get("opportunity_thread_id")
            and state.get("subtype") == "BOOKING_REQUEST"
        ):
            call_booked_published = _publish_call_booked(
                state["opportunity_thread_id"], db, call_booked_at=state.get("received_at", ""),
            )

        record = store.ReplyRecord(
            reply_id=state["reply_id"],
            opportunity_thread_id=state.get("opportunity_thread_id"),
            from_address=state.get("from_address", ""),
            subject=state.get("subject", ""),
            body_text=state.get("body_text", ""),
            received_at=state.get("received_at", ""),
            intent=state.get("intent"),
            subtype=state.get("subtype"),
            response_subject=state.get("response_subject"),
            response_body=state.get("response_body"),
            status=state.get("status", "manual_review"),
            published=call_booked_published,
        )
        store.append_reply(record)

        if state.get("status") == "pending_approval" and state.get("opportunity_thread_id"):
            opportunity_state.mark_replied(state["opportunity_thread_id"], reason="reply_received")
            if db is not None:
                try:
                    from src.services.agent_lane_experiment_engine import record_outcome
                    from config.prompt_variants import PROMPT_EXPERIMENT_NAME
                    record_outcome(state["opportunity_thread_id"], PROMPT_EXPERIMENT_NAME, "reply", db)
                except Exception:
                    logger.warning(
                        "reply: failed to record prompt-experiment outcome for thread=%s — continuing",
                        state.get("opportunity_thread_id"), exc_info=True,
                    )
            fleet_event = contracts.make_fleet_event(
                "action.ready", state["opportunity_thread_id"], reply_id=state["reply_id"],
            )
            contracts.emit_fleet_event_stub(fleet_event)

        status = state.get("status", "manual_review")
        # Reaching persist without an uncaught exception IS the graph's success
        # case, regardless of business status — manual_review/suppressed are
        # legitimate outcomes, not graph failures. main_graph._node_route reads
        # terminal_status generically across all three subgraphs.
        return {
            "terminal_status": "completed",
            "reject_reason": status if status != "pending_approval" else None,
        }

    return _node_persist


def _after_match(state: ReplyState) -> str:
    return "persist" if state.get("status") == "manual_review" else "load_conversation"


def _after_load(state: ReplyState) -> str:
    return "persist" if state.get("status") == "manual_review" else "classify_intent"


def _after_classify(state: ReplyState) -> str:
    return "persist" if state.get("status") == "manual_review" else "compose_response"


def build_reply_graph(db: Optional[Session] = None) -> StateGraph:
    # `db` is captured by closure into the nodes that need it, never placed in
    # graph state — see outreach.py's build_outreach_graph for why.
    g = StateGraph(ReplyState)
    g.add_node("match_thread", _make_node_match_thread(db))
    g.add_node("load_conversation", _make_node_load_conversation(db))
    g.add_node("classify_intent", _make_node_classify_intent(db))
    g.add_node("compose_response", _make_node_compose_response(db))
    g.add_node("persist", _make_node_persist(db))

    g.add_edge(START, "match_thread")
    g.add_conditional_edges("match_thread", _after_match, {"load_conversation": "load_conversation", "persist": "persist"})
    g.add_conditional_edges("load_conversation", _after_load, {"classify_intent": "classify_intent", "persist": "persist"})
    g.add_conditional_edges("classify_intent", _after_classify, {"compose_response": "compose_response", "persist": "persist"})
    g.add_edge("compose_response", "persist")
    g.add_edge("persist", END)
    return g


def run_reply(inputs: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    inputs = dict(inputs)
    inputs.pop("db", None)
    graph = build_reply_graph(db).compile()
    final = graph.invoke(inputs)
    return dict(final)


CALL_BOOKED_RETRY_INTERVAL_SECONDS = 3 * 60


def run_periodic(stop_event, interval_seconds: int = CALL_BOOKED_RETRY_INTERVAL_SECONDS) -> None:
    """Wired into __main__.py --serve — retries any call.booked publishes that failed at persist time."""
    from src.core.database import get_db_context

    logger.info("reply.retry_unpublished_call_booked: starting periodic sweep every %ds", interval_seconds)
    while not stop_event.is_set():
        try:
            with get_db_context() as db:
                retried = retry_unpublished_call_booked(db)
                if retried:
                    logger.info("reply.retry_unpublished_call_booked: republished %d call.booked event(s)", retried)
        except Exception:  # noqa: BLE001
            logger.exception("reply.retry_unpublished_call_booked: sweep failed — will retry next interval")
        stop_event.wait(interval_seconds)
    logger.info("reply.retry_unpublished_call_booked: stopped")
