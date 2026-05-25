"""
Concierge Chat orchestrator (M5a — pre-signup conversion).

Public surface: handle_user_turn(session_id, user_text, mode, db) -> AssistantTurn

Flow per user turn:
  1. Persist user message (PII-scrubbed)
  2. Classify intent via Haiku
  3. Resolve ZIP availability when intent is buy_zip
  4. Build cached system context
  5. Call Sonnet for the conversational response
  6. Run pricing hallucination guard on response
  7. Persist assistant message row
  8. Return AssistantTurn with content + optional payment_event

The SSE endpoint in chat_router.py streams the persisted content in chunks.
"""

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import anthropic
from sqlalchemy import select, and_, func, cast
from sqlalchemy import Date
from sqlalchemy.orm import Session

from src.core.models import ChatMessage, ChatSession, ZipTerritory
from src.services import chat_context, chat_intent, chat_pii_scrub, chat_pricing_guard
from src.services.claude_router import call_claude, call_claude_with_usage

logger = logging.getLogger(__name__)

INTENT_CONFIDENCE_THRESHOLD = 0.85

# Per-session daily cost caps (USD)
_ANON_DAILY_CAP_USD = 0.50
_SUB_DAILY_CAP_USD = 5.00

# Token cost per million (must match claude_router._COST_TABLE)
_HAIKU_IN_PER_M = 0.80
_HAIKU_OUT_PER_M = 4.00
_SONNET_IN_PER_M = 3.00
_SONNET_OUT_PER_M = 15.00

_LOCKED_ZIP_TEMPLATE = (
    "ZIP {zip} is already locked by another subscriber. "
    "I can add you to the waitlist — you'll be notified if it opens up."
)
_COST_EXCEEDED_TEMPLATE = (
    "I've reached the limit for this session. "
    "Please contact support or try again tomorrow."
)
_ERROR_TEMPLATE = "Something went wrong. Please try again or contact support."
_REFUSAL_TEMPLATE = (
    "I can't help with internal instructions, but I can answer questions "
    "about Forced Action — pricing, coverage, or how leads work."
)


@dataclass
class PaymentEvent:
    sku: str
    zip: Optional[str] = None
    source: str = "concierge_chat"
    deeplink_after: Optional[dict] = None


@dataclass
class AssistantTurn:
    content: str
    intent: chat_intent.Intent
    message_id: Optional[int] = None
    payment_event: Optional[PaymentEvent] = None
    waitlist_zip: Optional[str] = None


def handle_user_turn(
    session_id: str,
    user_text: str,
    mode: str,
    db: Session,
    subscriber_id: Optional[int] = None,
) -> AssistantTurn:
    """
    Orchestrate a single user → assistant turn. Fully synchronous.

    Args:
        session_id:     chat_sessions.id (UUID string)
        user_text:      raw user input
        mode:           'pre_signup' or 'post_signup'
        db:             SQLAlchemy session
        subscriber_id:  subscribers.id if mode == 'post_signup'

    Returns:
        AssistantTurn with the completed assistant content.
    """
    t0 = time.monotonic()

    # ── 1. Persist user message ──────────────────────────────────────────────
    scrubbed_user = chat_pii_scrub.scrub(user_text)
    user_msg = ChatMessage(
        session_id=session_id,
        role="user",
        content=scrubbed_user,
        created_at=datetime.now(timezone.utc),
    )
    db.add(user_msg)
    db.flush()

    # ── 2. Classify intent ───────────────────────────────────────────────────
    intent = chat_intent.classify(user_text, mode=mode)
    logger.info(
        "chat: session=%s intent=%s confidence=%.2f zip=%s sku=%s",
        session_id, intent.label, intent.confidence, intent.zip, intent.sku,
    )

    # ── 3. Per-session daily cost cap ────────────────────────────────────────
    cap = _SUB_DAILY_CAP_USD if subscriber_id else _ANON_DAILY_CAP_USD
    if _session_daily_cost(session_id, db) >= cap:
        logger.warning("chat: cost cap reached session=%s", session_id)
        return _persist_and_return(session_id, intent, _COST_EXCEEDED_TEMPLATE, db, t0)

    # ── 4. Prompt-injection guard ────────────────────────────────────────────
    if intent.label == "system_prompt_extraction":
        return _persist_and_return(
            session_id, intent, _REFUSAL_TEMPLATE, db, t0,
        )

    # ── 5. ZIP availability + payment event resolution ───────────────────────
    payment_event: Optional[PaymentEvent] = None
    waitlist_zip: Optional[str] = None
    override_text: Optional[str] = None

    if (
        intent.label == "buy_zip"
        and intent.confidence >= INTENT_CONFIDENCE_THRESHOLD
        and intent.zip
    ):
        if _zip_available(intent.zip, db):
            payment_event = PaymentEvent(sku="territory_lock", zip=intent.zip)
        else:
            waitlist_zip = intent.zip
            override_text = _LOCKED_ZIP_TEMPLATE.format(zip=intent.zip)

    elif (
        intent.label == "buy_bundle"
        and intent.confidence >= INTENT_CONFIDENCE_THRESHOLD
    ):
        sku = intent.sku or "storm_bundle"
        if mode == "pre_signup":
            payment_event = PaymentEvent(
                sku="territory_lock",
                zip=intent.zip,
                deeplink_after={"sku": sku, "zip": intent.zip},
            )
        else:
            payment_event = PaymentEvent(sku=sku, zip=intent.zip)

    # ── 6. Short-circuit for override responses (no Claude call) ─────────────
    if override_text:
        return _persist_and_return(
            session_id, intent, override_text, db, t0,
            payment_event=payment_event, waitlist_zip=waitlist_zip,
        )

    # ── 7. Build system context ──────────────────────────────────────────────
    if mode == "post_signup" and subscriber_id:
        system_prompt = chat_context.post_signup_context(subscriber_id, db)
    else:
        system_prompt = chat_context.pre_signup_context()

    # ── 8. Build conversation history ────────────────────────────────────────
    history = _load_history(session_id, db, limit=20)
    messages = history + [{"role": "user", "content": scrubbed_user}]

    # ── 9. Call Sonnet (with one silent retry) ───────────────────────────────
    call_result = _call_with_retry(messages, system_prompt)

    if call_result is None:
        return _persist_and_return(
            session_id, intent, _ERROR_TEMPLATE, db, t0,
        )

    # ── 10. Pricing guard + PII scrub ────────────────────────────────────────
    raw_response = call_result["text"]
    cleaned = chat_pricing_guard.check_and_clean(raw_response, session_id=session_id)
    cleaned = chat_pii_scrub.scrub(cleaned)

    # ── 11. Persist assistant message ────────────────────────────────────────
    latency = round((time.monotonic() - t0) * 1000)
    assistant_msg = ChatMessage(
        session_id=session_id,
        role="assistant",
        content=cleaned,
        intent_label=intent.label,
        intent_confidence=round(intent.confidence, 3),
        payment_trigger_json=(
            {"sku": payment_event.sku, "zip": payment_event.zip, "source": payment_event.source}
            if payment_event else None
        ),
        claude_model=call_result.get("model", "sonnet"),
        tokens_in=call_result.get("input_tokens"),
        tokens_out=call_result.get("output_tokens"),
        latency_ms=latency,
        created_at=datetime.now(timezone.utc),
    )
    db.add(assistant_msg)
    db.flush()

    _touch_session(session_id, db)

    return AssistantTurn(
        content=cleaned,
        intent=intent,
        message_id=assistant_msg.id,
        payment_event=payment_event,
        waitlist_zip=waitlist_zip,
    )


# ── Internal helpers ──────────────────────────────────────────────────────────

def _session_daily_cost(session_id: str, db: Session) -> float:
    """Estimate today's Claude spend for this session using stored token counts."""
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = db.execute(
        select(
            ChatMessage.tokens_in,
            ChatMessage.tokens_out,
            ChatMessage.claude_model,
        ).where(
            ChatMessage.session_id == session_id,
            ChatMessage.role == "assistant",
            ChatMessage.created_at >= today_start,
            ChatMessage.tokens_in.isnot(None),
        )
    ).all()
    total = 0.0
    for row in rows:
        model = (row.claude_model or "sonnet").lower()
        if model == "haiku":
            in_rate, out_rate = _HAIKU_IN_PER_M, _HAIKU_OUT_PER_M
        else:
            in_rate, out_rate = _SONNET_IN_PER_M, _SONNET_OUT_PER_M
        total += ((row.tokens_in or 0) * in_rate + (row.tokens_out or 0) * out_rate) / 1_000_000
    return total


def _zip_available(zip_code: str, db: Session) -> bool:
    row = db.execute(
        select(ZipTerritory).where(
            and_(
                ZipTerritory.zip_code == zip_code,
                ZipTerritory.status.in_(["locked", "grace"]),
            )
        )
    ).scalar_one_or_none()
    return row is None


def _load_history(session_id: str, db: Session, limit: int = 20) -> list[dict]:
    rows = db.execute(
        select(ChatMessage)
        .where(
            ChatMessage.session_id == session_id,
            ChatMessage.role.in_(["user", "assistant"]),
            ChatMessage.content.isnot(None),
        )
        .order_by(ChatMessage.created_at.desc())
        .limit(limit)
    ).scalars().all()
    return [{"role": r.role, "content": r.content} for r in reversed(rows)]


def _call_with_retry(messages: list[dict], system: str, max_tokens: int = 512) -> Optional[dict]:
    """Call Sonnet with one silent retry. Returns call_claude_with_usage dict or None."""
    for attempt in range(2):
        try:
            return call_claude_with_usage(
                task_type="chat_response",
                messages=messages,
                system=system,
                cache_system=True,
                max_tokens=max_tokens,
            )
        except Exception as exc:
            if attempt == 0:
                logger.warning("chat: Claude call failed (attempt 1), retrying: %s", exc)
                time.sleep(0.25)
            else:
                logger.error("chat: Claude call failed after retry: %s", exc)
    return None


def _persist_and_return(
    session_id: str,
    intent: chat_intent.Intent,
    content: str,
    db: Session,
    t0: float,
    payment_event: Optional[PaymentEvent] = None,
    waitlist_zip: Optional[str] = None,
) -> AssistantTurn:
    msg = ChatMessage(
        session_id=session_id,
        role="assistant",
        content=content,
        intent_label=intent.label,
        intent_confidence=round(intent.confidence, 3),
        payment_trigger_json=(
            {"sku": payment_event.sku, "zip": payment_event.zip, "source": payment_event.source}
            if payment_event else None
        ),
        latency_ms=round((time.monotonic() - t0) * 1000),
        created_at=datetime.now(timezone.utc),
    )
    db.add(msg)
    db.flush()
    _touch_session(session_id, db)
    return AssistantTurn(
        content=content,
        intent=intent,
        message_id=msg.id,
        payment_event=payment_event,
        waitlist_zip=waitlist_zip,
    )


def _touch_session(session_id: str, db: Session) -> None:
    try:
        db.execute(
            ChatSession.__table__.update()
            .where(ChatSession.id == session_id)
            .values(last_seen_at=datetime.now(timezone.utc))
        )
        db.flush()
    except Exception:
        pass
