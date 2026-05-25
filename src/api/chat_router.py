"""
Concierge Chat API router (M5a — pre-signup conversion chat).

Endpoints:
  POST /api/chat/sessions                    — issue anonymous session (HttpOnly cookie)
  POST /api/chat/sessions/{id}/link          — link anonymous session to subscriber
  POST /api/chat/messages                    — send a user message; returns intent + optional payment_event
  GET  /api/chat/stream?session_id=&turn_id= — SSE stream for assistant response chunks
  POST /api/chat/sessions/{id}/escalate      — open human-handoff record

Rate limits (from rate_limit.py enforce_or_429):
  Anonymous:  20 messages / 10 min / IP+session   scope="chat_anonymous"
  Subscriber: 60 messages / 10 min / subscriber   scope="chat_subscriber"
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import ChatMessage, ChatSession, Subscriber
from src.services.concierge_chat import handle_user_turn
from src.services.rate_limit import enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chat", tags=["chat"])

SESSION_COOKIE = "chat_session_id"
SESSION_TTL_DAYS = 30


# ── DB dependency ─────────────────────────────────────────────────────────────

def get_db():
    with get_db_context() as db:
        yield db


# ── Pydantic models ───────────────────────────────────────────────────────────

class SendMessageRequest(BaseModel):
    session_id: Optional[str] = None
    content: str
    mode: str = "pre_signup"  # pre_signup | post_signup
    feed_uuid: Optional[str] = None


class LinkSessionRequest(BaseModel):
    subscriber_id: int


class EscalateRequest(BaseModel):
    reason: str = "user_requested"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _resolve_session_id(
    body_session_id: Optional[str],
    cookie_session_id: Optional[str],
) -> Optional[str]:
    return body_session_id or cookie_session_id


def _get_or_create_session(
    session_id: Optional[str],
    mode: str,
    db: Session,
    response: Response,
) -> ChatSession:
    if session_id:
        session = db.execute(
            select(ChatSession).where(ChatSession.id == session_id)
        ).scalar_one_or_none()
        if session:
            return session

    # Create new session
    new_id = str(uuid.uuid4())
    source = "landing" if mode == "pre_signup" else "dashboard"
    session = ChatSession(
        id=new_id,
        anonymous_id=str(uuid.uuid4()),
        source=source,
        created_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    db.add(session)
    db.flush()

    response.set_cookie(
        key=SESSION_COOKIE,
        value=new_id,
        max_age=SESSION_TTL_DAYS * 86400,
        httponly=True,
        samesite="lax",
        secure=True,
    )
    return session


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/sessions")
def create_session(
    request: Request,
    response: Response,
    mode: str = "pre_signup",
    db: Session = Depends(get_db),
):
    """Issue a new chat_session_id. Sets an HttpOnly cookie."""
    enforce_or_429(request, scope="chat_anonymous", limit=10, window_seconds=60)
    session = _get_or_create_session(None, mode, db, response)
    db.commit()
    return {"session_id": session.id}


@router.post("/sessions/{session_id}/link")
def link_session(
    session_id: str,
    body: LinkSessionRequest,
    db: Session = Depends(get_db),
):
    """Link an anonymous session to a subscriber after signup. Idempotent."""
    session = db.execute(
        select(ChatSession).where(ChatSession.id == session_id)
    ).scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if session.subscriber_id and session.subscriber_id == body.subscriber_id:
        return {"ok": True}  # already linked

    session.subscriber_id = body.subscriber_id
    session.linked_at = datetime.now(timezone.utc)
    db.commit()
    return {"ok": True}


@router.post("/sessions/{session_id}/escalate")
def escalate_session(
    session_id: str,
    body: EscalateRequest,
    db: Session = Depends(get_db),
):
    """
    Open a human-handoff record. Routes by subscriber tier:
      - annual_lock / autopilot_lite / autopilot_pro → human_close_routing
      - all others → support_email
    """
    session = db.execute(
        select(ChatSession).where(ChatSession.id == session_id)
    ).scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    route = "support_email"
    sub_tier = "anon"
    if session.subscriber_id:
        sub = db.execute(
            select(Subscriber).where(Subscriber.id == session.subscriber_id)
        ).scalar_one_or_none()
        if sub:
            sub_tier = sub.tier or "unknown"
            if sub.tier in {"annual_lock", "autopilot_lite", "autopilot_pro"}:
                route = "human_close_routing"

    logger.info(
        "chat: escalation session=%s subscriber=%s tier=%s reason=%s route=%s",
        session_id, session.subscriber_id, sub_tier, body.reason, route,
    )
    return {"route": route, "ok": True}


@router.post("/messages")
def send_message(
    body: SendMessageRequest,
    request: Request,
    response: Response,
    chat_session_id: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    """
    Accept a user message. Runs intent classification + queues assistant turn.
    Returns turn metadata; the assistant text arrives via GET /api/chat/stream.
    """
    # Rate limit
    if body.mode == "pre_signup":
        enforce_or_429(request, scope="chat_anonymous", limit=20, window_seconds=600)
    else:
        enforce_or_429(request, scope="chat_subscriber", limit=60, window_seconds=600)

    if not body.content or not body.content.strip():
        raise HTTPException(status_code=422, detail="Message content is required")

    session_id = _resolve_session_id(body.session_id, chat_session_id)

    # Resolve subscriber for post-signup mode
    subscriber_id: Optional[int] = None
    if body.mode == "post_signup" and body.feed_uuid:
        sub = db.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == body.feed_uuid)
        ).scalar_one_or_none()
        if sub:
            subscriber_id = sub.id

    with get_db_context() as turn_db:
        session = _get_or_create_session(session_id, body.mode, turn_db, response)
        session_id = session.id

        try:
            turn = handle_user_turn(
                session_id=session_id,
                user_text=body.content.strip(),
                mode=body.mode,
                db=turn_db,
                subscriber_id=subscriber_id,
            )
            turn_db.commit()
        except Exception as exc:
            logger.error("chat: handle_user_turn failed session=%s: %s", session_id, exc, exc_info=True)
            turn_db.rollback()
            raise HTTPException(status_code=500, detail="Chat service error")

    payment_event = None
    if turn.payment_event:
        payment_event = {
            "type": "payment_sheet",
            "sku": turn.payment_event.sku,
            "zip": turn.payment_event.zip,
            "source": turn.payment_event.source,
        }
        if turn.payment_event.deeplink_after:
            payment_event["deeplink_after"] = turn.payment_event.deeplink_after

    return {
        "session_id": session_id,
        "turn_id": turn.message_id,
        "content": turn.content,
        "intent": {
            "label": turn.intent.label,
            "confidence": round(turn.intent.confidence, 3),
        },
        "payment_event": payment_event,
        "waitlist_zip": turn.waitlist_zip,
    }


@router.get("/stream")
async def stream_response(
    request: Request,
    session_id: str,
    turn_id: int,
    db: Session = Depends(get_db),
):
    """
    SSE endpoint. Streams the assistant response for a turn.

    Events:
      data: {"type":"chunk","text":"..."}\n\n
      data: {"type":"done"}\n\n
      data: {"type":"error","message":"..."}\n\n
    """
    # Fetch the assistant message row
    msg = db.execute(
        select(ChatMessage).where(
            ChatMessage.id == turn_id,
            ChatMessage.session_id == session_id,
            ChatMessage.role == "assistant",
        )
    ).scalar_one_or_none()

    if not msg:
        raise HTTPException(status_code=404, detail="Turn not found")

    async def event_generator():
        try:
            text = msg.content or ""
            # Simulate streaming by emitting ~40-char chunks with a tiny delay
            chunk_size = 40
            for i in range(0, len(text), chunk_size):
                chunk = text[i:i + chunk_size]
                yield f"data: {json.dumps({'type': 'chunk', 'text': chunk})}\n\n"
                await asyncio.sleep(0.012)
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as exc:
            logger.error("chat: SSE stream error: %s", exc)
            yield f"data: {json.dumps({'type': 'error', 'message': 'Stream error'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
