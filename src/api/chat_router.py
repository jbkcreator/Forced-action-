"""
Concierge Chat API router — minimal PDF/Markdown-grounded variant.

Endpoints:
  POST /api/chat/sessions   — create a chat session (sets HttpOnly cookie)
  POST /api/chat/messages   — send a user message; returns {session_id, reply}

The knowledge base is a single Markdown file loaded once at startup
(see src.services.chat_knowledge).
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.api.deps import get_db
from src.core.models import ChatSession
from src.services.concierge_chat import handle_user_turn
from src.services.rate_limit import enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chat", tags=["chat"])

SESSION_COOKIE = "chat_session_id"
SESSION_TTL_DAYS = 30


class SendMessageRequest(BaseModel):
    session_id: Optional[str] = None
    content: str


def _get_or_create_session(
    session_id: Optional[str],
    db: Session,
    response: Response,
) -> ChatSession:
    if session_id:
        existing = db.execute(
            select(ChatSession).where(ChatSession.id == session_id)
        ).scalar_one_or_none()
        if existing:
            return existing

    new_id = str(uuid.uuid4())
    session = ChatSession(
        id=new_id,
        anonymous_id=str(uuid.uuid4()),
        source="landing",
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


@router.post("/sessions")
def create_session(
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
):
    """Create a new chat session. Sets an HttpOnly cookie."""
    enforce_or_429(request, scope="chat_anonymous", limit=10, window_seconds=60)
    session = _get_or_create_session(None, db, response)
    db.commit()
    return {"session_id": session.id}


@router.post("/messages")
def send_message(
    body: SendMessageRequest,
    request: Request,
    response: Response,
    chat_session_id: Optional[str] = Cookie(default=None),
):
    """Send a user message; returns the assistant reply grounded in the knowledge base."""
    enforce_or_429(request, scope="chat_anonymous", limit=20, window_seconds=600)

    if not body.content or not body.content.strip():
        raise HTTPException(status_code=422, detail="Message content is required")

    session_id = body.session_id or chat_session_id

    with get_db_context() as db:
        session = _get_or_create_session(session_id, db, response)
        try:
            turn = handle_user_turn(
                session_id=session.id,
                user_text=body.content,
                db=db,
            )
            db.commit()
        except Exception as exc:
            logger.error(
                "chat: handle_user_turn failed session=%s: %s",
                session.id, exc, exc_info=True,
            )
            db.rollback()
            raise HTTPException(status_code=500, detail="Chat service error")

        return {
            "session_id": session.id,
            "reply": turn.content,
            "followups": turn.followups,
        }
