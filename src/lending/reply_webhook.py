"""WP-GL-10: GHL reply webhook -> Slack handoff / quoted-number alert (see reply_guard).

A GHL workflow ("Customer replied" and "Conversation AI message sent" -> Webhook) POSTs each message here.
Auth: the same ``X-Webhook-Secret`` as the other lending GHL webhooks; closed while the secret is unset.
UNVERIFIED: GHL's payload field names are from its public reference, not a captured webhook.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.api.lending_ghl_router import _verify_secret
from src.lending.after_hours_conversation import (OUTCOME_ANSWERED, OUTCOME_REPROMPT, OUTCOME_SLOT_CHOSEN,
                                                  conversations_enabled, record_reply)
from src.lending.after_hours_responder import run_send_cycle_now
from src.lending.payload_shape import log_shape
from src.lending.reply_guard import ReplyEvent, handle_reply_event, parse_event, slack_poster

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


def _advance_after_hours_conversation(db: Session, event: ReplyEvent) -> bool:
    """An inbound message from a lead in an open after-hours conversation is that conversation's next answer. True
    when it made the next message or the booking due. A failure here never blocks the handoff below: GHL redelivers, and an
    applied message is not applied twice."""
    if event.direction != "inbound" or not conversations_enabled():
        return False
    try:
        outcome = record_reply(db, message_id=event.message_id, body=event.body, phone=event.phone,
                               ghl_contact_id=event.contact_id, now=datetime.now(timezone.utc))
        db.commit()
        return outcome in (OUTCOME_ANSWERED, OUTCOME_REPROMPT, OUTCOME_SLOT_CHOSEN)
    except Exception as exc:  # class only: the message text can carry personal details
        logger.error("[reply-webhook] after-hours conversation for message %s failed (%s)", event.message_id,
                     type(exc).__name__)
        db.rollback()
        return False


@router.post("/ghl-reply")
def ghl_reply(
    body: dict[str, Any],
    background: BackgroundTasks,
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _verify_secret(x_webhook_secret)
    log_shape("ghl-reply", body)
    event = parse_event(body)
    if event is None:
        return {"status": "ignored", "reason": "no_message"}
    if _advance_after_hours_conversation(db, event):
        background.add_task(run_send_cycle_now)  # the next question, right after the response
    try:
        outcome = handle_reply_event(db, event, poster=slack_poster())
    except Exception as exc:  # class only: the message text can carry personal details
        logger.error("[reply-webhook] handling message %s failed (%s)", event.message_id, type(exc).__name__)
        db.rollback()
        raise HTTPException(status_code=502, detail="Could not post the handoff; it will be retried") from None
    if outcome == "not_configured":
        raise HTTPException(status_code=503, detail="Reply handoff channel is not configured; it will be retried")
    return {"status": outcome}
