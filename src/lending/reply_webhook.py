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
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_after_hours_conversation import CHANNEL_SMS, SOURCE_INBOUND_TEXT
from src.api.deps import get_db
from src.api.lending_ghl_router import _verify_secret
from src.lending.after_hours_conversation import (OUTCOME_ANSWERED, OUTCOME_NO_CONVERSATION, OUTCOME_REPROMPT,
                                                  OUTCOME_SLOT_CHOSEN, conversations_enabled,
                                                  inbound_text_inquiry_key, open_conversation, record_reply)
from src.lending.after_hours_responder import run_send_cycle_now
from src.lending.instantly_replies import is_stop_request
from src.lending.payload_shape import log_shape
from src.lending.reply_guard import ReplyEvent, handle_reply_event, in_reply_hours, parse_event, slack_poster

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])

_LENDINGFLOW_LEAD = text("SELECT EXISTS (SELECT 1 FROM lending.lendingflow_leads WHERE phone = :p)")


def _open_for_inbound_text(db: Session, event: ReplyEvent, now: datetime) -> bool:
    """An inbound text outside Josh's hours, from someone with no open after-hours conversation, starts one: they
    texted first and are waiting for an answer. A LendingFlow lead is left out (the intro text and the speed-to-lead
    queue already reach them), and so is a stop request.

    Text consent is not required here: GHL reports the consent this text carries on a separate webhook that can
    arrive after this one. The responder holds the first question until the consent is recorded, for
    ``CONSENT_WAIT_MINUTES``, and closes the conversation unsent if it never is. A failure raises, so GHL
    redelivers the text; the inquiry key is the message id, so a redelivery never opens a second conversation."""
    if not event.phone or in_reply_hours(now) or is_stop_request(event.body):
        return False
    if db.execute(_LENDINGFLOW_LEAD, {"p": event.phone}).scalar():
        return False
    return open_conversation(db, inquiry_key=inbound_text_inquiry_key(event.message_id), source=SOURCE_INBOUND_TEXT,
                             channel=CHANNEL_SMS, phone=event.phone, ghl_contact_id=event.contact_id,
                             first_name=event.first_name, now=now) is not None


def _advance_after_hours_conversation(db: Session, event: ReplyEvent) -> Optional[bool]:
    """An inbound message from a lead in an open after-hours conversation is that conversation's next answer; an
    inbound text after hours from a lead with none starts one. True when it made a message or the booking due,
    False when it changed nothing, None when it could not be saved (an answer, or the opening of a conversation):
    the caller still runs the handoff, then answers with an error so GHL redelivers the message (an applied message
    is never applied twice)."""
    if event.direction != "inbound" or not conversations_enabled():
        return False
    try:
        now = datetime.now(timezone.utc)
        outcome = record_reply(db, message_id=event.message_id, body=event.body, phone=event.phone,
                               ghl_contact_id=event.contact_id, now=now)
        opened = outcome == OUTCOME_NO_CONVERSATION and _open_for_inbound_text(db, event, now)
        db.commit()
        return opened or outcome in (OUTCOME_ANSWERED, OUTCOME_REPROMPT, OUTCOME_SLOT_CHOSEN)
    except Exception as exc:  # class only: the message text can carry personal details
        logger.error("[reply-webhook] after-hours conversation for message %s failed (%s)", event.message_id,
                     type(exc).__name__)
        db.rollback()
        return None


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
    advanced = _advance_after_hours_conversation(db, event)
    if advanced:
        background.add_task(run_send_cycle_now)  # the next question, right after the response
    try:
        outcome = handle_reply_event(db, event, poster=slack_poster())
    except Exception as exc:  # class only: the message text can carry personal details
        logger.error("[reply-webhook] handling message %s failed (%s)", event.message_id, type(exc).__name__)
        db.rollback()
        raise HTTPException(status_code=502, detail="Could not post the handoff; it will be retried") from None
    if outcome == "not_configured":
        raise HTTPException(status_code=503, detail="Reply handoff channel is not configured; it will be retried")
    if advanced is None:
        # The answer is not saved and nothing else replays it: a 200 here would lose it for good.
        raise HTTPException(status_code=503, detail="The reply could not be saved; it will be retried")
    return {"status": outcome}
