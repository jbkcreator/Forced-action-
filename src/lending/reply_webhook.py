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
from src.lending import webhook_failures
from src.lending.inbound import journal
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


def _apply_after_hours(db: Session, event: ReplyEvent) -> bool:
    """Apply an inbound message to its open after-hours conversation, or open one for an inbound text after hours.
    True when it made a message or the booking due. Does not commit; raises when it cannot be saved."""
    if event.direction != "inbound" or not conversations_enabled():
        return False
    now = datetime.now(timezone.utc)
    outcome = record_reply(db, message_id=event.message_id, body=event.body, phone=event.phone,
                           ghl_contact_id=event.contact_id, now=now)
    opened = outcome == OUTCOME_NO_CONVERSATION and _open_for_inbound_text(db, event, now)
    return opened or outcome in (OUTCOME_ANSWERED, OUTCOME_REPROMPT, OUTCOME_SLOT_CHOSEN)


def _advance_after_hours_conversation(db: Session, event: ReplyEvent) -> Optional[bool]:
    """An inbound message from a lead in an open after-hours conversation is that conversation's next answer; an
    inbound text after hours from a lead with none starts one. True when it made a message or the booking due,
    False when it changed nothing, None when it could not be saved (an answer, or the opening of a conversation):
    the caller still runs the handoff, then answers with an error so GHL redelivers the message (an applied message
    is never applied twice)."""
    if event.direction != "inbound" or not conversations_enabled():
        return False
    try:
        advanced = _apply_after_hours(db, event)
        db.commit()
        return advanced
    except Exception as exc:  # class only: the message text can carry personal details
        logger.error("[reply-webhook] after-hours conversation for message %s failed (%s)", event.message_id,
                     type(exc).__name__)
        db.rollback()
        return None


# ---- inbound journal (R1 0.5, LENDING_INBOUND_JOURNAL_ENABLED) --------------------------------------------------

JOURNAL_SOURCE = "ghl_reply"
HANDOFF_SOURCE = "ghl_reply_handoff"  # a handoff whose post failed, retried on its own (the answer is already kept)
HANDOFF_SAVED = "handoff_saved_for_retry"


class ReplyHandoffNotConfigured(RuntimeError):
    """A handoff is due but the replies channel or bot token is unset: saved for the 5-minute retry."""


class HandoffNotSaved(RuntimeError):
    """The handoff failed and could not be saved for retry: the whole message is retried instead."""


def handle_reply_handoff(db: Session, raw: dict[str, Any]) -> str:
    """The Slack handoff alone (also the retry of a saved handoff). Never commits; idempotent on the message id.
    Raises when the post fails or the channel is unset."""
    event = parse_event(raw)
    if event is None:
        return "ignored"
    outcome = handle_reply_event(db, event, poster=slack_poster(), commit=False)
    if outcome == "not_configured":
        raise ReplyHandoffNotConfigured("reply handoff channel is not configured")
    return outcome


def handle_ghl_reply(db: Session, raw: dict[str, Any]) -> dict[str, Any]:
    """The journal / replay handler (never commits). The after-hours answer is applied in the caller's transaction;
    the handoff runs in a savepoint, and a failed handoff is saved for its own retry (``ghl_reply_handoff``), so a
    Slack outage or an unset channel never holds back the borrower's answer. Raises when the answer cannot be saved
    or a failed handoff cannot be saved either: the whole message is then retried (both steps are idempotent)."""
    event = parse_event(raw)
    if event is None:
        return {"status": "ignored", "advanced": False}
    advanced = _apply_after_hours(db, event)
    try:
        with db.begin_nested():
            status = handle_reply_handoff(db, raw)
    except Exception as exc:  # class only: the message text can carry personal details
        if webhook_failures.report_webhook_failure(HANDOFF_SOURCE, raw, exc) is None:
            raise HandoffNotSaved(type(exc).__name__) from None
        status = HANDOFF_SAVED
    return {"status": status, "advanced": advanced}


journal.register_source(JOURNAL_SOURCE, handle_ghl_reply, label="GHL reply (handoff / after-hours answer)")
journal.register_source(HANDOFF_SOURCE, handle_reply_handoff, label="GHL reply handoff (Slack post)")


def _journal_body(body: dict[str, Any], event: ReplyEvent) -> dict[str, Any]:
    """The body as saved: a message without a GHL id keeps the fallback id it got now (it depends on the time),
    so a replay later is still the same message."""
    if body.get("messageId"):
        return body
    return {**body, "messageId": event.message_id}


def _ghl_reply_via_journal(body: dict[str, Any], event: ReplyEvent, background: BackgroundTasks) -> dict[str, Any]:
    try:
        result = journal.ingest(JOURNAL_SOURCE, event.message_id, _journal_body(body, event))
    except Exception:  # the journal logged the class; the body never reaches the log
        raise HTTPException(status_code=503, detail="The reply could not be saved; it will be retried") from None
    if result.outcome != "processed":
        return {"status": result.outcome}
    if result.value["advanced"]:
        background.add_task(run_send_cycle_now)  # the next question, right after the response
    return {"status": result.value["status"]}


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
    if journal.journal_enabled():
        return _ghl_reply_via_journal(body, event, background)
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
