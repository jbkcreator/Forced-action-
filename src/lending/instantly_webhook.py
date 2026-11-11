"""P1.3: ``POST /webhooks/lending/instantly``, NDL's Instantly webhook (``reply_received``, ``lead_unsubscribed``).

Served by ``lending-api``. Auth: Instantly attaches a custom header to every delivery; set
``LENDING_INSTANTLY_WEBHOOK_SECRET`` to the value sent in ``X-Instantly-Webhook-Secret``. Closed (503) while the
secret is unset; a missing or wrong header is refused (403).

The event is stored and committed first. A STOP reply or an unsubscribe is then suppressed straight away; if that
fails, Cora's minute sweep retries it, so the 60-second stop rule does not depend on Instantly redelivering. Every
other reply is classified and drafted by Cora's sweep (``instantly_replies.ReplyProcessor``). An event this route
does not handle is answered 200 and ignored, so Instantly does not retry it.
"""
from __future__ import annotations

import hmac
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.api.deps import get_db
from src.lending.instantly_client import block_address, lending_instantly_account
from src.lending.instantly_replies import (
    EVENT_UNSUBSCRIBED,
    parse_event,
    queue_suppression_for_cora,
    record_event,
    suppress_reply,
    suppression_notice,
)
from src.lending.ops_alerts import post_ops_notice
from src.lending.payload_shape import log_shape

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


def _verify_secret(provided: Optional[str]) -> None:
    configured = get_settings().lending_instantly_webhook_secret
    if not configured:
        raise HTTPException(status_code=503, detail="Instantly webhook is not configured")
    if not provided or not hmac.compare_digest(configured.get_secret_value(), provided):
        raise HTTPException(status_code=403, detail="Invalid webhook secret")


def _suppress_now(db: Session, reply_id: int, email: str, *, via_unsubscribe_link: bool) -> None:
    """Record the opt-out, then announce it and block the address in Instantly. Never raises: a failure is left
    for Cora's sweep to retry."""
    try:
        event_id = suppress_reply(db, reply_id)
        db.commit()
    except Exception as exc:  # class only: SQL errors embed the address
        db.rollback()
        logger.error("[instantly-webhook] opt-out for reply %s not recorded yet (%s); Cora's sweep retries it",
                     reply_id, type(exc).__name__)
        return
    posted = post_ops_notice(suppression_notice(email, event_id, via_unsubscribe_link=via_unsubscribe_link))
    queue_suppression_for_cora(db, reply_id, email, event_id, posted=posted)
    db.commit()
    account = lending_instantly_account()
    if account is not None:
        block_address(account, email)


@router.post("/instantly")
def instantly_event(
    body: dict[str, Any],
    x_instantly_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _verify_secret(x_instantly_webhook_secret)
    log_shape("instantly", body)
    event = parse_event(body)
    if event is None:
        return {"status": "ignored"}
    try:
        recorded = record_event(db, event)
        db.commit()
    except Exception as exc:  # class only: the payload carries the address and the reply
        db.rollback()
        logger.error("[instantly-webhook] %s not stored (%s)", event.event_type, type(exc).__name__)
        raise HTTPException(status_code=503, detail="Could not store the event; it will be retried") from None
    if recorded.status != "recorded":
        return {"status": recorded.status}
    logger.info("[instantly-webhook] stored %s as reply %s stop=%s", event.event_type, recorded.reply_id,
                recorded.stop_request)
    if recorded.stop_request:
        _suppress_now(db, recorded.reply_id, event.lead_email,
                      via_unsubscribe_link=event.event_type == EVENT_UNSUBSCRIBED)
        return {"status": "suppressed"}
    return {"status": "recorded"}
