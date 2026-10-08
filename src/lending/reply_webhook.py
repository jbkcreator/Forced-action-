"""WP-GL-10: GHL reply webhook -> Slack handoff / quoted-number alert (see reply_guard).

A GHL workflow ("Customer replied" and "Conversation AI message sent" -> Webhook) POSTs each message here.
Auth: the same ``X-Webhook-Secret`` as the other lending GHL webhooks; closed while the secret is unset.
UNVERIFIED: GHL's payload field names are from its public reference, not a captured webhook.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.api.lending_ghl_router import _verify_secret
from src.lending.payload_shape import log_shape
from src.lending.reply_guard import handle_reply_event, parse_event, slack_poster

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


@router.post("/ghl-reply")
def ghl_reply(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _verify_secret(x_webhook_secret)
    log_shape("ghl-reply", body)
    event = parse_event(body)
    if event is None:
        return {"status": "ignored", "reason": "no_message"}
    try:
        outcome = handle_reply_event(db, event, poster=slack_poster())
    except Exception as exc:  # class only: the message text can carry personal details
        logger.error("[reply-webhook] handling message %s failed (%s)", event.message_id, type(exc).__name__)
        db.rollback()
        raise HTTPException(status_code=502, detail="Could not post the handoff; it will be retried") from None
    if outcome == "not_configured":
        raise HTTPException(status_code=503, detail="Reply handoff channel is not configured; it will be retried")
    return {"status": outcome}
