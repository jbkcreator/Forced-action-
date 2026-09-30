"""POST /webhooks/lending/dialer — real-time dialer call events (spec §4.4).

Order inside one request: authenticate → parse → keep only lending campaigns →
save the call row and commit → compliance hooks → reply. The Sheet and Slack
run after the reply, so the compliance hooks never wait on Google or Slack.

A real failure returns 5xx so the dialer redelivers the event; every step is
idempotent, so redelivery is safe. Ignored events always return 200.

Unlike the CDR poller (src/lending/cdr_poll.py), which ignores inbound calls except DNC
requests, this route applies no inbound filter. It is not the live ingestion path.
"""
from __future__ import annotations

import hmac
import json
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from config.settings import get_settings
from src.lending.db import get_lending_db
from src.lending.call_pipeline import follow_up, lending_campaign_ids, process_event
from src.lending.dispositions import last4, parse_event

logger = logging.getLogger(__name__)
router = APIRouter()


def is_authentic(presented: Optional[str]) -> bool:
    """Constant-time check of the shared secret (header ``X-Webhook-Secret`` or ``?token=``).

    ponytail: the dialer's own scheme (signature vs token) is unconfirmed; a shared
    secret is the floor. Add signature verification once the docs say what is sent.
    """
    secret = get_settings().lending_dialer_webhook_secret
    if not secret:
        logger.error("[lending] dialer webhook secret not configured — rejecting event")
        return False
    return bool(presented) and hmac.compare_digest(str(presented), secret.get_secret_value())


@router.post("/webhooks/lending/dialer", status_code=200)
async def dialer_call_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    token: Optional[str] = None,
    db: Session = Depends(get_lending_db),
    x_webhook_secret: Optional[str] = Header(None, alias="X-Webhook-Secret"),
):
    if not is_authentic(x_webhook_secret or token):
        raise HTTPException(status_code=401, detail="invalid secret")

    try:
        body = json.loads(await request.body() or b"{}")
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid payload")
    ev = parse_event(body)
    if ev is None:
        raise HTTPException(status_code=400, detail="invalid payload")

    if ev.campaign_id not in lending_campaign_ids():
        logger.debug("[lending] ignoring event for a non-lending campaign")
        return {"ok": True}

    try:
        recorded = await run_in_threadpool(process_event, db, ev)
    except OperationalError:
        db.rollback()
        logger.error("[lending] database error processing call %s", ev.call_id)
        raise HTTPException(status_code=503, detail="database temporarily unavailable")
    except Exception as exc:
        db.rollback()
        logger.error("[lending] processing call %s failed: %s", ev.call_id, type(exc).__name__)
        raise HTTPException(status_code=500, detail="processing failed")

    logger.info("[lending] call %s (%s) disposition=%s", recorded.call_id, last4(recorded.phone), recorded.disposition)
    follow_up(recorded, background_tasks.add_task)
    return {"ok": True}
