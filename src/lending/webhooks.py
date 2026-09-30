"""POST /webhooks/lending/dialer — real-time dialer call events (spec §4.4).

Order inside one request: authenticate → parse → keep only lending campaigns →
save the call row and commit → compliance hooks → reply. The Sheet and Slack
run after the reply, so the compliance hooks never wait on Google or Slack.

A real failure returns 5xx so the dialer redelivers the event; every step is
idempotent, so redelivery is safe. Ignored events always return 200.
"""
from __future__ import annotations

import dataclasses
import hmac
import json
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from config.settings import get_settings
from src.lending.compliance import on_attempt_recorded, propagate_opt_out
from src.lending.db import get_lending_db
from src.lending.disposition_delivery import (
    alert_dnc_removal_pending,
    alert_unknown_code,
    alert_unpropagated_dnc,
    deliver_disposition,
)
from src.lending.dispositions import DialerCallEvent, RecordedCall, last4, parse_event, record_dialer_event

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


def lending_campaign_ids() -> frozenset[str]:
    raw = get_settings().lending_dialer_campaign_ids
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def process_event(db: Session, ev: DialerCallEvent) -> RecordedCall:
    recorded = record_dialer_event(db, ev)
    db.commit()

    if recorded.call_ended and recorded.phone:
        on_attempt_recorded(db, recorded.phone)
    if recorded.dnc_requested and not recorded.opt_out_propagated:
        if recorded.phone:
            event_id = propagate_opt_out(db, phone=recorded.phone, source_ref=recorded.call_id, actor=recorded.caller_seat)
            db.execute(
                text("UPDATE lending.call_dispositions SET opt_out_propagated_at = now() WHERE id = :id"),
                {"id": recorded.row_id},
            )
            if event_id and db.execute(
                text("SELECT status FROM lending.opt_out_events WHERE id = :id"), {"id": event_id}
            ).scalar() == "dialer_pending":
                recorded = dataclasses.replace(recorded, dnc_removal_pending=True)
        else:
            logger.error("[lending] call %s: DNC_REQUEST without a usable phone number", recorded.call_id)
    db.commit()
    return recorded


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
    background_tasks.add_task(deliver_disposition, recorded.row_id)
    if recorded.unknown_code:
        background_tasks.add_task(alert_unknown_code, recorded.call_id, recorded.unknown_code, recorded.caller_seat)
    if recorded.dnc_requested and not recorded.phone:
        background_tasks.add_task(alert_unpropagated_dnc, recorded.call_id, recorded.caller_seat)
    if recorded.dnc_removal_pending:
        background_tasks.add_task(alert_dnc_removal_pending, recorded.call_id, recorded.caller_seat)
    return {"ok": True}
