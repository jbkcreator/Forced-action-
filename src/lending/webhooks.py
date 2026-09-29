"""POST /webhooks/aircall/disposition — real-time call events (spec §4.4).

Order inside one request: authenticate → parse → keep only lending lines →
save the call row and commit → compliance hooks → reply. The Sheet and Slack
run after the reply, so the compliance hooks never wait on Google or Slack.

A real failure returns 5xx so Aircall redelivers the event; every step is
idempotent, so redelivery is safe. Ignored events always return 200.
"""
from __future__ import annotations

import hashlib
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
from src.lending.disposition_delivery import alert_unpropagated_dnc, deliver_disposition
from src.lending.dispositions import HANDLED_EVENTS, RecordedCall, last4, record_aircall_event

logger = logging.getLogger(__name__)
router = APIRouter()
TAG_EVENTS = frozenset({"call.tagged", "call.untagged"})


def _token() -> Optional[str]:
    secret = get_settings().lending_aircall_webhook_token
    return secret.get_secret_value() if secret else None


def is_authentic(raw_body: bytes, signature: Optional[str], body_token: Optional[str]) -> bool:
    """Accept a valid HMAC-SHA256 signature header or the shared token in the body.

    ponytail: both Aircall schemes accepted until a live event confirms which one
    it sends; drop the other once known.
    """
    token = _token()
    if not token:
        logger.error("[lending] aircall webhook token not configured — rejecting event")
        return False
    if signature:
        expected = hmac.new(token.encode(), raw_body, hashlib.sha256).hexdigest()
        if hmac.compare_digest(expected, signature):
            return True
    return bool(body_token) and hmac.compare_digest(str(body_token), token)


def lending_line_ids() -> frozenset[str]:
    raw = get_settings().lending_aircall_line_ids
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def _process(db: Session, etype: str, data: dict) -> Optional[RecordedCall]:
    recorded = record_aircall_event(db, etype, data)
    if recorded is None:
        return None
    db.commit()

    if etype == "call.ended" and recorded.phone:
        on_attempt_recorded(db, recorded.phone)
    if (recorded.disposition == "DNC_REQUEST" or recorded.dnc_tagged) and not recorded.opt_out_propagated:
        if recorded.phone:
            propagate_opt_out(db, phone=recorded.phone, source_ref=recorded.call_id, actor=recorded.caller_seat)
            db.execute(
                text("UPDATE lending.call_dispositions SET opt_out_propagated_at = now() WHERE id = :id"),
                {"id": recorded.row_id},
            )
        else:
            logger.error("[lending] call %s: DNC_REQUEST without a usable phone number", recorded.call_id)
    db.commit()
    return recorded


@router.post("/webhooks/aircall/disposition", status_code=200)
async def aircall_disposition_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_lending_db),
    x_aircall_signature: Optional[str] = Header(None, alias="X-Aircall-Signature"),
):
    raw_body = await request.body()
    try:
        event = json.loads(raw_body or b"{}")
    except ValueError:
        event = None
    body_token = event.get("token") if isinstance(event, dict) else None

    if not is_authentic(raw_body, x_aircall_signature, body_token):
        raise HTTPException(status_code=401, detail="invalid signature")
    if not isinstance(event, dict):
        raise HTTPException(status_code=400, detail="invalid payload")

    etype = event.get("event")
    data = event.get("data")
    if etype not in HANDLED_EVENTS:
        return {"ok": True}
    if not isinstance(data, dict) or not (data.get("id") or data.get("call_id")):
        raise HTTPException(status_code=400, detail="invalid payload")

    line = (data.get("number") or {}).get("id")
    if str(line) not in lending_line_ids():
        logger.debug("[lending] ignoring event for a non-lending line")
        return {"ok": True}

    try:
        recorded = await run_in_threadpool(_process, db, etype, data)
    except OperationalError:
        db.rollback()
        logger.error("[lending] database error processing %s", etype)
        raise HTTPException(status_code=503, detail="database temporarily unavailable")
    except Exception as exc:
        db.rollback()
        logger.error("[lending] processing %s failed: %s", etype, type(exc).__name__)
        raise HTTPException(status_code=500, detail="processing failed")

    if recorded is None:
        return {"ok": True}
    if recorded.disposition:
        logger.info("[lending] call %s (%s) disposition=%s", recorded.call_id, last4(recorded.phone),
                    recorded.disposition)
    if recorded.disposition or etype in TAG_EVENTS:
        # A removed result also needs delivering; delivery is a no-op when nothing is behind.
        background_tasks.add_task(deliver_disposition, recorded.row_id)
    if (recorded.disposition == "DNC_REQUEST" or recorded.dnc_tagged) and not recorded.phone:
        background_tasks.add_task(alert_unpropagated_dnc, recorded.call_id, recorded.caller_seat)
    return {"ok": True}
