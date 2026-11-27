"""LendingFlow lead receiver: ``POST /api/lending/lendingflow`` (T-11), served by ``lending-api``.

Auth: shared secret in ``X-Webhook-Secret`` (``LENDING_LENDINGFLOW_WEBHOOK_SECRET``, its own secret,
not the GHL one). The receiver is closed (503) while the feature flag is off or the secret is unset.
The lead and its consent certificate are committed first; GHL delivery, the event and the pre-qual
hand-off run after the response, with ``src.tasks.lending_lendingflow_sweep`` as the safety net.

With ``LENDING_INBOUND_JOURNAL_ENABLED`` (R1 0.5 T4) the parsed body is saved raw to the inbound journal first
(``src.lending.inbound.lendingflow``); a failed save of the lead is retried by the 5-minute DLQ cron and the
receiver still answers 200. ``LENDING_LENDINGFLOW_HMAC_SECRET`` (optional, D6) adds an HMAC-SHA256 check of the
raw body on top of the shared secret.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session

from config.lending_fallbacks import FEED_LENDINGFLOW_INBOUND
from config.lending_lendingflow import MAX_BODY_BYTES
from config.settings import get_settings
from src.lending.db import get_lending_db
from config.lending_speed_to_lead import SOURCE_LENDINGFLOW
from src.lending import dependency_health, speed_to_lead
from src.lending.inbound import journal
from src.lending.inbound import lendingflow as lf_journal
from src.lending.lendingflow import ParseError, deliver_and_follow_up, parse_lendingflow, save_lead
from src.lending.lendingflow_ghl import get_live_sink

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending", tags=["lending"])

INVALID_PAYLOAD = "Invalid LendingFlow payload"


def _verify_lendingflow_secret(received: Optional[str]) -> None:
    """The one place sender authentication lives (swap for HMAC / allow-list if David specifies one)."""
    settings = get_settings()
    secret = settings.lending_lendingflow_webhook_secret
    if not settings.lending_lendingflow_enabled or secret is None:
        raise HTTPException(status_code=503, detail="LendingFlow intake is disabled")
    if not received or not hmac.compare_digest(received.encode(), secret.get_secret_value().encode()):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _verify_lendingflow_signature(request: Request, body: bytes) -> None:
    """D6: only while ``LENDING_LENDINGFLOW_HMAC_SECRET`` is set; hex digest, an optional ``sha256=`` prefix allowed."""
    settings = get_settings()
    key = settings.lending_lendingflow_hmac_secret
    if key is None:
        return
    received = (request.headers.get(settings.lending_lendingflow_hmac_header) or "").strip()
    if received.lower().startswith("sha256="):
        received = received[len("sha256="):]
    expected = hmac.new(key.get_secret_value().encode(), body, hashlib.sha256).hexdigest()
    if not received or not hmac.compare_digest(received.lower().encode(), expected.encode()):
        logger.warning("[lendingflow] rejected payload: bad signature")
        raise HTTPException(status_code=401, detail="Invalid webhook signature")


def _deliver_in_background(lead_id: int) -> None:
    deliver_and_follow_up(get_live_sink(), lead_id=lead_id,
                          after_delivery=speed_to_lead.live_step(SOURCE_LENDINGFLOW))


@router.post("/lendingflow")
async def receive_lendingflow_lead(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_lending_db),
    x_webhook_secret: Optional[str] = Header(default=None),
) -> dict:
    _verify_lendingflow_secret(x_webhook_secret)
    body = await request.body()
    if len(body) > MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")
    _verify_lendingflow_signature(request, body)
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        logger.warning("[lendingflow] rejected payload: not JSON")
        raise HTTPException(status_code=400, detail=INVALID_PAYLOAD) from None
    try:
        parsed = parse_lendingflow(payload)
    except ParseError as exc:
        logger.warning("[lendingflow] rejected payload: %s", exc)
        raise HTTPException(status_code=400, detail=INVALID_PAYLOAD) from None
    if journal.journal_enabled():
        return _ingest(parsed, payload, background_tasks)
    try:
        result = save_lead(db, parsed, payload)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lendingflow] save failed vendor=%s: %s", parsed.vendor_lead_id, type(exc).__name__)
        raise HTTPException(status_code=500, detail="Could not store the lead") from None
    dependency_health.record_success(FEED_LENDINGFLOW_INBOUND)  # R1 0.6: LendingFlow is delivering (silence watch)
    if not result.created:
        return {"status": "duplicate", "lead_id": result.lead_uuid}
    if not result.suppressed:
        background_tasks.add_task(_deliver_in_background, result.lead_id)
    return {"status": "created", "lead_id": result.lead_uuid}


def _ingest(parsed, payload: dict, background_tasks: BackgroundTasks) -> dict:
    try:
        result = lf_journal.ingest_lead(parsed, payload)
    except Exception as exc:  # raw save failed: 5xx so LendingFlow delivers it again (class only: SQL echoes the body)
        logger.error("[lendingflow] raw save failed vendor=%s: %s", parsed.vendor_lead_id, type(exc).__name__)
        raise HTTPException(status_code=500, detail="Could not store the lead") from None
    if result.outcome == "failed":
        return {"status": "accepted"}
    if result.outcome == "duplicate" or not result.value.created:
        return {"status": "duplicate", "lead_id": result.value.lead_uuid if result.value else None}
    if not result.value.suppressed:
        background_tasks.add_task(_deliver_in_background, result.value.lead_id)
    return {"status": "created", "lead_id": result.value.lead_uuid}
