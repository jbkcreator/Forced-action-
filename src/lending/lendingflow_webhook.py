"""LendingFlow lead receiver: ``POST /api/lending/lendingflow`` (T-11), served by ``lending-api``.

Auth: shared secret in ``X-Webhook-Secret`` (``LENDING_LENDINGFLOW_WEBHOOK_SECRET``, its own secret,
not the GHL one). The receiver is closed (503) while the feature flag is off or the secret is unset.
The lead and its consent certificate are committed first; GHL delivery, the event and the pre-qual
hand-off run after the response, with ``src.tasks.lending_lendingflow_sweep`` as the safety net.
"""
from __future__ import annotations

import hmac
import json
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session

from config.lending_lendingflow import MAX_BODY_BYTES
from config.settings import get_settings
from src.lending.db import get_lending_db
from src.lending.lendingflow import ParseError, deliver_and_follow_up, parse_lendingflow, save_lead
from src.lending.lendingflow_ghl import get_live_sink

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending", tags=["lending"])


def _verify_lendingflow_secret(received: Optional[str]) -> None:
    """The one place sender authentication lives (swap for HMAC / allow-list if David specifies one)."""
    settings = get_settings()
    secret = settings.lending_lendingflow_webhook_secret
    if not settings.lending_lendingflow_enabled or secret is None:
        raise HTTPException(status_code=503, detail="LendingFlow intake is disabled")
    if not received or not hmac.compare_digest(received, secret.get_secret_value()):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _deliver_in_background(lead_id: int) -> None:
    deliver_and_follow_up(get_live_sink(), lead_id=lead_id)


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
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Body is not valid JSON") from None
    try:
        parsed = parse_lendingflow(payload)
    except ParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        result = save_lead(db, parsed, payload)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lendingflow] save failed vendor=%s: %s", parsed.vendor_lead_id, type(exc).__name__)
        raise HTTPException(status_code=500, detail="Could not store the lead") from None
    if not result.created:
        return {"status": "duplicate", "lead_id": result.lead_uuid}
    if not result.suppressed:
        background_tasks.add_task(_deliver_in_background, result.lead_id)
    return {"status": "created", "lead_id": result.lead_uuid}
