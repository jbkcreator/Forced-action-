"""POST /webhooks/lending/slack-events: the lending Slack app's Event Subscriptions Request URL (Cora).

Served by lending-api (nginx /webhooks/lending/). Every request is signature-checked with
LENDING_SLACK_SIGNING_SECRET; the route is closed (503) while the secret is unset. Slack's
``url_verification`` challenge is answered directly. Events are acknowledged at once and handled
in a background task, because a Claude answer takes longer than the 3 seconds Slack waits; a
Slack retry (``X-Slack-Retry-Num``) is acknowledged and dropped, so a slow first attempt never
produces a second answer.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from config.settings import get_settings
from src.lending.cora_runtime import handle_slack_request
from src.lending.soft_approval.slack_card import verify_slack_signature

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


@router.post("/slack-events")
async def slack_events(request: Request, background: BackgroundTasks) -> dict[str, Any]:
    secret = get_settings().lending_slack_signing_secret
    if not secret:
        raise HTTPException(status_code=503, detail="Slack events are not configured")
    body = await request.body()
    if not verify_slack_signature(
        secret.get_secret_value(), request.headers.get("X-Slack-Request-Timestamp"),
        request.headers.get("X-Slack-Signature"), body,
    ):
        raise HTTPException(status_code=401, detail="Invalid signature")
    try:
        payload = json.loads(body)
    except ValueError:
        raise HTTPException(status_code=400, detail="Malformed payload") from None
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Malformed payload")

    if payload.get("type") == "url_verification":
        return {"challenge": str(payload.get("challenge") or "")}
    if request.headers.get("X-Slack-Retry-Num"):
        logger.info("[cora] Slack retry %s for event %s dropped (first attempt is being handled)",
                    request.headers.get("X-Slack-Retry-Num"), payload.get("event_id"))
        return {}
    if payload.get("type") == "event_callback":
        background.add_task(handle_slack_request, "events_api", {"event": payload.get("event") or {}},
                            str(payload.get("event_id") or ""))
    return {}
