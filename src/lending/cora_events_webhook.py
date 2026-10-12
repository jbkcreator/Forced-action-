"""POST /webhooks/lending/slack-events: the lending Slack app's Event Subscriptions Request URL (Cora).

Served by lending-api (nginx /webhooks/lending/). Every request is signature-checked with
LENDING_SLACK_SIGNING_SECRET; the route is closed (503) while the secret is unset. Slack's
``url_verification`` challenge is answered directly. Events are acknowledged at once and handled
in a background task, because a Claude answer takes longer than the 3 seconds Slack waits.

Each event is accepted once: an atomic Redis receipt per Slack ``event_id`` (shared by every
lending-api worker) is taken before the work is scheduled. A Slack retry of an event already
accepted is acknowledged and dropped; a retry of an event that never reached us (say nginx
answered 502 during a restart) has no receipt and is processed. When Redis cannot be reached
the event is processed: a rare second answer is better than a lost message. The receipt runs in a
worker thread, so a slow Redis never stalls the event loop that answers Slack.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from config.settings import get_settings
from src.lending.cora_runtime import handle_slack_request
from src.lending.slack_signature import verify_slack_signature

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])

# Slack retries a failed delivery three times within about five minutes; an hour is ample.
RECEIPT_TTL_SECONDS = 60 * 60
RECEIPT_KEY = "cora:slack_event:{event_id}"
RECONNECT_SECONDS = 30
_REDIS_TIMEOUT_SECONDS = 1

_reconnect_lock = threading.Lock()
_own_client: Any = None
_next_connect_at = 0.0


def _receipt_store() -> Optional[Any]:
    """A Redis client for receipts, or None while Redis cannot be reached.

    The shared client (``get_redis``) connects once per process and stays None for good if Redis was
    down at that moment; Cora then keeps trying her own connection, at most every RECONNECT_SECONDS.
    """
    global _own_client, _next_connect_at
    from src.core.redis_client import get_redis

    shared = get_redis()
    if shared is not None:
        return shared
    with _reconnect_lock:
        if _own_client is not None:
            return _own_client
        if time.monotonic() < _next_connect_at:
            return None
        _next_connect_at = time.monotonic() + RECONNECT_SECONDS
        url = getattr(get_settings(), "redis_url", None)
        if not url:
            return None
        try:
            import redis

            client = redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=_REDIS_TIMEOUT_SECONDS,
                                          socket_timeout=_REDIS_TIMEOUT_SECONDS)
            client.ping()
        except Exception as exc:
            logger.warning("[cora] Redis still unreachable (%s); retrying in %ss", type(exc).__name__,
                           RECONNECT_SECONDS)
            return None
        _own_client = client
        return _own_client


def accept_event_once(event_id: str) -> bool:
    """True the first time an event id is seen by any worker; False for a repeat of an accepted event."""
    if not event_id:
        return True
    client = _receipt_store()
    if client is None:
        logger.warning("[cora] Redis unavailable; processing event %s without a duplicate check", event_id)
        return True
    try:
        return bool(client.set(RECEIPT_KEY.format(event_id=event_id), 1, nx=True, ex=RECEIPT_TTL_SECONDS))
    except Exception as exc:
        logger.warning("[cora] Redis receipt failed for event %s (%s); processing it", event_id, type(exc).__name__)
        return True


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
    if payload.get("type") != "event_callback":
        return {}
    event_id = str(payload.get("event_id") or "")
    if not await run_in_threadpool(accept_event_once, event_id):
        logger.info("[cora] event %s already accepted; Slack delivery %s dropped", event_id,
                    request.headers.get("X-Slack-Retry-Num") or "repeat")
        return {}
    background.add_task(handle_slack_request, "events_api", {"event": payload.get("event") or {}}, event_id)
    return {}
