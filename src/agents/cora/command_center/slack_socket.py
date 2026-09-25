"""
Command Center Slack Socket Mode listener.

Replaces the polling-based slack_listener.py with a persistent WebSocket
connection to Slack. Events arrive in ~50-150ms instead of up to poll_interval.

Requirements:
  1. Slack App-Level Token (xapp-…) with connections:write scope
     → set FA_MAX_SLACK_APP_TOKEN env var
  2. Socket Mode enabled in the Slack app dashboard
  3. message.channels event subscription enabled

Run via worker.py main() — started as a daemon thread alongside the CC worker.
Falls back to the polling listener if FA_MAX_SLACK_APP_TOKEN is not set.
"""
from __future__ import annotations

import logging
import re
import threading
from typing import Any, Optional

logger = logging.getLogger(__name__)

_BOT_USER_ID: Optional[str] = None  # resolved at startup via auth.test


def _listen_channel() -> Optional[str]:
    """Command Center is restricted to the one channel Josh uses to manage
    his whole FA Max pipeline (submissions, status updates, questions) —
    client decision, WP-T2-6 addendum Task 21. Read per call rather than
    frozen into a module-level constant at import time; get_settings() is
    itself lru_cached process-wide, so a settings change still needs a
    restart to take effect."""
    from config.settings import get_settings
    return get_settings().fa_max_slack_cc_channel


def _get_app_token() -> Optional[str]:
    try:
        from config.settings import get_settings
        t = get_settings().fa_max_slack_app_token
        if t:
            return t.get_secret_value()
    except Exception:
        pass
    import os
    return os.environ.get("FA_MAX_SLACK_APP_TOKEN", "").strip() or None


def _handle_message(event: dict) -> None:
    """Called for every message event. Filters and publishes to cc:events."""
    channel = event.get("channel", "")
    user_id = event.get("user", "")
    subtype = event.get("subtype", "")
    text = (event.get("text") or "").strip()
    ts = event.get("ts", "")

    # Filter: bot messages, system subtypes, empty text, wrong channel
    if not text or not ts:
        return
    if subtype:
        return
    if user_id == _BOT_USER_ID:
        return
    listen_channel = _listen_channel()
    if listen_channel and channel != listen_channel:
        return

    # Strip @-mention prefix — handled in guard too, but clean here for session key
    clean_text = re.sub(r"^<@[A-Z0-9]+>\s*", "", text).strip()
    if not clean_text:
        return

    from src.agents.cora.command_center.worker import publish_query

    session_id = f"cc:{user_id}:{channel}"
    mid = publish_query(
        session_id=session_id,
        question=clean_text,
        slack_user_id=user_id,
        slack_channel=channel,
        slack_thread_ts=None,
    )

    if mid:
        logger.info(
            "cc.socket: queued session=%s user=%s ts=%s text=%r",
            session_id, user_id, ts, clean_text[:80],
        )
    else:
        logger.warning("cc.socket: publish_query failed for ts=%s", ts)


_DEDUP_TTL_S = 600


def init_forwarder(web_client: Any) -> bool:
    """Prepare this module for an external socket owner (Relay listener).

    Called once from Relay's run() when FA_MAX_SLACK_SINGLE_SOCKET is True.
    Resolves the bot user ID so forward_message can filter the bot's own
    replies. Returns False (and logs an ERROR) when the channel is unset or
    the bot ID cannot be resolved — in both cases nothing is forwarded and
    Relay's existing handlers are unaffected.
    """
    global _BOT_USER_ID
    if not _listen_channel():
        logger.error(
            "cc.socket: FA_MAX_SLACK_CC_CHANNEL unset, refusing to forward — "
            "without a channel filter _handle_message passes every channel, "
            "so Cora would answer in MONEY/EXCEPTIONS/RELATIONSHIPS too"
        )
        return False
    _BOT_USER_ID = resolve_bot_user_id(web_client)
    if not _BOT_USER_ID:
        logger.error(
            "cc.socket: cannot resolve bot user ID — CC forwarding disabled "
            "(an unfiltered listener re-ingests the bot's own replies as questions)"
        )
        return False
    return True


def forward_message(event: dict, event_id: Optional[str]) -> None:
    """Filter, dedupe and publish one message event to cc:events. Never raises.

    Called by Relay's _on_cc_message after the envelope has already been
    acked, so a slow Redis call here cannot make Slack time out or redeliver.
    Channel and bot_id checks run before any Redis call.
    """
    try:
        if event.get("channel") != _listen_channel() or event.get("bot_id"):
            return
        if _BOT_USER_ID and event.get("user") == _BOT_USER_ID:
            return
        if not _first_delivery(event_id):
            return
        _handle_message(event)
    except Exception:
        logger.exception("cc.socket: forward_message raised ts=%s", event.get("ts"))


def _first_delivery(event_id: Optional[str]) -> bool:
    """Return True and mark seen on first delivery; False on a duplicate.

    Falls through to True (allow) when event_id is absent or Redis is down
    so a Redis outage never silently drops questions. Clicks are already
    idempotent at the queue-row CAS level and do not call this path.
    """
    if not event_id:
        return True
    from src.core.redis_client import get_redis

    r = get_redis()
    if r is None:
        return True
    try:
        return bool(r.set(f"cc:seen:{event_id}", 1, nx=True, ex=_DEDUP_TTL_S))
    except Exception:
        logger.warning("cc.socket: dedup check failed event_id=%s", event_id)
        return True


def resolve_bot_user_id(web_client: Any) -> Optional[str]:
    """Resolve the bot's own Slack user ID, needed to filter its own replies.

    Thin public re-export so Relay's run() can call init_forwarder() without
    importing bot_identity directly (keeping relay independent of Cora internals).
    Delegates to the canonical implementation in bot_identity.py.
    """
    from src.agents.cora.command_center.bot_identity import resolve_bot_user_id as _resolve
    return _resolve(web_client)


def run_socket_mode(stop_event: threading.Event) -> None:
    """
    Connect to Slack via Socket Mode WebSocket and handle message events.
    Blocks until stop_event is set or an unrecoverable error occurs.
    """
    app_token = _get_app_token()
    if not app_token:
        logger.warning(
            "cc.socket: FA_MAX_SLACK_APP_TOKEN not set — Socket Mode unavailable. "
            "Set the env var and enable Socket Mode in the Slack app dashboard."
        )
        return

    try:
        from slack_sdk.socket_mode import SocketModeClient
        from slack_sdk.socket_mode.response import SocketModeResponse
        from slack_sdk.socket_mode.request import SocketModeRequest
        from config.settings import get_settings
        from slack_sdk import WebClient

        from src.agents.cora.command_center.bot_identity import resolve_bot_user_id
    except ImportError as exc:
        logger.error("cc.socket: slack_sdk missing socket_mode support: %s", exc)
        return

    settings = get_settings()
    if not settings.fa_max_slack_bot_token:
        logger.error("cc.socket: FA_MAX_SLACK_BOT_TOKEN not set")
        return

    bot_token = settings.fa_max_slack_bot_token.get_secret_value()
    web_client = WebClient(token=bot_token)

    global _BOT_USER_ID
    _BOT_USER_ID = resolve_bot_user_id(web_client)
    if not _BOT_USER_ID:
        logger.error(
            "cc.socket: refusing to start — an unfiltered listener re-ingests the "
            "bot's own replies as new questions. Set FA_MAX_SLACK_BOT_USER_ID."
        )
        return

    client = SocketModeClient(app_token=app_token, web_client=web_client)

    def _on_event(socket_client: SocketModeClient, req: SocketModeRequest) -> None:
        # Acknowledge immediately — Slack requires ack within 3 seconds
        socket_client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))

        if req.type != "events_api":
            return

        payload = req.payload or {}
        event = payload.get("event", {})
        if event.get("type") == "message":
            try:
                _handle_message(event)
            except Exception:
                logger.exception("cc.socket: _handle_message raised")

    client.socket_mode_request_listeners.append(_on_event)

    logger.info("cc.socket: connecting via Socket Mode (real-time, no polling)")
    client.connect()

    # Block until stop_event fires
    stop_event.wait()
    logger.info("cc.socket: disconnecting")
    try:
        client.close()
    except Exception:
        pass
