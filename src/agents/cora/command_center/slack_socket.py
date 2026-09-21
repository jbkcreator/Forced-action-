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
from typing import Optional

logger = logging.getLogger(__name__)

_BOT_USER_ID: Optional[str] = None  # resolved at startup via auth.test
_LISTEN_CHANNEL = None  # None = all channels the bot is in; set to filter


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
    if _LISTEN_CHANNEL and channel != _LISTEN_CHANNEL:
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
    try:
        auth = web_client.auth_test()
        _BOT_USER_ID = auth["user_id"]
        logger.info("cc.socket: bot user_id resolved via auth.test: %s", _BOT_USER_ID)
    except Exception as exc:
        logger.warning("cc.socket: auth.test failed — bot self-reply filter disabled: %s", exc)

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
