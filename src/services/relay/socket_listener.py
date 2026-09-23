"""Socket Mode listener for the FA Max Slack app.

This is the long-running inbound half of the FA Max Slack integration — one
process, one WebSocket connection, one growing list of listener functions.
Each registered listener gets every incoming envelope and independently
decides whether it applies, so unrelated workflows sharing this one
connection never step on each other (see ``_on_request`` vs
``_on_tracked_link_request`` below).

Relay approval cards (``_on_request``): receives Block Kit button envelopes
over Slack's authenticated WebSocket and delegates them to the same durable
approval handler used by the HTTP endpoint. Deliberately separate from the
send sweep: losing or restarting it never changes a pending queue row, and
Slack can redeliver an unacknowledged envelope.

``/tracked-link`` (``_on_tracked_link_request``, WP-7 self-serve pre-fill):
lets Josh mint a tracked link himself, no engineer needed. Owned by
src/services/tracked_links.py — this file only registers it.

Run with ``python -m src.services.relay.socket_listener``.  It needs both
``SLACK_BOT_TOKEN`` (to update cards) and ``RELAY_SLACK_APP_TOKEN`` (xapp,
with ``connections:write`` scope).
"""
from __future__ import annotations

import logging
from typing import Any

from config.settings import get_settings

logger = logging.getLogger(__name__)


def handle_socket_request(client: Any, request: Any) -> bool:
    """Acknowledge and dispatch one Slack Socket Mode envelope this listener owns.

    Only ``events_api`` and ``interactive`` envelopes belong to this listener.
    Other envelope types (e.g. ``slash_commands``, owned by
    ``handle_tracked_link_socket_request``) are left un-acked and untouched so
    the listener that actually owns them can send Slack the real response —
    Slack Socket Mode resolves an envelope on its *first* acknowledgement, so
    acking here for an envelope this listener does not own would silently
    swallow the other listener's reply.

    Returns ``True`` only when this listener handled a Relay Approve/Reject
    action.
    """
    if request.type not in {"events_api", "interactive"}:
        return False

    from slack_sdk.socket_mode.response import SocketModeResponse

    # Slack requires this acknowledgement within three seconds.  The durable
    # handler is intentionally called only after it, and remains idempotent.
    client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))

    payload = request.payload or {}
    if request.type == "events_api":
        # Socket Mode delivers subscribed Events API payloads in an
        # ``events_api`` envelope. Reuse the HTTP route's thread-command
        # handler so an exact approve/reject reply has the same durable
        # authorization, transition, and audit behaviour as a button click.
        from src.api.admin_router import _handle_relay_thread_action

        _handle_relay_thread_action(payload)
        return payload.get("type") == "event_callback"

    if payload.get("type") != "block_actions":
        return False
    actions = payload.get("actions") or []
    action_id = actions[0].get("action_id") if actions else None

    from src.api.admin_router import (
        _handle_relay_decision,
        _handle_confirm_entity_link,
        _handle_reject_entity_link,
        _handle_view_entity_link,
        _handle_add_builder_to_diallist,
        _handle_snooze_builder,
        _handle_dismiss_builder,
    )
    from src.core.database import get_db_context

    user_id = payload.get("user", {}).get("id", "?")

    # EXCEPTIONS lane (WP-T2-8)
    if action_id and action_id.startswith("confirm_entity_link_"):
        logger.info("[RelaySocket] entity-link confirm: action_id=%s user=%s", action_id, user_id)
        with get_db_context() as db:
            result = _handle_confirm_entity_link(payload, db)
        _post_socket_ephemeral(client, payload, result)
        return True
    if action_id and action_id.startswith("reject_entity_link_"):
        logger.info("[RelaySocket] entity-link reject: action_id=%s user=%s", action_id, user_id)
        with get_db_context() as db:
            result = _handle_reject_entity_link(payload, db)
        _post_socket_ephemeral(client, payload, result)
        return True
    if action_id and action_id.startswith("view_entity_link_"):
        logger.info("[RelaySocket] entity-link view: action_id=%s user=%s", action_id, user_id)
        with get_db_context() as db:
            result = _handle_view_entity_link(payload, db)
        _post_socket_ephemeral(client, payload, result)
        return True

    # RELATIONSHIPS lane (WP-T2-8)
    if action_id and action_id.startswith("add_builder_to_diallist_"):
        logger.info("[RelaySocket] builder dial-list add: action_id=%s user=%s", action_id, user_id)
        with get_db_context() as db:
            result = _handle_add_builder_to_diallist(payload, db)
        _post_socket_ephemeral(client, payload, result)
        return True
    if action_id and action_id.startswith("snooze_builder_"):
        logger.info("[RelaySocket] builder snooze: action_id=%s user=%s", action_id, user_id)
        with get_db_context() as db:
            result = _handle_snooze_builder(payload, db)
        _post_socket_ephemeral(client, payload, result)
        return True
    if action_id and action_id.startswith("dismiss_builder_"):
        logger.info("[RelaySocket] builder dismiss: action_id=%s user=%s", action_id, user_id)
        with get_db_context() as db:
            result = _handle_dismiss_builder(payload, db)
        _post_socket_ephemeral(client, payload, result)
        return True

    # Relay approve/reject
    if action_id not in {"approve", "reject"}:
        return False

    import json as _json
    try:
        _action_data = _json.loads((payload.get("actions") or [{}])[0].get("value", "{}"))
    except Exception:
        _action_data = {}
    _item_id = _action_data.get("item_id")
    logger.info("[RelaySocket] relay decision: item_id=%s action=%s user=%s",
                _item_id, action_id, payload.get("user", {}).get("id", "?"))

    try:
        result = _handle_relay_decision(payload)
    except Exception:
        logger.exception("[RelaySocket] _handle_relay_decision raised for item_id=%s", _item_id)
        return True

    if (result or {}).get("ok") is not True:
        logger.warning(
            "[RelaySocket] approval left pending: item_id=%s user=%s reason=%s",
            _item_id,
            payload.get("user", {}).get("id", "unknown"),
            (result or {}).get("text", "no detail"),
        )
    else:
        logger.info("[RelaySocket] decision committed: item_id=%s result=%s", _item_id, result)
    return True


def _post_socket_ephemeral(client: Any, payload: dict, result: dict) -> None:
    """Send an ephemeral reply after a Socket Mode button action.

    Socket Mode already acknowledged the envelope — ephemeral replies must go
    through chat.postEphemeral, not the envelope response.
    """
    text = (result or {}).get("text", "")
    if not text:
        return
    try:
        channel = payload.get("channel", {}).get("id", "")
        user_id = payload.get("user", {}).get("id", "")
        if channel and user_id:
            client.web_client.chat_postEphemeral(channel=channel, user=user_id, text=text)
    except Exception:
        logger.warning("[RelaySocket] could not post ephemeral reply: %s", text)


def run() -> None:
    """Connect the Relay listener and keep it alive until the service stops."""
    settings = get_settings()
    app_token = settings.fa_max_slack_app_token
    bot_token = settings.fa_max_slack_bot_token
    if not app_token or not bot_token:
        logger.warning(
            "[RelaySocket] listener not started: FA_MAX_SLACK_APP_TOKEN or "
            "FA_MAX_SLACK_BOT_TOKEN is unset"
        )
        return

    from slack_sdk import WebClient
    from slack_sdk.socket_mode import SocketModeClient

    web = WebClient(token=bot_token.get_secret_value())
    # concurrency=25 (library default: 10) -- this one connection carries both
    # Relay approval-card clicks and /tracked-link slash commands. A burst of
    # real approval activity can occupy all 10 default workers, queuing a
    # slash command envelope behind them; if that queue wait pushes past
    # Slack's 3-second ack window the command never even starts, and Slack
    # shows the user "the app did not respond" with nothing logged on our
    # side. Confirmed via isolated reproduction (2026-09-21): the same
    # rapid-fire /tracked-link test dropped replies against the live
    # production workspace but never dropped a single reply against an
    # isolated test app/workspace with no concurrent traffic, run both
    # locally and from this server -- ruling out the server's network path
    # and the slash-command code itself, and pointing at worker-pool
    # contention on the shared connection.
    socket = SocketModeClient(app_token=app_token.get_secret_value(), web_client=web, concurrency=25)

    def _on_request(client: Any, request: Any) -> None:
        try:
            handle_socket_request(client, request)
        except Exception:
            # An action failure is logged after acknowledgement.  The durable
            # queue row remains pending unless the handler commits its CAS.
            logger.exception("[RelaySocket] failed to process Slack envelope")

    def _on_tracked_link_request(client: Any, request: Any) -> None:
        # WP-7 (Forced Action MAX) self-serve pre-fill — /tracked-link slash
        # command. Owned by src/services/tracked_links.py, imported lazily
        # here (same convention as WebClient/SocketModeClient above) so this
        # module has no hard dependency on WP-7's package at load time.
        try:
            from src.services.tracked_links import handle_tracked_link_socket_request

            handled = handle_tracked_link_socket_request(client, request)
            if handled:
                logger.info("[RelaySocket] processed /tracked-link command")
        except Exception:
            logger.exception("[RelaySocket] failed to process /tracked-link request")

    socket.socket_mode_request_listeners.append(_on_request)
    socket.socket_mode_request_listeners.append(_on_tracked_link_request)
    logger.info("[RelaySocket] connecting via Socket Mode")
    socket.connect()

    # Keep the process alive. SocketModeClient manages reconnects itself.
    from threading import Event
    Event().wait()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
