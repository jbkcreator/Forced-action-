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
    if action_id not in {"approve", "reject"}:
        return False

    # Imports stay local so a worker startup does not create an API server.
    from src.api.admin_router import _handle_relay_decision

    result = _handle_relay_decision(payload)
    # HTTP can return an ephemeral refusal directly to Slack. Socket Mode has
    # already acknowledged the envelope, so retain the same result in logs for
    # an operator to diagnose an authorization or state-transition refusal.
    if (result or {}).get("ok") is not True:
        logger.warning(
            "[RelaySocket] approval left pending for Slack user %s: %s",
            payload.get("user", {}).get("id", "unknown"),
            (result or {}).get("text", "no result detail"),
        )
    return True


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
    socket = SocketModeClient(app_token=app_token.get_secret_value(), web_client=web)

    def _on_request(client: Any, request: Any) -> None:
        try:
            handled = handle_socket_request(client, request)
            if handled:
                logger.info("[RelaySocket] processed Relay approval action")
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
