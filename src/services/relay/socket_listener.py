"""Socket Mode listener for Relay approval cards.

This is the long-running inbound half of the Relay Slack integration.  It
receives Block Kit button envelopes over Slack's authenticated WebSocket and
delegates them to the same durable approval handler used by the HTTP endpoint.
The listener is deliberately separate from the send sweep: losing or
restarting it never changes a pending queue row, and Slack can redeliver an
unacknowledged envelope.

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
    """Acknowledge and dispatch one Slack Socket Mode envelope.

    Returns ``True`` only when this listener handled a Relay Approve/Reject
    action.  Other app actions are acknowledged and left untouched so a
    shared Socket Mode app does not accidentally mutate another workflow.
    """
    from slack_sdk.socket_mode.response import SocketModeResponse

    # Slack requires this acknowledgement within three seconds.  The durable
    # handler is intentionally called only after it, and remains idempotent.
    client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))

    if request.type != "interactive":
        return False
    payload = request.payload or {}
    if payload.get("type") != "block_actions":
        return False
    actions = payload.get("actions") or []
    action_id = actions[0].get("action_id") if actions else None
    if action_id not in {"approve", "reject"}:
        return False

    # Imports stay local so a worker startup does not create an API server.
    from src.api.admin_router import _handle_relay_decision

    _handle_relay_decision(payload)
    return True


def run() -> None:
    """Connect the Relay listener and keep it alive until the service stops."""
    settings = get_settings()
    app_token = settings.relay_slack_app_token
    bot_token = settings.slack_bot_token
    if not app_token or not bot_token:
        logger.warning(
            "[RelaySocket] listener not started: RELAY_SLACK_APP_TOKEN or "
            "SLACK_BOT_TOKEN is unset"
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

    socket.socket_mode_request_listeners.append(_on_request)
    logger.info("[RelaySocket] connecting via Socket Mode")
    socket.connect()

    # Keep the process alive. SocketModeClient manages reconnects itself.
    from threading import Event
    Event().wait()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
