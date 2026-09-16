"""WP-9 Dial List — Socket Mode action listener.

Long-running process (separate from the daily cron) that turns dial-list card
button taps and thread replies into dispositions via ``actions.handle_action`` /
``handle_thread_reply``. Socket Mode → no public request URL needed.

Run:  python -m src.services.dial_list.action_listener

No-ops with a clear log (never crashes the box) when the app-level token is
unset — the read-only daily digest still works without this listener.
"""
from __future__ import annotations

import logging

from config.settings import get_settings
from src.core.database import get_db_context

from .actions import handle_action, handle_thread_reply

logger = logging.getLogger(__name__)


def run() -> None:
    settings = get_settings()
    app_token = settings.dial_list_slack_app_token
    bot_token = settings.slack_bot_token
    approver = settings.dial_list_approver_user_id

    if not app_token or not bot_token:
        logger.warning(
            "[DialList] action listener not started — "
            "dial_list_slack_app_token / slack_bot_token unset."
        )
        return
    if not approver:
        logger.warning(
            "[DialList] action listener starting WITHOUT an approver gate "
            "(dial_list_approver_user_id unset) — any workspace member can act. "
            "Set DIAL_LIST_APPROVER_USER_ID before running against a real channel."
        )

    from slack_sdk.socket_mode import SocketModeClient
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.socket_mode.response import SocketModeResponse
    from slack_sdk.web import WebClient

    web = WebClient(token=bot_token.get_secret_value())
    sm = SocketModeClient(app_token=app_token.get_secret_value(), web_client=web)

    try:
        bot_user_id = web.auth_test()["user_id"]
    except Exception:  # noqa: BLE001 - startup diagnostics only
        bot_user_id = ""
        logger.warning("[DialList] auth_test failed — bot_user_id unknown", exc_info=True)

    def _on(client: SocketModeClient, req: SocketModeRequest) -> None:
        # Slack requires a prompt ack before any work.
        client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))
        try:
            if req.type == "interactive":
                payload = req.payload or {}
                if payload.get("type") != "block_actions":
                    return
                with get_db_context() as session:
                    handle_action(payload, session, approver_id=approver, client=web)
                return

            if req.type == "events_api":
                event = (req.payload or {}).get("event") or {}
                if event.get("type") != "message":
                    return
                # only threaded replies, from the approver, not the bot itself
                if not event.get("thread_ts") or event.get("bot_id"):
                    return
                if approver and event.get("user") != approver:
                    return
                if event.get("user") == bot_user_id:
                    return
                thread_id = _resolve_thread(event)
                if thread_id is None:
                    return
                with get_db_context() as session:
                    handle_thread_reply(
                        event.get("text", ""), session,
                        opportunity_thread_id=thread_id,
                    )
        except Exception:  # noqa: BLE001 - a bad event must not kill the loop
            logger.error("[DialList] action listener error", exc_info=True)

    sm.socket_mode_request_listeners.append(_on)
    logger.info("[DialList] action listener connecting (Socket Mode)…")
    sm.connect()
    from threading import Event

    Event().wait()


def _resolve_thread(event: dict) -> str | None:
    """Map a Slack thread reply back to an opportunity_thread_id.

    The digest posts all entries in one message, so a reply's ``thread_ts`` maps
    to the digest, not a single entry. Resolving which entry a free-text reply
    targets needs a message-ts → entry mapping (a listener-state concern, out of
    this v1). Returns None until that mapping exists, so button taps — which
    already carry the thread id in their payload — remain the primary path.
    """
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
