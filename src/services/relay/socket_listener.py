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
from typing import Any, Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)


def handle_socket_request(client: Any, request: Any) -> bool:
    """Acknowledge and dispatch one Slack Socket Mode envelope this listener owns.

    Only ``events_api`` and ``interactive`` envelopes belong to this listener.
    Other envelope types (``slash_commands``, owned by
    ``handle_tracked_link_socket_request`` or
    ``handle_fa_max_slash_command_request``) are left un-acked and untouched so
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

    payload = request.payload or {}

    # view_submission (modal Save/Submit) and block_suggestion (borrower-
    # search autocomplete) are NOT a fire-and-forget ack like block_actions
    # below -- Slack requires the SAME acknowledgement to CARRY the real
    # response (empty {} closes a modal; {"response_action": "errors", ...}
    # re-opens it with inline field errors; block_suggestion needs its
    # {"options": [...]} body). Unlike a slash command there is no
    # response_url-style side channel, so each handler must run BEFORE the
    # single ack this envelope gets. Confirmed gap: this listener previously
    # had ZERO view_submission handling, so neither the Relay Revise modal
    # nor Quote Ready's Modify modal ever actually processed a submission
    # over Socket Mode (the production fa-relay-slack-listener.service
    # transport).
    if request.type == "interactive" and payload.get("type") == "view_submission":
        callback_id = (payload.get("view") or {}).get("callback_id")
        user_id = payload.get("user", {}).get("id", "?")

        if callback_id == "fa_max_new_file_submit":
            # This handler's DB work (profiled live: ~10s against this
            # dev DB's network latency, well past Slack's ~3s Socket Mode
            # ack window) cannot ride the ack the way fa_max_revise_submit /
            # quote_ready_modify_submit below do -- found live during manual
            # E2E testing (Slack reported dispatch_failed even though the DB
            # write eventually succeeded). _new_file_pre_validate covers
            # both of the handler's error-returning checks and needs no DB
            # access, so it runs before the ack; once it passes, ack
            # immediately (closing the modal) and do the real DB work after
            # -- same "ack fast, defer the work" pattern already used for
            # the FA Max slash commands. There is no response_action channel
            # left post-ack, so a DB-layer failure here can only be logged,
            # not shown inline in the modal.
            from src.api.admin_router import (
                _handle_new_file_view_submit,
                _new_file_pre_validate,
            )
            from src.core.database import get_db_context

            try:
                pre_validation_error = _new_file_pre_validate(payload)
            except Exception:
                logger.exception("[RelaySocket] new-file pre-validation raised")
                pre_validation_error = None

            if pre_validation_error is not None:
                client.send_socket_mode_response(
                    SocketModeResponse(envelope_id=request.envelope_id, payload=pre_validation_error)
                )
                return True

            client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))
            try:
                with get_db_context() as db:
                    _handle_new_file_view_submit(payload, db)
            except Exception:
                logger.exception("[RelaySocket] new-file DB work raised after ack")
            return True

        if callback_id == "quote_ready_modify_submit":
            # Split the same way as fa_max_new_file_submit above: everything
            # that can produce an inline {"response_action": "errors", ...}
            # (validation, recompute, persist, commit) runs before the ack,
            # since Slack requires that SAME ack to carry field errors. The
            # two Slack API calls this submission triggers on success --
            # posting the new dossier card and neutralizing the old one --
            # are real network round trips and do NOT need to complete
            # before the modal closes, so they run after via
            # finalize_modify_submission(). Confirmed real gap: this used to
            # run both calls before the ack, on the same ~3s Socket Mode
            # budget that caused a live dispatch_failed for
            # fa_max_new_file_submit's DB work alone.
            from src.api.admin_router import _handle_quote_ready_modify_submission

            try:
                ack_body, finalize_kwargs = _handle_quote_ready_modify_submission(payload)
            except Exception:
                logger.exception("[RelaySocket] quote_ready_modify_submit handler raised")
                ack_body, finalize_kwargs = None, None

            client.send_socket_mode_response(
                SocketModeResponse(envelope_id=request.envelope_id, payload=ack_body)
            )
            if finalize_kwargs is not None:
                from src.services.quote_ready.dossier import finalize_modify_submission
                try:
                    finalize_modify_submission(**finalize_kwargs)
                except Exception:
                    logger.exception(
                        "[RelaySocket] finalize_modify_submission raised after ack for new_result_id=%s",
                        finalize_kwargs.get("new_result_id"),
                    )
            return True

        response_body: Optional[dict] = None
        try:
            if callback_id == "fa_max_revise_submit":
                # _handle_relay_revise_submission raises HTTPException on
                # malformed view metadata -- that's caught by FastAPI's own
                # exception middleware on the HTTP path, but there is no
                # such middleware here. Left uncaught, this envelope would
                # simply never get acked at all: Slack sees a silent
                # 3-second timeout rather than a clean error.
                from src.api.admin_router import _handle_relay_revise_submission
                response_body = _handle_relay_revise_submission(payload)
            elif callback_id == "quote_ready_override_arv_submit":
                # A single DB write with no follow-on Slack API calls (see
                # _handle_quote_ready_override_arv_submission's docstring),
                # so unlike quote_ready_modify_submit above it needs no
                # ack-first split -- safe to run fully before the ack.
                from src.api.admin_router import _handle_quote_ready_override_arv_submission
                response_body = _handle_quote_ready_override_arv_submission(payload)
            else:
                logger.info(
                    "[RelaySocket] view_submission with unrecognized callback_id=%r user=%s — "
                    "no handler registered, modal will silently close",
                    callback_id, user_id,
                )
        except Exception:
            logger.exception(
                "[RelaySocket] view_submission handler raised for callback_id=%s", callback_id,
            )
            response_body = None
        logger.info(
            "[RelaySocket] view_submission callback_id=%r user=%s -> response=%s",
            callback_id, user_id, "errors" if (response_body or {}).get("response_action") == "errors" else "ok",
        )
        client.send_socket_mode_response(
            SocketModeResponse(envelope_id=request.envelope_id, payload=response_body)
        )
        return True

    if (
        request.type == "interactive"
        and payload.get("type") == "block_suggestion"
        and payload.get("action_id") == "borrower_search"
    ):
        from src.api.admin_router import _handle_borrower_search_suggestion
        from src.core.database import get_db_context

        try:
            with get_db_context() as db:
                result = _handle_borrower_search_suggestion(payload, db)
        except Exception:
            logger.exception("[RelaySocket] block_suggestion handler raised for borrower_search")
            result = {"options": []}
        client.send_socket_mode_response(
            SocketModeResponse(envelope_id=request.envelope_id, payload=result)
        )
        return True

    # Slack requires this acknowledgement within three seconds.  The durable
    # handler is intentionally called only after it, and remains idempotent.
    client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))

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
        _handle_quote_ready_decision,
        _handle_quote_ready_modify_open,
        _handle_new_file_new_borrower_click,
    )
    from src.core.database import get_db_context

    user_id = payload.get("user", {}).get("id", "?")

    # WP-T2-6 addendum: "Not on this list -- new borrower" button inside
    # the new-file modal. No DB session and no ephemeral reply --
    # the handler's only effect is a views.update call swapping the modal
    # in place, and it always returns {}.
    if action_id == "new_file_new_borrower":
        logger.info("[RelaySocket] new-file new-borrower click user=%s", user_id)
        try:
            _handle_new_file_new_borrower_click(payload)
        except Exception:
            logger.exception("[RelaySocket] failed to open new-borrower view")
        return True

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

    # Quote Ready dossier decision (WP-8B — MONEY lane)
    if action_id in ("quote_ready_approve", "quote_ready_reject"):
        logger.info("[RelaySocket] quote-ready decision: action_id=%s user=%s", action_id, user_id)
        with get_db_context() as db:
            result = _handle_quote_ready_decision(payload, db)
        _post_socket_ephemeral(client, payload, result)
        return True
    if action_id == "quote_ready_modify":
        logger.info("[RelaySocket] quote-ready modify (opening modal): user=%s", user_id)
        with get_db_context() as db:
            result = _handle_quote_ready_modify_open(payload, db)
        _post_socket_ephemeral(client, payload, result)
        return True

    # Dial List card buttons — cards are posted by the FA Max bot (delivery.py)
    # so their block_actions envelopes arrive here. No separate listener handled
    # them before; without this branch every dial_* click was silently dropped.
    # Runs regardless of FA_MAX_SLACK_SINGLE_SOCKET (always was the right home).
    if action_id and action_id.startswith("dial_"):
        from src.services.dial_list.actions import handle_action as _dial_handle_action

        logger.info("[RelaySocket] dial-list action: action_id=%s user=%s", action_id, user_id)
        with get_db_context() as db:
            dial_result = _dial_handle_action(
                payload, db,
                approver_id=get_settings().dial_list_approver_user_id,
                client=client.web_client,
            )
        if dial_result.status in ("error", "ignored"):
            logger.warning(
                "[RelaySocket] dial-list action %s not applied: status=%s message=%s",
                action_id, dial_result.status, dial_result.message,
            )
            _post_socket_ephemeral(
                client, payload,
                {"text": dial_result.message or "Could not apply that action."},
            )
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


_FA_MAX_SLASH_COMMANDS = frozenset({
    "/fa-max-new-file", "/fa-max-update-file", "/fa-max-open-files",
})


def _post_slash_reply(response_url: Optional[str], reply: dict) -> None:
    """Deliver a slash command's reply after the 3-second ack window has
    already been used. Best-effort: Slack has nothing to retry against if
    this fails, so log and move on rather than raising into the listener.

    Local copy of src/services/tracked_links.py's helper of the same
    name and shape -- small enough that duplicating it keeps each
    listener's Slack reply plumbing independent, matching this module's
    own contract of unrelated workflows never stepping on each other.
    """
    if not response_url:
        logger.warning("[FaMaxSlash] no response_url on envelope, reply dropped")
        return
    from src.utils.http_helpers import requests_post_with_retry

    try:
        resp = requests_post_with_retry(response_url, json=reply, timeout=5)
        logger.info("[FaMaxSlash] response_url POST -> status=%s", resp.status_code)
    except Exception:
        logger.exception("[FaMaxSlash] failed to deliver reply via response_url")


def handle_fa_max_slash_command_request(client: Any, request: Any) -> bool:
    """Socket Mode envelope handler for '/fa-max-new-file',
    '/fa-max-update-file', and '/fa-max-open-files' (WP-T2-6). This
    Slack app has no Interactivity/slash-command Request URL option once
    Socket Mode is enabled, so these commands -- and view_submission/
    block_suggestion, handled in handle_socket_request above -- are
    unreachable via the HTTP routes in src/api/admin_router.py in this
    deployment. The same pure functions those routes call are reused here
    rather than duplicated.

    Returns True only when this listener handled one of these commands --
    every other envelope is left untouched, same contract as
    handle_tracked_link_socket_request.
    """
    if request.type != "slash_commands":
        return False
    payload = request.payload or {}
    command = payload.get("command")
    if command not in _FA_MAX_SLASH_COMMANDS:
        return False

    from slack_sdk.socket_mode.response import SocketModeResponse

    # Ack bare immediately, same discipline as _on_request /
    # handle_tracked_link_socket_request -- the real reply goes out via
    # response_url once the (fast) DB work below is done.
    client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))

    response_url = payload.get("response_url")
    if command == "/fa-max-new-file":
        from src.api.admin_router import _fa_max_new_file_command

        reply = _fa_max_new_file_command(payload)
    elif command == "/fa-max-update-file":
        from src.api.admin_router import _fa_max_update_file_command
        from src.core.database import get_db_context

        with get_db_context() as db:
            reply = _fa_max_update_file_command(payload, db)
    else:
        from src.api.admin_router import _fa_max_open_files_command
        from src.core.database import get_db_context

        with get_db_context() as db:
            reply = _fa_max_open_files_command(payload, db)

    if reply.get("text"):
        # A reply with no text (e.g. _fa_max_new_file_command's
        # success case -- the modal already opened via views.open, there
        # is nothing left to say) was harmless as a direct HTTP ack, but
        # POSTing it to response_url is a real Slack API call, and Slack
        # rejects a contentless ephemeral message with a 500. Simplest
        # correct behavior: nothing to say means nothing to post.
        _post_slash_reply(response_url, reply)
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

    def _on_fa_max_slash_request(client: Any, request: Any) -> None:
        try:
            handle_fa_max_slash_command_request(client, request)
        except Exception:
            logger.exception("[RelaySocket] failed to process FA Max slash command request")

    socket.socket_mode_request_listeners.append(_on_request)
    socket.socket_mode_request_listeners.append(_on_tracked_link_request)
    socket.socket_mode_request_listeners.append(_on_fa_max_slash_request)

    # FA_MAX_SLACK_SINGLE_SOCKET: Relay is the sole socket owner. Forward CC
    # channel messages to Cora's cc:events stream so Cora's worker handles them.
    # Registered LAST so it runs after _on_request has already acked the envelope
    # — a slow Redis call here cannot delay the ack or cause Slack to redeliver.
    if get_settings().fa_max_slack_single_socket:
        from src.agents.cora.command_center import slack_socket as cc_socket

        if cc_socket.init_forwarder(web):
            def _on_cc_message(client: Any, request: Any) -> None:
                if request.type != "events_api":
                    return
                payload = request.payload or {}
                event = payload.get("event") or {}
                if event.get("type") == "message":
                    cc_socket.forward_message(event, payload.get("event_id"))

            socket.socket_mode_request_listeners.append(_on_cc_message)
            logger.info("[RelaySocket] CC forwarding enabled")
        else:
            logger.error("[RelaySocket] CC forwarding disabled: see cc.socket error above")

    logger.info("[RelaySocket] connecting via Socket Mode")
    socket.connect()

    # Keep the process alive. SocketModeClient manages reconnects itself.
    from threading import Event
    Event().wait()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
