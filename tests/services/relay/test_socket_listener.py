"""tests/services/relay/test_socket_listener.py

WP-T2-6: Socket Mode is the only Interactivity/slash-command delivery
mechanism for this Slack app (no Request URL option once Socket Mode is
enabled), so handle_socket_request's view_submission/block_suggestion
branches and handle_fa_max_slash_command_request are what actually serves
the log-submission modal and both FA Max slash commands in production --
the HTTP routes in src/api/admin_router.py are reachable only on a
dev/test app running with Socket Mode off.
"""
from __future__ import annotations

from unittest import mock

from src.services.relay import socket_listener


def _request(type_: str, envelope_id: str, payload: dict):
    return mock.Mock(type=type_, envelope_id=envelope_id, payload=payload)


class TestHandleSocketRequestIgnoresUnownedEnvelopes:
    def test_ignores_slash_commands(self):
        client = mock.MagicMock()
        request = _request("slash_commands", "env-1", {"command": "/tracked-link"})
        assert socket_listener.handle_socket_request(client, request) is False
        client.send_socket_mode_response.assert_not_called()


class TestNewBorrowerButtonClick:
    def test_calls_handler_and_acks_blank(self):
        # Regression: "new_file_new_borrower" was never dispatched
        # in this Socket-Mode-only app -- clicking the button silently
        # did nothing (blank ack, no view.update, no log line), found
        # live during manual E2E testing.
        client = mock.MagicMock()
        payload = {
            "type": "block_actions",
            "actions": [{"action_id": "new_file_new_borrower"}],
            "user": {"id": "U123"},
            "view": {"id": "V1", "hash": "h1", "private_metadata": '{"mode": "search"}'},
        }
        request = _request("interactive", "env-50", payload)

        with mock.patch(
            "src.api.admin_router._handle_new_file_new_borrower_click", return_value={},
        ) as mock_handle:
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        mock_handle.assert_called_once_with(payload)
        # Blank ack -- the real effect is the handler's own views.update call.
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.payload is None

    def test_handler_raising_does_not_crash_dispatch(self):
        client = mock.MagicMock()
        payload = {
            "type": "block_actions",
            "actions": [{"action_id": "new_file_new_borrower"}],
            "user": {"id": "U123"},
            "view": {"id": "V1", "hash": "h1", "private_metadata": "not json"},
        }
        request = _request("interactive", "env-51", payload)

        with mock.patch(
            "src.api.admin_router._handle_new_file_new_borrower_click",
            side_effect=RuntimeError("boom"),
        ):
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True


class TestNewFileAckFastDeferWork:
    """fa_max_new_file_submit's DB work (profiled live: ~10s
    against this dev DB's network latency) blows past Slack's ~3s
    Socket Mode ack window -- found live during manual E2E testing
    (Slack reported dispatch_failed even though the DB write eventually
    succeeded). It now runs _new_file_pre_validate (DB-free)
    BEFORE acking, and defers the real DB work to after the ack.
    """

    def test_pre_validation_failure_acks_with_errors_without_touching_db(self):
        client = mock.MagicMock()
        payload = {
            "type": "view_submission",
            "view": {"callback_id": "fa_max_new_file_submit"},
        }
        request = _request("interactive", "env-2", payload)
        error = {"response_action": "errors", "errors": {"new_full_name_block": "Borrower name is required."}}

        with mock.patch(
            "src.api.admin_router._new_file_pre_validate", return_value=error,
        ) as mock_prevalidate, mock.patch(
            "src.api.admin_router._handle_new_file_view_submit",
        ) as mock_handle:
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        mock_prevalidate.assert_called_once_with(payload)
        mock_handle.assert_not_called()
        client.send_socket_mode_response.assert_called_once()
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.envelope_id == "env-2"
        assert sent.payload == error

    def test_pre_validation_pass_acks_blank_then_runs_db_work_after(self):
        client = mock.MagicMock()
        payload = {
            "type": "view_submission",
            "view": {"callback_id": "fa_max_new_file_submit"},
        }
        request = _request("interactive", "env-2b", payload)

        with mock.patch(
            "src.api.admin_router._new_file_pre_validate", return_value=None,
        ), mock.patch(
            "src.api.admin_router._handle_new_file_view_submit", return_value={},
        ) as mock_handle, mock.patch("src.core.database.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__.return_value = mock.MagicMock()
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        mock_handle.assert_called_once()
        assert mock_handle.call_args.args[0] == payload
        # Exactly one ack, sent before the (slow) DB work, carrying no
        # payload -- there is no response_action channel left afterward.
        client.send_socket_mode_response.assert_called_once()
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.payload is None

    def test_db_work_raising_after_ack_is_logged_not_raised(self):
        client = mock.MagicMock()
        payload = {
            "type": "view_submission",
            "view": {"callback_id": "fa_max_new_file_submit"},
        }
        request = _request("interactive", "env-2c", payload)

        with mock.patch(
            "src.api.admin_router._new_file_pre_validate", return_value=None,
        ), mock.patch(
            "src.api.admin_router._handle_new_file_view_submit",
            side_effect=RuntimeError("db exploded"),
        ), mock.patch("src.core.database.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__.return_value = mock.MagicMock()
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        client.send_socket_mode_response.assert_called_once()


class TestViewSubmissionDispatch:
    def test_revise_modal_acks_with_handler_result(self):
        # Pre-existing Revise modal -- same Socket-Mode-only gap, fixed by
        # the same branch that fixes the new log-submission modal.
        client = mock.MagicMock()
        payload = {"type": "view_submission", "view": {"callback_id": "fa_max_revise_submit"}}
        request = _request("interactive", "env-3", payload)
        result = {"response_action": "clear"}

        with mock.patch(
            "src.api.admin_router._handle_relay_revise_submission", return_value=result,
        ) as mock_handle:
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        mock_handle.assert_called_once_with(payload)
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.payload == result

    def test_revise_modal_handler_raising_http_exception_still_acks(self):
        # Regression: _handle_relay_revise_submission raises HTTPException
        # on malformed view metadata -- caught by FastAPI's middleware on
        # the HTTP path, but there is no such middleware here. Before this
        # fix, that exception would propagate past the ack entirely, and
        # Slack would see a silent 3-second timeout instead of any
        # response for the envelope.
        from fastapi import HTTPException

        client = mock.MagicMock()
        payload = {"type": "view_submission", "view": {"callback_id": "fa_max_revise_submit"}}
        request = _request("interactive", "env-99", payload)

        with mock.patch(
            "src.api.admin_router._handle_relay_revise_submission",
            side_effect=HTTPException(status_code=400, detail="Invalid view metadata"),
        ):
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        client.send_socket_mode_response.assert_called_once()
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.envelope_id == "env-99"
        assert sent.payload == {}

    def test_unknown_callback_id_acks_blank_and_returns_true(self):
        client = mock.MagicMock()
        payload = {"type": "view_submission", "view": {"callback_id": "something_else"}}
        request = _request("interactive", "env-4", payload)

        handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.payload == {}


class TestBlockSuggestionDispatch:
    def test_borrower_search_acks_with_options(self):
        client = mock.MagicMock()
        payload = {"type": "block_suggestion", "action_id": "borrower_search", "value": "Jane"}
        request = _request("interactive", "env-5", payload)
        result = {"options": [{"text": {"type": "plain_text", "text": "Jane Doe"}, "value": "p-1"}]}

        with mock.patch(
            "src.api.admin_router._handle_borrower_search_suggestion", return_value=result,
        ) as mock_handle:
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        mock_handle.assert_called_once()
        assert mock_handle.call_args.args[0] == payload
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.payload == result

    def test_handler_raising_still_acks_with_empty_options(self):
        client = mock.MagicMock()
        payload = {"type": "block_suggestion", "action_id": "borrower_search", "value": "Jane"}
        request = _request("interactive", "env-97", payload)

        with mock.patch(
            "src.api.admin_router._handle_borrower_search_suggestion",
            side_effect=RuntimeError("db exploded"),
        ):
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.payload == {"options": []}

    def test_other_block_suggestion_action_id_falls_through_to_block_actions_path(self):
        # Not borrower_search, and no "actions" list either -- must not
        # crash indexing into an absent "actions" key, must return False.
        client = mock.MagicMock()
        payload = {"type": "block_suggestion", "action_id": "something_else"}
        request = _request("interactive", "env-6", payload)

        handled = socket_listener.handle_socket_request(client, request)

        assert handled is False
        # Falls through to the unconditional blank ack every other
        # interactive envelope gets before the block_actions type check.
        client.send_socket_mode_response.assert_called_once()


class TestFaMaxSlashCommandRequest:
    def test_ignores_other_slash_commands(self):
        client = mock.MagicMock()
        request = _request("slash_commands", "env-7", {"command": "/tracked-link"})
        assert socket_listener.handle_fa_max_slash_command_request(client, request) is False
        client.send_socket_mode_response.assert_not_called()

    def test_ignores_non_slash_command_envelopes(self):
        client = mock.MagicMock()
        request = _request("interactive", "env-8", {"command": "/fa-max-update-file"})
        assert socket_listener.handle_fa_max_slash_command_request(client, request) is False
        client.send_socket_mode_response.assert_not_called()

    def test_backflip_files_acks_blank_then_replies_via_response_url(self):
        client = mock.MagicMock()
        payload = {
            "command": "/fa-max-open-files",
            "user_id": "U123", "channel_id": "C123",
            "response_url": "https://hooks.slack.test/z",
        }
        request = _request("slash_commands", "env-9b", payload)
        reply = {"response_type": "ephemeral", "text": "Jane Doe — bl1234 — under_review"}

        with mock.patch(
            "src.api.admin_router._fa_max_open_files_command", return_value=reply,
        ) as mock_handle, mock.patch(
            "src.core.database.get_db_context",
        ) as mock_ctx, mock.patch(
            "src.utils.http_helpers.requests_post_with_retry",
        ) as mock_post:
            mock_ctx.return_value.__enter__.return_value = mock.MagicMock()
            mock_post.return_value = mock.Mock(status_code=200, text="ok")
            handled = socket_listener.handle_fa_max_slash_command_request(client, request)

        assert handled is True
        mock_handle.assert_called_once()
        assert mock_handle.call_args.args[0] == payload
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.payload is None
        mock_post.assert_called_once_with("https://hooks.slack.test/z", json=reply, timeout=5)

    def test_file_update_acks_blank_then_replies_via_response_url(self):
        client = mock.MagicMock()
        payload = {
            "command": "/fa-max-update-file", "text": "BF-1 under_review",
            "user_id": "U123", "channel_id": "C123",
            "response_url": "https://hooks.slack.test/x",
        }
        request = _request("slash_commands", "env-9", payload)
        reply = {"response_type": "ephemeral", "text": "BF-1 updated to stage: under_review."}

        with mock.patch(
            "src.api.admin_router._fa_max_update_file_command", return_value=reply,
        ) as mock_handle, mock.patch(
            "src.utils.http_helpers.requests_post_with_retry",
        ) as mock_post:
            mock_post.return_value = mock.Mock(status_code=200, text="ok")
            handled = socket_listener.handle_fa_max_slash_command_request(client, request)

        assert handled is True
        mock_handle.assert_called_once()
        assert mock_handle.call_args.args[0] == payload
        # Bare ack -- no payload -- the real reply goes via response_url.
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.payload is None
        mock_post.assert_called_once_with("https://hooks.slack.test/x", json=reply, timeout=5)

    def test_new_file_success_has_nothing_to_say_and_does_not_post(self):
        # Regression: _fa_max_new_file_command's success case
        # returns {"response_type": "ephemeral"} with no "text" -- the
        # modal already opened via views.open, there's nothing left to
        # say. That's harmless as a direct HTTP ack, but POSTing it to
        # response_url is a real Slack API call, and Slack rejects a
        # contentless ephemeral with a 500 (confirmed against the live
        # test app during manual E2E testing).
        client = mock.MagicMock()
        payload = {
            "command": "/fa-max-new-file", "trigger_id": "trig-1",
            "user_id": "U123", "channel_id": "C123",
            "response_url": "https://hooks.slack.test/y",
        }
        request = _request("slash_commands", "env-10", payload)
        reply = {"response_type": "ephemeral"}

        with mock.patch(
            "src.api.admin_router._fa_max_new_file_command", return_value=reply,
        ) as mock_handle, mock.patch(
            "src.utils.http_helpers.requests_post_with_retry",
        ) as mock_post:
            handled = socket_listener.handle_fa_max_slash_command_request(client, request)

        assert handled is True
        mock_handle.assert_called_once_with(payload)
        mock_post.assert_not_called()

    def test_new_file_failure_reply_has_text_and_does_post(self):
        client = mock.MagicMock()
        payload = {
            "command": "/fa-max-new-file", "trigger_id": "trig-1",
            "user_id": "U123", "channel_id": "C123",
            "response_url": "https://hooks.slack.test/y",
        }
        request = _request("slash_commands", "env-10b", payload)
        reply = {"response_type": "ephemeral", "text": "Couldn't open the form — try again in a moment."}

        with mock.patch(
            "src.api.admin_router._fa_max_new_file_command", return_value=reply,
        ), mock.patch(
            "src.utils.http_helpers.requests_post_with_retry",
        ) as mock_post:
            mock_post.return_value = mock.Mock(status_code=200, text="ok")
            handled = socket_listener.handle_fa_max_slash_command_request(client, request)

        assert handled is True
        mock_post.assert_called_once_with("https://hooks.slack.test/y", json=reply, timeout=5)

    def test_missing_response_url_drops_reply_without_raising(self):
        client = mock.MagicMock()
        payload = {"command": "/fa-max-update-file", "text": "BF-1 under_review"}
        request = _request("slash_commands", "env-11", payload)

        with mock.patch(
            "src.api.admin_router._fa_max_update_file_command",
            return_value={"response_type": "ephemeral", "text": "done"},
        ):
            handled = socket_listener.handle_fa_max_slash_command_request(client, request)

        assert handled is True
