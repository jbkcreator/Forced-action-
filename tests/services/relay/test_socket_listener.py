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


class TestViewSubmissionDispatch:
    def test_log_submission_modal_acks_with_handler_result(self):
        client = mock.MagicMock()
        payload = {
            "type": "view_submission",
            "view": {"callback_id": "fa_max_log_submission_submit"},
        }
        request = _request("interactive", "env-2", payload)
        result = {"response_action": "errors", "errors": {"new_full_name_block": "required"}}

        with mock.patch(
            "src.api.admin_router._handle_log_submission_view_submit", return_value=result,
        ) as mock_handle, mock.patch("src.core.database.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__.return_value = mock.MagicMock()
            handled = socket_listener.handle_socket_request(client, request)

        assert handled is True
        mock_handle.assert_called_once()
        assert mock_handle.call_args.args[0] == payload
        client.send_socket_mode_response.assert_called_once()
        sent = client.send_socket_mode_response.call_args.args[0]
        assert sent.envelope_id == "env-2"
        assert sent.payload == result

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
        request = _request("interactive", "env-8", {"command": "/fa-max-file-update"})
        assert socket_listener.handle_fa_max_slash_command_request(client, request) is False
        client.send_socket_mode_response.assert_not_called()

    def test_file_update_acks_blank_then_replies_via_response_url(self):
        client = mock.MagicMock()
        payload = {
            "command": "/fa-max-file-update", "text": "BF-1 under_review",
            "user_id": "U123", "channel_id": "C123",
            "response_url": "https://hooks.slack.test/x",
        }
        request = _request("slash_commands", "env-9", payload)
        reply = {"response_type": "ephemeral", "text": "BF-1 updated to stage: under_review."}

        with mock.patch(
            "src.api.admin_router._fa_max_file_update_command", return_value=reply,
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

    def test_log_submission_success_has_nothing_to_say_and_does_not_post(self):
        # Regression: _fa_max_log_submission_command's success case
        # returns {"response_type": "ephemeral"} with no "text" -- the
        # modal already opened via views.open, there's nothing left to
        # say. That's harmless as a direct HTTP ack, but POSTing it to
        # response_url is a real Slack API call, and Slack rejects a
        # contentless ephemeral with a 500 (confirmed against the live
        # test app during manual E2E testing).
        client = mock.MagicMock()
        payload = {
            "command": "/fa-max-log-submission", "trigger_id": "trig-1",
            "user_id": "U123", "channel_id": "C123",
            "response_url": "https://hooks.slack.test/y",
        }
        request = _request("slash_commands", "env-10", payload)
        reply = {"response_type": "ephemeral"}

        with mock.patch(
            "src.api.admin_router._fa_max_log_submission_command", return_value=reply,
        ) as mock_handle, mock.patch(
            "src.utils.http_helpers.requests_post_with_retry",
        ) as mock_post:
            handled = socket_listener.handle_fa_max_slash_command_request(client, request)

        assert handled is True
        mock_handle.assert_called_once_with(payload)
        mock_post.assert_not_called()

    def test_log_submission_failure_reply_has_text_and_does_post(self):
        client = mock.MagicMock()
        payload = {
            "command": "/fa-max-log-submission", "trigger_id": "trig-1",
            "user_id": "U123", "channel_id": "C123",
            "response_url": "https://hooks.slack.test/y",
        }
        request = _request("slash_commands", "env-10b", payload)
        reply = {"response_type": "ephemeral", "text": "Couldn't open the form — try again in a moment."}

        with mock.patch(
            "src.api.admin_router._fa_max_log_submission_command", return_value=reply,
        ), mock.patch(
            "src.utils.http_helpers.requests_post_with_retry",
        ) as mock_post:
            mock_post.return_value = mock.Mock(status_code=200, text="ok")
            handled = socket_listener.handle_fa_max_slash_command_request(client, request)

        assert handled is True
        mock_post.assert_called_once_with("https://hooks.slack.test/y", json=reply, timeout=5)

    def test_missing_response_url_drops_reply_without_raising(self):
        client = mock.MagicMock()
        payload = {"command": "/fa-max-file-update", "text": "BF-1 under_review"}
        request = _request("slash_commands", "env-11", payload)

        with mock.patch(
            "src.api.admin_router._fa_max_file_update_command",
            return_value={"response_type": "ephemeral", "text": "done"},
        ):
            handled = socket_listener.handle_fa_max_slash_command_request(client, request)

        assert handled is True
