"""WP-7 Phase B — /tracked-link registered on the shared FA Max Socket Mode
connection (src/services/relay/socket_listener.py), alongside the existing
Relay approval listener from PR #276 (wp2-socket-mode-approvals) and the
WP-T2-6 FA Max slash-command listener (/fa-max-new-file,
/fa-max-update-file).

Only tests the *wiring* — that run() registers all three listeners on one
connection and never blocks the test on socket.connect(), and that a
slash_commands envelope dispatched through every registered listener (as
run() wires them) is acked exactly once, by the listener that owns it. Each
command's own parsing/logic is tested in its own PR
(tests/test_tracked_link_slack_command.py for /tracked-link;
tests/api/test_fa_max_update_file_slack_command.py and
tests/api/test_new_file_slack_wiring.py for the FA Max commands),
since that logic lives in src/services/tracked_links.py and
src/api/admin_router.py, not here.
"""
from unittest import mock

from config.settings import get_settings
from src.services.relay import socket_listener


def test_run_registers_all_three_listeners(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "fa_max_slack_app_token", mock.Mock(get_secret_value=lambda: "xapp-test"))
    monkeypatch.setattr(settings, "fa_max_slack_bot_token", mock.Mock(get_secret_value=lambda: "xoxb-test"))
    monkeypatch.setattr(settings, "fa_max_slack_single_socket", False)
    monkeypatch.setattr(socket_listener, "get_settings", lambda: settings)

    fake_socket = mock.MagicMock()
    fake_socket.socket_mode_request_listeners = []
    fake_socket.connect = mock.MagicMock()

    # run() ends with threading.Event().wait() to keep the daemon process
    # alive forever by design — patch it so the test returns instead of
    # hanging (this is what the real process is SUPPOSED to do; nothing to
    # fix in socket_listener.py itself).
    fake_event = mock.MagicMock()
    fake_event.wait = mock.MagicMock(return_value=None)

    with mock.patch("slack_sdk.socket_mode.SocketModeClient", return_value=fake_socket), \
         mock.patch("slack_sdk.WebClient", return_value=mock.MagicMock()), \
         mock.patch("threading.Event", return_value=fake_event):
        socket_listener.run()

    assert len(fake_socket.socket_mode_request_listeners) == 3
    fake_socket.connect.assert_called_once()


def test_run_does_not_start_without_both_tokens(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "fa_max_slack_app_token", None)
    monkeypatch.setattr(socket_listener, "get_settings", lambda: settings)

    with mock.patch("slack_sdk.socket_mode.SocketModeClient") as client_cls:
        socket_listener.run()
        client_cls.assert_not_called()


def test_slash_command_envelope_is_acked_once_by_the_owning_listener(monkeypatch):
    """Regression for the double-ack bug found in PR #278 review.

    A /tracked-link (``slash_commands``) envelope dispatched through BOTH
    registered listeners, in the order run() wires them, must be acknowledged
    exactly once — by handle_tracked_link_socket_request, carrying its real
    reply payload — never by the Relay listener's unconditional ack.
    """
    settings = get_settings()
    monkeypatch.setattr(settings, "fa_max_slack_app_token", mock.Mock(get_secret_value=lambda: "xapp-test"))
    monkeypatch.setattr(settings, "fa_max_slack_bot_token", mock.Mock(get_secret_value=lambda: "xoxb-test"))
    monkeypatch.setattr(settings, "fa_max_slack_single_socket", False)
    monkeypatch.setattr(socket_listener, "get_settings", lambda: settings)

    fake_socket = mock.MagicMock()
    fake_socket.socket_mode_request_listeners = []
    fake_socket.connect = mock.MagicMock()
    fake_event = mock.MagicMock()
    fake_event.wait = mock.MagicMock(return_value=None)

    real_reply = {"response_type": "ephemeral", "text": "Created: https://example.com/t/abc123"}

    with mock.patch("slack_sdk.socket_mode.SocketModeClient", return_value=fake_socket), \
         mock.patch("slack_sdk.WebClient", return_value=mock.MagicMock()), \
         mock.patch("threading.Event", return_value=fake_event):
        socket_listener.run()

    listeners = list(fake_socket.socket_mode_request_listeners)
    assert len(listeners) == 3

    request = mock.Mock(
        type="slash_commands",
        envelope_id="env-1",
        payload={"command": "/tracked-link", "text": "partner acme", "channel_id": "C123", "user_name": "josh"},
    )
    client = mock.MagicMock()

    with mock.patch(
        "src.services.tracked_links.handle_tracked_link_socket_request",
        return_value=True,
    ) as fake_handler:
        def _send_real_reply(client, request):
            client.send_socket_mode_response(
                mock.Mock(envelope_id=request.envelope_id, payload=real_reply)
            )
            return True

        fake_handler.side_effect = _send_real_reply

        for listener in listeners:
            listener(client, request)

    assert client.send_socket_mode_response.call_count == 1
    sent = client.send_socket_mode_response.call_args.args[0]
    assert sent.payload == real_reply


def test_fa_max_slash_command_envelope_is_acked_once_by_the_owning_listener(monkeypatch):
    """Same regression coverage as the /tracked-link test above, for the
    WP-T2-6 /fa-max-update-file command -- must not be double-acked by
    the Relay listener's unconditional ack, since all three listeners on
    this shared connection see every envelope."""
    settings = get_settings()
    monkeypatch.setattr(settings, "fa_max_slack_app_token", mock.Mock(get_secret_value=lambda: "xapp-test"))
    monkeypatch.setattr(settings, "fa_max_slack_bot_token", mock.Mock(get_secret_value=lambda: "xoxb-test"))
    monkeypatch.setattr(settings, "fa_max_slack_single_socket", False)
    monkeypatch.setattr(socket_listener, "get_settings", lambda: settings)

    fake_socket = mock.MagicMock()
    fake_socket.socket_mode_request_listeners = []
    fake_socket.connect = mock.MagicMock()
    fake_event = mock.MagicMock()
    fake_event.wait = mock.MagicMock(return_value=None)

    with mock.patch("slack_sdk.socket_mode.SocketModeClient", return_value=fake_socket), \
         mock.patch("slack_sdk.WebClient", return_value=mock.MagicMock()), \
         mock.patch("threading.Event", return_value=fake_event):
        socket_listener.run()

    listeners = list(fake_socket.socket_mode_request_listeners)
    assert len(listeners) == 3

    request = mock.Mock(
        type="slash_commands",
        envelope_id="env-2",
        payload={
            "command": "/fa-max-update-file", "text": "BF-1 under_review",
            "user_id": "U123", "channel_id": "C123", "response_url": "https://hooks.slack.test/x",
        },
    )
    client = mock.MagicMock()

    with mock.patch(
        "src.api.admin_router._fa_max_update_file_command",
        return_value={"response_type": "ephemeral", "text": "BF-1 updated to stage: under_review."},
    ), mock.patch("src.utils.http_helpers.requests_post_with_retry") as mock_post:
        mock_post.return_value = mock.Mock(status_code=200, text="ok")
        for listener in listeners:
            listener(client, request)

    # Only the bare, blank ack from handle_fa_max_slash_command_request --
    # the real reply goes via response_url (_post_slash_reply), never
    # through send_socket_mode_response a second time.
    assert client.send_socket_mode_response.call_count == 1
    sent = client.send_socket_mode_response.call_args.args[0]
    assert sent.payload is None
