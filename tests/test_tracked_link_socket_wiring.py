"""WP-7 Phase B — /tracked-link registered on the shared FA Max Socket Mode
connection (src/services/relay/socket_listener.py), alongside the existing
Relay approval listener from PR #276 (wp2-socket-mode-approvals).

Only tests the *wiring* — that run() registers both listeners on one
connection and never blocks the test on socket.connect(). The command's own
parsing/minting logic is tested in WP-7's own PR
(tests/test_tracked_link_slack_command.py), since that logic lives in
src/services/tracked_links.py, not here.
"""
from unittest import mock

from config.settings import get_settings
from src.services.relay import socket_listener


def test_run_registers_both_relay_and_tracked_link_listeners(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "relay_slack_app_token", mock.Mock(get_secret_value=lambda: "xapp-test"))
    monkeypatch.setattr(settings, "slack_bot_token", mock.Mock(get_secret_value=lambda: "xoxb-test"))
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

    assert len(fake_socket.socket_mode_request_listeners) == 2
    fake_socket.connect.assert_called_once()


def test_run_does_not_start_without_both_tokens(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "relay_slack_app_token", None)
    monkeypatch.setattr(socket_listener, "get_settings", lambda: settings)

    with mock.patch("slack_sdk.socket_mode.SocketModeClient") as client_cls:
        socket_listener.run()
        client_cls.assert_not_called()
