"""
PR-290 review-fix regression test.

Covers: run_socket_mode must fail closed (refuse to start) if auth.test
never succeeds, instead of running with the bot self-reply filter
permanently disabled.
"""
from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from src.agents.cora.command_center import slack_socket


def test_run_socket_mode_refuses_to_start_when_auth_test_always_fails():
    settings = MagicMock()
    settings.fa_max_slack_bot_token.get_secret_value.return_value = "xoxb-fake"

    with patch.object(slack_socket, "_get_app_token", return_value="xapp-fake"), \
         patch("slack_sdk.WebClient") as MockWebClient, \
         patch("slack_sdk.socket_mode.SocketModeClient") as MockSocketClient, \
         patch("config.settings.get_settings", return_value=settings), \
         patch("time.sleep"):
        MockWebClient.return_value.auth_test.side_effect = RuntimeError("boom")

        slack_socket.run_socket_mode(threading.Event())

        MockSocketClient.assert_not_called()
        assert slack_socket._BOT_USER_ID is None
