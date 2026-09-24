"""The handoff-rejection Slack post must stay silent during a test run.

A full-suite run exercises the real rejection path with fixture data. Slack
credentials are present in this checkout, so without a test-aware guard every
run posts a burst of rejections that reads like a live incident.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

from src.agents.contracts.base import HandoffRejected, notify_slack_rejection


def _configured_settings() -> MagicMock:
    """Settings with Slack fully configured, so only the pytest guard can stop a post."""
    settings = MagicMock()
    settings.slack_bot_token = MagicMock()
    settings.slack_bot_token.get_secret_value.return_value = "xoxb-fake"
    settings.quality_contracts_slack_channel = "C_CONTRACTS"
    return settings


def _rejection() -> HandoffRejected:
    return HandoffRejected("hunter_to_cora", ["confidence_score: 40 < 70"], "OPP-2026-00042")


def test_no_slack_post_while_running_under_pytest():
    mock_client_class = MagicMock()
    with patch("src.agents.contracts.base.get_settings", return_value=_configured_settings()), \
         patch("slack_sdk.WebClient", mock_client_class):
        notify_slack_rejection(_rejection())

    mock_client_class.assert_not_called()


def test_slack_post_still_sent_outside_pytest():
    """Control for test_no_slack_post_while_running_under_pytest.

    Without it that test also passes if the post were broken or removed
    entirely, leaving real rejections silently undelivered.
    """
    mock_client_class = MagicMock()
    modules_without_pytest = {k: v for k, v in sys.modules.items() if k != "pytest"}

    with patch.dict(sys.modules, modules_without_pytest, clear=True), \
         patch("src.agents.contracts.base.get_settings", return_value=_configured_settings()), \
         patch("slack_sdk.WebClient", mock_client_class):
        notify_slack_rejection(_rejection())

    mock_client_class.assert_called_once()
    posted = mock_client_class.return_value.chat_postMessage
    posted.assert_called_once()
    assert posted.call_args.kwargs["channel"] == "C_CONTRACTS"
    assert "hunter_to_cora" in posted.call_args.kwargs["text"]
