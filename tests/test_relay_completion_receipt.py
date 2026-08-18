"""
Tests for relay completion receipt (post_completion_receipt).

Verifies the correct Slack message is posted after execute_batch() returns,
covering success, partial failure, halted, and unconfigured cases.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.services.relay.engine import BatchResult
from src.services.relay.slack_post import post_completion_receipt


def _result(sent=0, failed=0, deferred=0, skipped=0, halted=False) -> BatchResult:
    r = BatchResult()
    r.sent = sent
    r.failed = failed
    r.deferred = deferred
    r.skipped = skipped
    r.halted = halted
    return r


@pytest.fixture
def slack_configured(monkeypatch):
    monkeypatch.setattr(
        "src.services.relay.slack_post.get_settings",
        lambda: MagicMock(
            slack_bot_token=MagicMock(get_secret_value=lambda: "xoxb-test"),
        ),
    )
    monkeypatch.setattr(
        "src.services.relay.slack_post.get_venture_config",
        lambda *_: MagicMock(relay_slack_channel="#agent-daily"),
    )


@pytest.fixture
def capture_post(slack_configured):
    posted = []
    with patch("slack_sdk.WebClient") as MockClient:
        MockClient.return_value.chat_postMessage.side_effect = lambda **kw: posted.append(kw)
        yield posted, MockClient


def test_success_receipt_posts_to_correct_channel(capture_post):
    posted, _ = capture_post
    post_completion_receipt("batch-abc123", _result(sent=6), venture_key="test")
    assert len(posted) == 1
    assert posted[0]["channel"] == "#agent-daily"


def test_success_receipt_contains_checkmark_and_counts(capture_post):
    posted, _ = capture_post
    post_completion_receipt("batch-abc123", _result(sent=6, deferred=1), venture_key="test")
    text = posted[0]["text"]
    assert "✅" in text
    assert "6 sent" in text
    assert "1 deferred" in text
    assert "batch-abc123" in text


def test_failure_receipt_contains_warning_and_counts(capture_post):
    posted, _ = capture_post
    post_completion_receipt("batch-xyz", _result(sent=4, failed=2), venture_key="test")
    text = posted[0]["text"]
    assert "⚠️" in text
    assert "2 failed" in text
    assert "check logs" in text


def test_halted_receipt_contains_stop_sign(capture_post):
    posted, _ = capture_post
    post_completion_receipt("batch-xyz", _result(sent=1, halted=True), venture_key="test")
    text = posted[0]["text"]
    assert "🛑" in text
    assert "kill switch" in text


def test_noop_when_no_bot_token(monkeypatch):
    monkeypatch.setattr(
        "src.services.relay.slack_post.get_settings",
        lambda: MagicMock(slack_bot_token=None),
    )
    monkeypatch.setattr(
        "src.services.relay.slack_post.get_venture_config",
        lambda *_: MagicMock(relay_slack_channel="#agent-daily"),
    )
    with patch("slack_sdk.WebClient") as MockClient:
        post_completion_receipt("batch-abc", _result(sent=3), venture_key="test")
        MockClient.return_value.chat_postMessage.assert_not_called()


def test_noop_when_no_channel(monkeypatch):
    monkeypatch.setattr(
        "src.services.relay.slack_post.get_settings",
        lambda: MagicMock(
            slack_bot_token=MagicMock(get_secret_value=lambda: "xoxb-test"),
        ),
    )
    monkeypatch.setattr(
        "src.services.relay.slack_post.get_venture_config",
        lambda *_: MagicMock(relay_slack_channel=""),
    )
    with patch("slack_sdk.WebClient") as MockClient:
        post_completion_receipt("batch-abc", _result(sent=3), venture_key="test")
        MockClient.return_value.chat_postMessage.assert_not_called()


def test_does_not_raise_on_slack_api_error(slack_configured):
    with patch("slack_sdk.WebClient") as MockClient:
        MockClient.return_value.chat_postMessage.side_effect = Exception("network error")
        post_completion_receipt("batch-abc", _result(sent=3), venture_key="test")
