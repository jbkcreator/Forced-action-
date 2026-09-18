"""
Regression tests for the slack_listener seen-claim pattern.

Key scenario: a transient publish failure must NOT permanently drop the message.
After a failed publish the claim is released; the next poll must claim and
publish the same message successfully.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_message(ts: str, text: str, user: str = "U_HUMAN") -> dict:
    return {"ts": ts, "text": text, "user": user}


def _conversations_history_response(messages: list[dict]) -> dict:
    # Slack returns newest-first; the poller reverses them.
    return {"messages": list(reversed(messages))}


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def patch_settings():
    """Silence settings / Slack client bootstrapping for every test."""
    mock_settings = MagicMock()
    mock_settings.slack_bot_token = MagicMock()
    mock_settings.slack_bot_token.get_secret_value.return_value = "xoxb-fake"
    mock_settings.fa_max_slack_channel_relationships = "C_TEST"
    with patch("src.agents.cora.command_center.slack_listener._get_client") as mock_gc, \
         patch("config.settings.get_settings", return_value=mock_settings):
        mock_client = MagicMock()
        mock_gc.return_value = mock_client
        yield mock_client, mock_settings


# ── unit: _claim_seen / _promote_claim / _release_claim ───────────────────────

class TestClaimSeen:
    def test_first_caller_gets_claim(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True  # SET NX succeeded
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.get_redis", return_value=mock_redis):
            from src.agents.cora.command_center.slack_listener import _claim_seen
            claimed, token = _claim_seen("1234.5678")
        assert claimed is True
        assert len(token) == 32  # uuid4().hex

    def test_second_caller_does_not_get_claim(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = None  # SET NX failed — key exists
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.get_redis", return_value=mock_redis):
            from src.agents.cora.command_center.slack_listener import _claim_seen
            claimed, token = _claim_seen("1234.5678")
        assert claimed is False

    def test_redis_unavailable_fails_open(self):
        with patch("src.core.redis_client.redis_available", return_value=False):
            from src.agents.cora.command_center.slack_listener import _claim_seen
            claimed, token = _claim_seen("1234.5678")
        assert claimed is True  # fail-open: treat as new


class TestPromoteClaim:
    def test_promote_extends_ttl_when_token_matches(self):
        mock_redis = MagicMock()
        mock_redis.get.return_value = "mytoken"
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.get_redis", return_value=mock_redis):
            from src.agents.cora.command_center import slack_listener
            slack_listener._promote_claim("1234.5678", "mytoken")
        mock_redis.set.assert_called_once()
        args, kwargs = mock_redis.set.call_args
        assert kwargs.get("ex") == slack_listener._SEEN_TTL_SECONDS

    def test_promote_skipped_when_token_mismatch(self):
        mock_redis = MagicMock()
        mock_redis.get.return_value = "different_token"
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.get_redis", return_value=mock_redis):
            from src.agents.cora.command_center.slack_listener import _promote_claim
            _promote_claim("1234.5678", "mytoken")
        mock_redis.set.assert_not_called()


class TestReleaseClaim:
    def test_lua_release_called_with_correct_args(self):
        mock_redis = MagicMock()
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.get_redis", return_value=mock_redis):
            from src.agents.cora.command_center import slack_listener
            slack_listener._release_claim("1234.5678", "mytoken")
        mock_redis.eval.assert_called_once()
        _script, num_keys, key, token_arg = mock_redis.eval.call_args[0]
        assert num_keys == 1
        assert "1234.5678" in key
        assert token_arg == "mytoken"

    def test_release_skipped_when_no_token(self):
        mock_redis = MagicMock()
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.get_redis", return_value=mock_redis):
            from src.agents.cora.command_center.slack_listener import _release_claim
            _release_claim("1234.5678", "")
        mock_redis.eval.assert_not_called()


# ── integration: poll_once with transient publish failure ─────────────────────

class TestPollOnceTransientFailure:
    """
    Regression for the permanent-drop bug:
      Poll 1: publish fails → claim must be released.
      Poll 2: same message must be claimed again and published successfully.
    """

    def _setup_redis_for_two_polls(self):
        """
        Returns a mock Redis client whose SET NX returns:
          call 1 (poll 1, claim): True   → claimed
          call 2 (poll 2, claim): True   → claimed again (key was released)
          call 3 (poll 2, promote): ignored
        GET for promote returns a matching token so promote proceeds.
        """
        mock_redis = MagicMock()
        # SET NX: poll-1 claim succeeds, then get() matches for promote check,
        # poll-2 claim also succeeds (key was released by Lua after poll-1 failure).
        mock_redis.set.side_effect = [True, True, True]
        mock_redis.get.return_value = MagicMock()  # non-None; actual token comparison done by promote
        mock_redis.eval.return_value = 1  # Lua delete succeeded
        return mock_redis

    def test_failed_publish_retried_on_next_poll(self, patch_settings):
        mock_client, _ = patch_settings
        messages = [_make_message("111.000", "hello world")]
        mock_client.conversations_history.return_value = _conversations_history_response(messages)

        published_calls = []

        def fake_publish_query(**kwargs):
            published_calls.append(kwargs)
            # First call fails; second succeeds.
            return None if len(published_calls) == 1 else "mid_abc"

        # Single mock redis used across both polls.
        # set() always returns True — both polls claim the key (because poll-1
        # released it via eval() before poll-2 runs).
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_redis.eval.return_value = 1  # Lua delete succeeds
        # get() in _promote_claim returns something that won't match the uuid
        # token, so promote skips the inner set(); that's fine — published
        # count is incremented before promote returns.
        mock_redis.get.return_value = "__no_match__"

        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.get_redis", return_value=mock_redis), \
             patch("src.agents.cora.command_center.worker.publish_query", side_effect=fake_publish_query), \
             patch("src.agents.cora.command_center.slack_listener._get_watermark", return_value=None), \
             patch("src.agents.cora.command_center.slack_listener._save_watermark"):

            from src.agents.cora.command_center.slack_listener import poll_once

            # Poll 1: publish fails → 0 published, claim released via Lua
            n1 = poll_once("C_TEST")
            assert n1 == 0, f"expected 0 published on poll-1 (transient fail), got {n1}"

            # Lua release must have been called so next poll can reclaim
            mock_redis.eval.assert_called_once()

            # Poll 2: publish succeeds → 1 published
            n2 = poll_once("C_TEST")
            assert n2 == 1, f"expected 1 published on poll-2 (retry), got {n2}"

        assert len(published_calls) == 2, "publish_query should be called on both polls"
        assert published_calls[0]["question"] == "hello world"
        assert published_calls[1]["question"] == "hello world"

    def test_successful_publish_not_reprocessed(self, patch_settings):
        """A successfully published message must not be re-published on the next poll."""
        mock_client, _ = patch_settings
        messages = [_make_message("222.000", "first message")]
        mock_client.conversations_history.return_value = _conversations_history_response(messages)

        publish_count = [0]

        def fake_publish(**kwargs):
            publish_count[0] += 1
            return "mid_ok"

        # Poll 1: SET NX succeeds (new claim) → poll 2: SET NX returns None (already seen)
        set_nx_sequence = iter([True, None, True])  # poll1-claim, poll2-claim-fails, promote

        mock_redis = MagicMock()
        mock_redis.set.side_effect = lambda *a, **kw: next(set_nx_sequence)
        mock_redis.get.return_value = "some_token"
        mock_redis.eval.return_value = 0

        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.get_redis", return_value=mock_redis), \
             patch("src.agents.cora.command_center.worker.publish_query", side_effect=fake_publish), \
             patch("src.agents.cora.command_center.slack_listener._get_watermark", return_value=None), \
             patch("src.agents.cora.command_center.slack_listener._save_watermark"):

            from src.agents.cora.command_center.slack_listener import poll_once

            poll_once("C_TEST")  # publishes once, promotes claim
            poll_once("C_TEST")  # claim already held — should skip

        assert publish_count[0] == 1, "message must not be published twice"

    def test_bot_messages_skipped(self, patch_settings):
        """Messages from the bot's own user ID are never published."""
        mock_client, _ = patch_settings
        messages = [_make_message("333.000", "bot reply", user="U0BNFHF5STT")]
        mock_client.conversations_history.return_value = _conversations_history_response(messages)

        with patch("src.agents.cora.command_center.worker.publish_query") as mock_pub, \
             patch("src.agents.cora.command_center.slack_listener._get_watermark", return_value=None), \
             patch("src.agents.cora.command_center.slack_listener._save_watermark"), \
             patch("src.core.redis_client.redis_available", return_value=False):

            from src.agents.cora.command_center.slack_listener import poll_once
            n = poll_once("C_TEST")

        assert n == 0
        mock_pub.assert_not_called()
