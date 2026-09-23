"""Shared fixtures for tests/api."""
import pytest

from config.settings import get_settings


@pytest.fixture(autouse=True)
def _fa_max_slack_cc_channel_unset_by_default(monkeypatch):
    """FA_MAX_SLACK_CC_CHANNEL is a real, populated value in this worktree's
    local .env (WP-T2-6 addendum Task 21). get_settings() is @lru_cache'd,
    so it returns one process-wide singleton created at first import —
    monkeypatching the env var has no effect on it. Without this fixture,
    every api test that hits a real (unmocked) get_settings() would pick up
    that real channel ID and get rejected by _reject_if_wrong_command_channel
    / _listen_channel, even though those tests predate and are unrelated to
    the channel restriction. Patch the cached singleton's attribute directly
    so the feature's fail-open default is what unrelated tests exercise;
    tests that need the restriction path patch
    src.api.admin_router.get_settings directly.
    """
    monkeypatch.setattr(get_settings(), "fa_max_slack_cc_channel", None)
