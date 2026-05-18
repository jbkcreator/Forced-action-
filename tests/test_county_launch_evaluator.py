"""Unit tests for county_launch_evaluator."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def fake_redis(monkeypatch):
    """Patch rget/redis_available to return controllable values."""
    store = {}

    def _rget(key):
        return store.get(key)

    monkeypatch.setattr("src.tasks.county_launch_evaluator.rget", _rget)
    monkeypatch.setattr("src.tasks.county_launch_evaluator.redis_available", lambda: True)
    return store


def _all_green_redis(store, county_id="hillsborough"):
    """Populate Redis store with all-green values."""
    store[f"fa:ks_metric:{county_id}:first_payment_rate"] = "35.0"
    store[f"fa:ks_metric:{county_id}:saved_card_rate"] = "75.0"
    store[f"fa:ks_metric:{county_id}:wallet_adoption"] = "20.0"
    store[f"fa:ks_metric:{county_id}:lock_conversion"] = "7.0"
    store[f"fa:ks_metric:{county_id}:retention_30d"] = "75.0"
    store[f"fa:ks_metric:{county_id}:free_tier_cost_ratio"] = "35.0"
    store[f"fa:ks_metric:{county_id}:county_profitability"] = "1.0"


@pytest.fixture
def mock_db_context(monkeypatch):
    """Patch get_db_context to return a mock session."""
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)

    from contextlib import contextmanager

    @contextmanager
    def _mock_ctx():
        yield session

    monkeypatch.setattr("src.tasks.county_launch_evaluator.get_db_context", _mock_ctx)
    return session


def _make_candidate(id=1, county_id="pinellas", priority=100,
                    status="queued", last_slack_posted_at=None, last_slack_message_ts=None):
    c = MagicMock()
    c.id = id
    c.county_id = county_id
    c.priority = priority
    c.status = status
    c.last_slack_posted_at = last_slack_posted_at
    c.last_slack_message_ts = last_slack_message_ts
    return c


def _setup_candidate(mock_db, candidate):
    result = MagicMock()
    result.scalar_one_or_none.return_value = candidate
    mock_db.execute.return_value = result


class TestGateColorLogic:
    def test_profitability_green(self):
        from src.tasks.county_launch_evaluator import _gate_color
        assert _gate_color("county_profitability", 1.0) == "green"

    def test_profitability_red(self):
        from src.tasks.county_launch_evaluator import _gate_color
        assert _gate_color("county_profitability", 0.0) == "red"

    def test_profitability_none(self):
        from src.tasks.county_launch_evaluator import _gate_color
        assert _gate_color("county_profitability", None) == "red"

    def test_free_tier_cost_ratio_green_low(self):
        from src.tasks.county_launch_evaluator import _gate_color
        assert _gate_color("free_tier_cost_ratio", 30.0) == "green"

    def test_free_tier_cost_ratio_red_high(self):
        from src.tasks.county_launch_evaluator import _gate_color
        assert _gate_color("free_tier_cost_ratio", 55.0) == "red"

    def test_first_payment_rate_green(self):
        from src.tasks.county_launch_evaluator import _gate_color
        assert _gate_color("first_payment_rate", 35.0) == "green"

    def test_first_payment_rate_red(self):
        from src.tasks.county_launch_evaluator import _gate_color
        assert _gate_color("first_payment_rate", 15.0) == "red"


class TestEvaluator:
    def test_all_green_posts_slack(self, fake_redis, mock_db_context, monkeypatch):
        _all_green_redis(fake_redis)
        candidate = _make_candidate()
        _setup_candidate(mock_db_context, candidate)

        slack_calls = []
        mock_slack = MagicMock()
        mock_slack.chat_postMessage.return_value = {"ts": "1234.5678"}
        monkeypatch.setattr(
            "src.tasks.county_launch_evaluator.WebClient",
            lambda token: mock_slack,
        )
        monkeypatch.setattr(
            "config.settings.settings.slack_bot_token",
            MagicMock(get_secret_value=lambda: "xoxb-test"),
        )
        monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

        from src.tasks.county_launch_evaluator import run_county_launch_evaluator
        result = run_county_launch_evaluator(dry_run=False)

        assert result.get("all_green") is True
        assert result.get("posted") is True
        mock_slack.chat_postMessage.assert_called_once()

    def test_one_gate_yellow_no_post(self, fake_redis, mock_db_context, monkeypatch):
        _all_green_redis(fake_redis)
        fake_redis["fa:ks_metric:hillsborough:first_payment_rate"] = "25.0"  # yellow (20-30)

        monkeypatch.setattr("config.settings.settings.slack_bot_token",
                            MagicMock(get_secret_value=lambda: "xoxb-test"))
        monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

        from src.tasks.county_launch_evaluator import run_county_launch_evaluator
        result = run_county_launch_evaluator(dry_run=False)

        assert result.get("all_green") is False
        assert "first_payment_rate" in result.get("non_green", [])

    def test_one_gate_red_no_post(self, fake_redis, mock_db_context, monkeypatch):
        _all_green_redis(fake_redis)
        fake_redis["fa:ks_metric:hillsborough:retention_30d"] = "50.0"  # red (<55)

        monkeypatch.setattr("config.settings.settings.slack_bot_token",
                            MagicMock(get_secret_value=lambda: "xoxb-test"))
        monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

        from src.tasks.county_launch_evaluator import run_county_launch_evaluator
        result = run_county_launch_evaluator(dry_run=False)

        assert result.get("all_green") is False

    def test_all_green_no_candidate(self, fake_redis, mock_db_context, monkeypatch):
        _all_green_redis(fake_redis)
        _setup_candidate(mock_db_context, None)

        monkeypatch.setattr("config.settings.settings.slack_bot_token",
                            MagicMock(get_secret_value=lambda: "xoxb-test"))
        monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

        from src.tasks.county_launch_evaluator import run_county_launch_evaluator
        result = run_county_launch_evaluator(dry_run=False)

        assert result.get("all_green") is True
        assert result.get("candidate") is None

    def test_cooldown_active_skips(self, fake_redis, mock_db_context, monkeypatch):
        _all_green_redis(fake_redis)
        recent = datetime.now(timezone.utc) - timedelta(hours=1)
        candidate = _make_candidate(last_slack_posted_at=recent)
        _setup_candidate(mock_db_context, candidate)

        monkeypatch.setattr("config.settings.settings.slack_bot_token",
                            MagicMock(get_secret_value=lambda: "xoxb-test"))
        monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

        from src.tasks.county_launch_evaluator import run_county_launch_evaluator
        result = run_county_launch_evaluator(dry_run=False)

        assert result.get("cooldown_skipped") is True

    def test_reminder_after_7_days(self, fake_redis, mock_db_context, monkeypatch):
        _all_green_redis(fake_redis)
        old_posted = datetime.now(timezone.utc) - timedelta(days=8)
        candidate = _make_candidate(last_slack_posted_at=old_posted, last_slack_message_ts="old.ts")
        _setup_candidate(mock_db_context, candidate)

        mock_slack = MagicMock()
        mock_slack.chat_postMessage.return_value = {"ts": "new.ts"}
        monkeypatch.setattr("src.tasks.county_launch_evaluator.WebClient",
                            lambda token: mock_slack)
        monkeypatch.setattr("config.settings.settings.slack_bot_token",
                            MagicMock(get_secret_value=lambda: "xoxb-test"))
        monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

        from src.tasks.county_launch_evaluator import run_county_launch_evaluator
        result = run_county_launch_evaluator(dry_run=False)

        assert result.get("posted") is True
        mock_slack.chat_postMessage.assert_called_once()

    def test_missing_redis_key_treated_as_non_green(self, fake_redis, mock_db_context, monkeypatch):
        # Only set 6 of 7 gates — county_profitability missing
        fake_redis["fa:ks_metric:hillsborough:first_payment_rate"] = "35.0"
        fake_redis["fa:ks_metric:hillsborough:saved_card_rate"] = "75.0"
        fake_redis["fa:ks_metric:hillsborough:wallet_adoption"] = "20.0"
        fake_redis["fa:ks_metric:hillsborough:lock_conversion"] = "7.0"
        fake_redis["fa:ks_metric:hillsborough:retention_30d"] = "75.0"
        fake_redis["fa:ks_metric:hillsborough:free_tier_cost_ratio"] = "35.0"
        # county_profitability NOT set

        monkeypatch.setattr("config.settings.settings.slack_bot_token",
                            MagicMock(get_secret_value=lambda: "xoxb-test"))
        monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

        from src.tasks.county_launch_evaluator import run_county_launch_evaluator
        result = run_county_launch_evaluator(dry_run=False)

        assert result.get("all_green") is False

    def test_slack_not_configured_exits_cleanly(self, monkeypatch):
        monkeypatch.setattr("config.settings.settings.slack_bot_token", None)
        monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "")

        from src.tasks.county_launch_evaluator import run_county_launch_evaluator
        result = run_county_launch_evaluator(dry_run=False)

        assert result.get("skipped") is True
