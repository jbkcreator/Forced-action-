"""P0.3: a lending Slack post dropped because its channel or bot token is unset logs one structured ERROR and, with
SENTRY_DSN set, one Sentry event. Never a channel id, token or message text. Slack and Sentry are faked; no database."""
from __future__ import annotations

import logging
import sys
from types import ModuleType, SimpleNamespace

import pytest
from pydantic import SecretStr

from src.lending import lead_arrival_alert, ops_alerts, slack_channels, uncalled_alarm_worker
from src.utils import sentry

TOKEN = "xoxb-secret-token"


@pytest.fixture(autouse=True)
def _no_sentry_state():
    sentry.reset_for_tests()
    yield
    sentry.reset_for_tests()


def _settings(monkeypatch, *modules, **values):
    base = dict(lending_slack_bot_token=SecretStr(TOKEN), lending_cora_slack_bot_token=SecretStr("xoxb-cora"),
                lending_ops_channel="", lending_ops_notice_channel="", lending_daily_channel="",
                lending_cora_approver_user_ids="U1", lending_lead_arrival_alert_enabled=True,
                sentry_dsn=None, sentry_environment="test")
    base.update(values)
    fake = SimpleNamespace(**base)
    for module in (slack_channels, sentry, *modules):
        monkeypatch.setattr(module, "get_settings", lambda: fake)
    return fake


class _FakeSentry(ModuleType):
    """Stands in for sentry_sdk: records init kwargs and captured events."""

    def __init__(self):
        super().__init__("sentry_sdk")
        self.init_kwargs, self.events = None, []

    def init(self, **kwargs):
        self.init_kwargs = kwargs

    def new_scope(self):
        fake = self

        class _Scope:
            def __init__(self):
                self.tags, self.fingerprint = {}, None

            def set_tag(self, key, value):
                self.tags[key] = value

            def __enter__(self):
                fake._scope = self
                return self

            def __exit__(self, *exc):
                return False

        return _Scope()

    def capture_message(self, message, level):
        self.events.append({"message": message, "level": level, "tags": dict(self._scope.tags),
                            "fingerprint": self._scope.fingerprint})


@pytest.fixture
def fake_sentry(monkeypatch):
    module = _FakeSentry()
    monkeypatch.setitem(sys.modules, "sentry_sdk", module)
    return module


# --- the reporter --------------------------------------------------------------------------------------------

def test_an_unset_channel_logs_one_structured_error_naming_the_setting(monkeypatch, caplog):
    _settings(monkeypatch)
    with caplog.at_level(logging.ERROR, logger="src.lending.slack_channels"):
        missing = slack_channels.report_unset("scoreboard", ("LENDING_DAILY_CHANNEL", slack_channels.BOT_TOKEN))
    assert missing == ["LENDING_DAILY_CHANNEL"]
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert record.event == slack_channels.EVENT and record.post == "scoreboard"
    assert record.missing_settings == ["LENDING_DAILY_CHANNEL"]
    assert "scoreboard not posted: LENDING_DAILY_CHANNEL not set" in record.getMessage()
    assert TOKEN not in caplog.text


def test_a_missing_token_is_named_too(monkeypatch):
    _settings(monkeypatch, lending_slack_bot_token=None)
    assert slack_channels.report_unset("ops alert", ("LENDING_OPS_CHANNEL", slack_channels.BOT_TOKEN)) == [
        "LENDING_OPS_CHANNEL", "LENDING_SLACK_BOT_TOKEN"]


def test_nothing_is_reported_when_every_setting_is_present(monkeypatch, caplog):
    _settings(monkeypatch, lending_daily_channel="C_DAILY")
    with caplog.at_level(logging.ERROR):
        assert slack_channels.report_unset("scoreboard", ("LENDING_DAILY_CHANNEL", slack_channels.BOT_TOKEN)) == []
    assert caplog.records == []


def test_a_whitespace_only_channel_counts_as_unset(monkeypatch):
    _settings(monkeypatch, lending_daily_channel="   ")
    assert slack_channels.missing_settings(["LENDING_DAILY_CHANNEL"]) == ["LENDING_DAILY_CHANNEL"]


# --- Sentry --------------------------------------------------------------------------------------------------

def test_without_a_dsn_nothing_goes_to_sentry(monkeypatch, fake_sentry):
    _settings(monkeypatch)
    slack_channels.report_unset("scoreboard", ("LENDING_DAILY_CHANNEL",))
    assert fake_sentry.init_kwargs is None and fake_sentry.events == []


def test_with_a_dsn_the_unset_channel_is_one_grouped_sentry_error(monkeypatch, fake_sentry):
    _settings(monkeypatch, sentry_dsn=SecretStr("https://key@sentry.example/1"))
    slack_channels.report_unset("scoreboard", ("LENDING_DAILY_CHANNEL", slack_channels.BOT_TOKEN))
    slack_channels.report_unset("scoreboard", ("LENDING_DAILY_CHANNEL", slack_channels.BOT_TOKEN))
    assert len(fake_sentry.events) == 2
    event = fake_sentry.events[0]
    assert event["level"] == "error"
    assert event["message"] == "Lending Slack post dropped: LENDING_DAILY_CHANNEL not set"
    assert event["tags"] == {"event": slack_channels.EVENT, "post": "scoreboard",
                             "missing_settings": "LENDING_DAILY_CHANNEL"}
    assert event["fingerprint"] == [slack_channels.EVENT, "scoreboard", "LENDING_DAILY_CHANNEL"]
    assert fake_sentry.events[1]["fingerprint"] == event["fingerprint"]  # repeats group into one issue


def test_sentry_starts_once_without_its_automatic_capture_or_personal_data(monkeypatch, fake_sentry):
    _settings(monkeypatch, sentry_dsn=SecretStr("https://key@sentry.example/1"))
    sentry.capture_error("x", tags={}, fingerprint=["x"])
    first = fake_sentry.init_kwargs
    fake_sentry.init_kwargs = None
    sentry.capture_error("y", tags={}, fingerprint=["y"])
    assert fake_sentry.init_kwargs is None  # initialised once per process
    assert first["default_integrations"] is False and first["send_default_pii"] is False
    assert first["environment"] == "test"


def test_a_broken_sentry_never_breaks_the_caller(monkeypatch, fake_sentry):
    _settings(monkeypatch, sentry_dsn=SecretStr("https://key@sentry.example/1"))

    def boom(message, level):
        raise RuntimeError("network down")

    monkeypatch.setattr(fake_sentry, "capture_message", boom)
    assert slack_channels.report_unset("scoreboard", ("LENDING_DAILY_CHANNEL",)) == ["LENDING_DAILY_CHANNEL"]


# --- post paths that used to skip without an error -----------------------------------------------------------

def test_ops_alert_with_no_channel_posts_nothing_and_reports(monkeypatch, caplog):
    _settings(monkeypatch, ops_alerts)
    with caplog.at_level(logging.ERROR, logger="src.lending.slack_channels"):
        assert ops_alerts.post_ops_alert("call now") is False
    assert [r.post for r in caplog.records] == ["ops alert"]


def test_lead_arrival_alert_with_no_channel_reports_instead_of_returning_silently(monkeypatch, caplog):
    _settings(monkeypatch, lead_arrival_alert)
    with caplog.at_level(logging.ERROR, logger="src.lending.slack_channels"):
        assert lead_arrival_alert.alert_josh(SimpleNamespace(lead_id=1)) is None
    [record] = caplog.records
    assert record.post == "lead arrival alert" and record.missing_settings == ["LENDING_OPS_CHANNEL"]


def test_a_switched_off_lead_arrival_alert_is_not_an_error(monkeypatch, caplog):
    _settings(monkeypatch, lead_arrival_alert, lending_lead_arrival_alert_enabled=False)
    with caplog.at_level(logging.ERROR):
        assert lead_arrival_alert.alert_josh(SimpleNamespace(lead_id=1)) is None
    assert caplog.records == []


def test_the_uncalled_alarm_reports_an_unset_channel(monkeypatch, caplog):
    _settings(monkeypatch)
    with caplog.at_level(logging.ERROR, logger="src.lending.slack_channels"):
        status, _ = uncalled_alarm_worker._post_slack(object(), "", "5 minutes, no call")
    assert status == uncalled_alarm_worker.NOT_CONFIGURED
    assert [r.post for r in caplog.records] == ["uncalled-lead alarm"]


def test_the_scoreboard_task_reports_an_unset_channel(monkeypatch, caplog):
    from src.tasks import lending_daily_scoreboard

    _settings(monkeypatch, lending_daily_scoreboard)
    with caplog.at_level(logging.ERROR, logger="src.lending.slack_channels"):
        assert lending_daily_scoreboard.main(["--force"]) == 1
    assert [r.post for r in caplog.records] == ["scoreboard"]
