"""Each lending Slack post goes to its own channel setting, and an unset setting posts nothing (no fallback to an
old channel: the old workspace is being deleted). Slack is faked; no database."""
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from src.lending import disposition_delivery, email_opt_out, ops_alerts, webhook_failures


class _Slack:
    posts: list = []

    def __init__(self, token=None):
        pass

    def chat_postMessage(self, **kw):
        _Slack.posts.append(kw)
        return {"ts": "1.0"}


@pytest.fixture
def slack(monkeypatch):
    _Slack.posts = []
    monkeypatch.setattr("slack_sdk.WebClient", _Slack)
    return _Slack.posts


def _settings(monkeypatch, module, **channels):
    values = dict(lending_slack_bot_token=SecretStr("xoxb-t"), lending_ops_channel="", lending_ops_notice_channel="",
                  lending_dnc_alert_channel="", lending_dial_tasks_channel="C_DIAL")
    values.update(channels)
    monkeypatch.setattr(module, "get_settings", lambda: SimpleNamespace(**values))


def test_lead_alerts_and_notices_use_their_own_channels(monkeypatch, slack):
    _settings(monkeypatch, ops_alerts, lending_ops_channel="C_LEAD", lending_ops_notice_channel="C_NOTICE")
    assert ops_alerts.post_ops_alert("call now") and ops_alerts.post_ops_notice("webhook failed")
    assert [(p["channel"], p["text"]) for p in slack] == [("C_LEAD", "call now"), ("C_NOTICE", "webhook failed")]


def test_unset_notice_channel_posts_nothing_even_when_lead_alerts_is_set(monkeypatch, slack):
    _settings(monkeypatch, ops_alerts, lending_ops_channel="C_LEAD")
    assert ops_alerts.post_ops_notice("webhook failed") is False and slack == []


def test_opt_out_mailbox_and_webhook_failures_alert_the_notice_channel():
    for module in (email_opt_out, webhook_failures):
        assert module.post_ops_notice is ops_alerts.post_ops_notice and not hasattr(module, "post_ops_alert")


@pytest.mark.parametrize("alert", [disposition_delivery.alert_unpropagated_dnc, disposition_delivery.alert_dnc_removal_pending])
def test_dnc_alerts_go_to_the_dnc_channel_and_inbox_key(monkeypatch, slack, alert):
    inbox = []
    monkeypatch.setattr(disposition_delivery, "add_to_inbox_in_own_session", lambda **kw: inbox.append(kw))
    _settings(monkeypatch, disposition_delivery, lending_dnc_alert_channel="C_DNC")
    alert("call-1", "seat-2")
    assert [p["channel"] for p in slack] == ["C_DNC"]
    assert inbox[0]["target_channel"] == "dnc" and inbox[0]["channel_posted"] is True


@pytest.mark.parametrize("alert", [disposition_delivery.alert_unpropagated_dnc, disposition_delivery.alert_dnc_removal_pending])
def test_unset_dnc_channel_posts_nothing_but_still_raises_it_in_coras_inbox(monkeypatch, slack, alert):
    inbox = []
    monkeypatch.setattr(disposition_delivery, "add_to_inbox_in_own_session", lambda **kw: inbox.append(kw))
    _settings(monkeypatch, disposition_delivery)  # dial-tasks is set: it must not be used for DNC
    alert("call-1", "seat-2")
    assert slack == [] and inbox[0]["channel_posted"] is False
