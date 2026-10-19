"""T-05: Josh is alerted (Slack @-mention, pushed to his phone by the Slack app) when a LendingFlow lead arrives,
and the uncalled-lead alarm texts to his own phone keep going during the lending emergency halt.
Fake Slack client and recorded GHL calls: nothing real is posted or sent."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.lending import global_halt, lead_arrival_alert, lendingflow_events
from src.lending.ghl_account import GhlAccount
from src.lending.ghl_sms import GhlSmsError, GhlSmsSender

LEAD = {"lead_uuid": "7f6c1a2e-0000-4000-8000-000000000001", "phone": "+17275550100", "email": "sam@example.com",
        "received_at": datetime(2026, 10, 19, 14, 0, tzinfo=timezone.utc), "credit_band_min_fico": 700,
        "loan_amount": 250000, "property_state": "FL", "loan_type": "DSCR_RENTAL"}


class FakeSlack:
    def __init__(self, fail: bool = False) -> None:
        self.posts: list[dict] = []
        self._fail = fail

    def chat_postMessage(self, **kwargs):
        if self._fail:
            raise RuntimeError("slack down")
        self.posts.append(kwargs)


def _settings(**overrides):
    values = dict(lending_lead_arrival_alert_enabled=True, lending_slack_bot_token="xoxb-test",
                  lending_ops_channel="C_OPS", lending_cora_approver_user_ids="U_JOSH")
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture
def slack(monkeypatch):
    client = FakeSlack()
    monkeypatch.setattr(lead_arrival_alert, "_client_factory", lambda: client)
    monkeypatch.setattr(lead_arrival_alert, "get_settings", _settings)
    return client


def _event():
    return lendingflow_events.build_event(LEAD)


def test_a_new_lead_mentions_josh_in_the_ops_channel(slack) -> None:
    assert lead_arrival_alert.alert_josh(_event()) is True
    post = slack.posts[0]
    assert post["channel"] == "C_OPS"
    assert "<@U_JOSH>" in post["text"] and "New LendingFlow lead: call now." in post["text"]
    assert "$250,000" in post["text"] and "FL" in post["text"] and "Phone ending 0100" in post["text"]


def test_the_alert_never_shows_the_full_phone_or_email(slack) -> None:
    lead_arrival_alert.alert_josh(_event())
    text_out = slack.posts[0]["text"]
    assert "7275550100" not in text_out and "sam@example.com" not in text_out


@pytest.mark.parametrize("override", [{"lending_lead_arrival_alert_enabled": False}, {"lending_ops_channel": ""},
                                      {"lending_slack_bot_token": None}, {"lending_cora_approver_user_ids": ""}])
def test_nothing_is_posted_until_configured(monkeypatch, slack, override) -> None:
    monkeypatch.setattr(lead_arrival_alert, "get_settings", lambda: _settings(**override))
    assert lead_arrival_alert.alert_josh(_event()) is None
    assert slack.posts == []


def test_a_slack_failure_is_logged_and_never_raised(monkeypatch) -> None:
    monkeypatch.setattr(lead_arrival_alert, "_client_factory", lambda: FakeSlack(fail=True))
    monkeypatch.setattr(lead_arrival_alert, "get_settings", _settings)
    assert lead_arrival_alert.alert_josh(_event()) is False


def test_the_alert_is_wired_to_the_lead_created_event_in_both_processes() -> None:
    import src.lending.api  # noqa: F401  lending-api
    import src.tasks.lending_lendingflow_sweep  # noqa: F401  the GHL retry sweep (cron)

    assert lendingflow_events._handlers.count(lead_arrival_alert.alert_josh) == 1


class Resp:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code, self._body = status, body

    def json(self) -> dict:
        return self._body


def _halted(monkeypatch) -> None:
    monkeypatch.setattr(global_halt, "is_globally_halted", lambda: True)
    monkeypatch.setattr("src.lending.ghl_sms.is_globally_halted", lambda: True)


def _request(calls):
    responses = [Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"})]

    def request(method, url, **kwargs):
        calls.append(url)
        return responses.pop(0)

    return request


def test_alarm_texts_to_josh_keep_going_during_the_emergency_halt(monkeypatch) -> None:
    _halted(monkeypatch)
    calls: list[str] = []
    sender = GhlSmsSender(GhlAccount(api_key="k", location_id="loc"), "+18135550000", _request(calls),
                          internal_alert=True)
    assert sender("+18135559999", "URGENT: lead not called") == "m1"
    assert any("/conversations/messages" in url for url in calls)


def test_borrower_texts_are_still_refused_during_the_emergency_halt(monkeypatch) -> None:
    _halted(monkeypatch)
    calls: list[str] = []
    with pytest.raises(GhlSmsError, match="emergency halt"):
        GhlSmsSender(GhlAccount(api_key="k", location_id="loc"), "+18135550000", _request(calls))("+17275550100", "Hi")
    assert not any("/conversations/messages" in url for url in calls)


def test_only_the_alarm_worker_uses_the_internal_alert_sender() -> None:
    import pathlib

    users = [path.as_posix() for path in pathlib.Path("src").rglob("*.py")
             if "get_sender(internal_alert=True)" in path.read_text(encoding="utf-8")]
    assert users == ["src/lending/uncalled_alarm_worker.py"]


def test_the_5_minute_alarm_post_mentions_josh() -> None:
    from src.lending.uncalled_alarm_worker import MentioningSlack

    inner = FakeSlack()
    MentioningSlack(inner, frozenset({"U_JOSH"})).chat_postMessage(channel="C_OPS", text="URGENT: lead not called")
    assert inner.posts == [{"channel": "C_OPS", "text": "<@U_JOSH> URGENT: lead not called"}]


def test_no_approvers_means_the_alarm_post_is_unchanged() -> None:
    from src.lending.uncalled_alarm_worker import MentioningSlack

    inner = FakeSlack()
    MentioningSlack(inner, frozenset()).chat_postMessage(channel="C_OPS", text="URGENT")
    assert inner.posts[0]["text"] == "URGENT"
