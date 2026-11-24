import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from src.lending.ghl_sms import GhlSmsError
from src.lending.watchdog import notify
from src.lending.watchdog.rules import Alert

NOW = datetime(2026, 11, 24, 20, 30, tzinfo=timezone.utc)
SINCE = NOW - timedelta(minutes=12)
PHONE = "+15551234567"


def alert(kind="down", key="unit:fa-lending-cora", detail="inactive"):
    return Alert(key, kind, detail, SINCE)


def test_ghl_success_skips_telnyx():
    ghl, telnyx = MagicMock(return_value="id"), MagicMock()
    assert notify.send_sms_alert("b", [PHONE], ghl=ghl, telnyx=telnyx) == {"4567": "ghl"}
    telnyx.assert_not_called()


def test_ghl_error_falls_back_to_telnyx():
    ghl, telnyx = MagicMock(side_effect=GhlSmsError("x")), MagicMock(return_value={})
    assert notify.send_sms_alert("b", [PHONE], ghl=ghl, telnyx=telnyx) == {"4567": "telnyx"}
    telnyx.assert_called_once_with(PHONE, "b")


def test_ambiguous_ghl_error_still_falls_back():
    ghl, telnyx = MagicMock(side_effect=GhlSmsError("x", ambiguous=True)), MagicMock(return_value={})
    assert notify.send_sms_alert("b", [PHONE], ghl=ghl, telnyx=telnyx) == {"4567": "telnyx"}


def test_no_ghl_uses_telnyx():
    telnyx = MagicMock(return_value={})
    assert notify.send_sms_alert("b", [PHONE], ghl=None, telnyx=telnyx) == {"4567": "telnyx"}


def test_both_unavailable_returns_failed_and_logs(caplog):
    with caplog.at_level(logging.ERROR):
        result = notify.send_sms_alert("b", [PHONE], ghl=None, telnyx=None)
    assert result == {"4567": "failed"}
    assert "4567" in caplog.text and PHONE not in caplog.text


def test_both_raise_returns_failed_without_full_phone(caplog):
    ghl, telnyx = MagicMock(side_effect=GhlSmsError("x")), MagicMock(side_effect=RuntimeError(PHONE))
    with caplog.at_level(logging.INFO):
        assert notify.send_sms_alert("b", [PHONE], ghl=ghl, telnyx=telnyx) == {"4567": "failed"}
    assert PHONE not in caplog.text


@pytest.mark.parametrize("kind", ["down", "still_down", "recovered"])
def test_sms_body_under_160(kind):
    body = notify.format_sms(alert(kind, detail="d" * 400), now=NOW)
    assert len(body) <= 160
    assert "+1" not in body


def test_sms_copy():
    assert notify.format_sms(alert(), now=NOW) == "NDL watchdog: Cora DOWN since 15:18 ET. inactive"
    assert notify.format_sms(alert("still_down"), now=NOW).startswith("NDL watchdog: Cora STILL DOWN since 15:18 ET.")
    assert notify.format_sms(alert("recovered"), now=NOW) == "NDL watchdog: Cora recovered after 12 min."


@pytest.mark.parametrize("key,label", [
    ("door:ghl-disposition", "GHL outcome webhook"),
    ("ghl_outcomes", "GHL call outcomes"),
    ("cards", "Approval cards"),
    ("unit:fa-other", "Service fa-other"),
    ("mystery", "mystery"),
])
def test_labels(key, label):
    assert label in notify.format_sms(alert(key=key), now=NOW)


def test_slack_text_lists_all_alerts():
    text = notify.format_slack([alert(), alert("recovered", key="cards")], now=NOW)
    assert "Cora" in text and "Approval cards" in text


def test_slack_unset_reports_and_returns_false(monkeypatch):
    report = MagicMock()
    monkeypatch.setattr(notify, "report_unset", report)
    assert notify.post_slack_alert("t", slack=None, channel="C1") is False
    assert notify.post_slack_alert("t", slack=MagicMock(), channel="") is False
    report.assert_called_with("watchdog alert", ("LENDING_OPS_CHANNEL", notify.BOT_TOKEN))
    assert report.call_count == 2


def test_slack_post_success_and_failure():
    slack = MagicMock()
    assert notify.post_slack_alert("t", slack=slack, channel="C1") is True
    slack.chat_postMessage.assert_called_once_with(channel="C1", text="t")
    slack.chat_postMessage.side_effect = RuntimeError("boom")
    assert notify.post_slack_alert("t", slack=slack, channel="C1") is False


def test_deliver_posts_once_and_texts_each_alert_per_recipient():
    slack, ghl = MagicMock(), MagicMock(return_value="id")
    notify.deliver([alert(), alert(key="cards")], recipients=[PHONE, "+15557654321"], ghl=ghl, telnyx=None, slack=slack, channel="C1")
    assert slack.chat_postMessage.call_count == 1
    assert ghl.call_count == 4


def test_deliver_never_raises_when_everything_throws():
    boom = MagicMock(side_effect=RuntimeError("boom"))
    slack = MagicMock()
    slack.chat_postMessage.side_effect = RuntimeError("boom")
    notify.deliver([alert()], recipients=[PHONE], ghl=boom, telnyx=boom, slack=slack, channel="C1")
    notify.deliver([alert()], recipients=None, ghl=boom, telnyx=boom, slack=slack, channel="C1")


@pytest.mark.parametrize("bad", [
    Alert("database", "bogus", "x", SINCE),
    Alert("database", "recovered", "x", SINCE.replace(tzinfo=None)),
])
def test_deliver_isolates_a_bad_alert(bad):
    slack, ghl = MagicMock(), MagicMock(return_value="id")
    notify.deliver([bad, alert(key="cards")], recipients=[PHONE], ghl=ghl, telnyx=None, slack=slack, channel="C1")
    posted = slack.chat_postMessage.call_args.kwargs["text"]
    assert "Approval cards" in posted and "database" in posted
    bodies = [c.args[1] for c in ghl.call_args_list]
    assert len(bodies) == 2
    assert "NDL watchdog: database" in bodies[0]
    assert "Approval cards" in bodies[1]


def test_deliver_slack_failure_does_not_stop_sms():
    slack, ghl = MagicMock(), MagicMock(return_value="id")
    slack.chat_postMessage.side_effect = RuntimeError("boom")
    notify.deliver([alert()], recipients=[PHONE], ghl=ghl, telnyx=None, slack=slack, channel="C1")
    assert ghl.call_count == 1
