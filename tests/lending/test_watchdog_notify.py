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


def test_deliver_posts_once_and_sends_one_text_per_recipient():
    slack, ghl = MagicMock(), MagicMock(return_value="id")
    notify.deliver([alert(), alert(key="cards")], recipients=[PHONE, "+15557654321"], ghl=ghl, telnyx=None, slack=slack, channel="C1")
    assert slack.chat_postMessage.call_count == 1
    assert ghl.call_count == 2
    body = ghl.call_args_list[0].args[1]
    assert "Cora" in body and "Approval cards" in body


def test_single_alert_text_keeps_full_wording():
    ghl = MagicMock(return_value="id")
    notify.deliver([alert()], recipients=[PHONE], ghl=ghl, telnyx=None, slack=None, channel="")
    body = ghl.call_args.args[1]
    assert body.startswith("NDL watchdog: Cora DOWN since ") and body.endswith(" ET. inactive")


def test_digest_joins_labels_and_counts_the_rest():
    keys = ["unit:lending-api", "health:lending-api", "door:ghl-stage", "door:ghl-disposition", "door:lendingflow",
            "door:instantly", "door:website"]
    body = notify.format_sms_digest([alert(key=k) for k in keys], now=NOW)
    assert body.startswith("NDL watchdog DOWN: Lending API, Lending API health")
    assert body.endswith("Website lead form.")
    assert len(body) <= 160 and body.isascii()


def test_digest_all_fit_lists_every_label():
    body = notify.format_sms_digest([alert(key="cards"), alert(key="replies")], now=NOW)
    assert body == "NDL watchdog DOWN: Approval cards, Reply backlog."


def test_digest_mixed_has_separate_recovered_segment():
    body = notify.format_sms_digest([alert(key="cards"), alert("recovered"), alert(key="replies", kind="still_down")], now=NOW)
    assert body == "NDL watchdog DOWN: Approval cards, Reply backlog. Recovered: Cora."


def test_digest_only_recovered():
    body = notify.format_sms_digest([alert("recovered"), alert("recovered", key="cards")], now=NOW)
    assert body == "NDL watchdog recovered: Cora, Approval cards."


def test_digest_is_ascii_and_bounded_with_many_long_labels():
    alerts = [alert(key=f"unit:fa-lending-very-long-service-name-{i}") for i in range(30)]
    alerts += [alert("recovered", key=f"unit:caf\u00e9-{i}") for i in range(10)]
    body = notify.format_sms_digest(alerts, now=NOW)
    assert len(body) <= 160 and body.isascii()
    assert "Recovered:" in body and " more" in body


def test_truncated_sms_is_ascii_and_160_chars():
    body = notify.format_sms(alert(detail="d" * 400), now=NOW)
    assert len(body) == 160 and body.isascii() and body.endswith("...")


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
    assert len(bodies) == 1
    assert "Database" in bodies[0] and "Approval cards" in bodies[0]


def test_deliver_slack_failure_does_not_stop_sms():
    slack, ghl = MagicMock(), MagicMock(return_value="id")
    slack.chat_postMessage.side_effect = RuntimeError("boom")
    notify.deliver([alert()], recipients=[PHONE], ghl=ghl, telnyx=None, slack=slack, channel="C1")
    assert ghl.call_count == 1


def test_deliver_reports_whether_any_route_succeeded():
    boom = MagicMock(side_effect=GhlSmsError("down"))
    slack_down = MagicMock()
    slack_down.chat_postMessage.side_effect = RuntimeError("x")
    kwargs = dict(recipients=[PHONE], telnyx=None, channel="C1")
    assert notify.deliver([alert()], ghl=boom, slack=slack_down, **kwargs) is False
    assert notify.deliver([alert()], ghl=MagicMock(return_value="m"), slack=slack_down, **kwargs) is True
    assert notify.deliver([alert()], ghl=boom, slack=MagicMock(), **kwargs) is True
    assert notify.deliver([], ghl=boom, slack=slack_down, **kwargs) is True
