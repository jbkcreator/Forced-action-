"""Watchdog alert delivery: SMS (GHL first, then Telnyx on the alerts number) and one Slack post per cycle.

A direct Telnyx send is a documented exception to the "SMS via send_sms" rule: it is an internal ops alert that must survive GHL being down.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Sequence
from zoneinfo import ZoneInfo

from config.lending_watchdog import ALERT_LABELS, ALERT_TEMPLATES, SMS_MAX_CHARS
from src.lending.slack_channels import BOT_TOKEN, report_unset
from src.lending.watchdog.rules import Alert

logger = logging.getLogger(__name__)

_EASTERN = ZoneInfo("America/New_York")


def _label(key: str) -> str:
    if key in ALERT_LABELS:
        return ALERT_LABELS[key]
    if key.startswith("unit:"):
        return f"Service {key[len('unit:'):]}"
    return key


def _render(alert: Alert, now: datetime, detail: str) -> str:
    return ALERT_TEMPLATES[alert.kind].format(
        label=_label(alert.key),
        since=alert.since.astimezone(_EASTERN).strftime("%H:%M"),
        minutes=max(0, int((now - alert.since).total_seconds() // 60)),
        detail=detail,
    )


def format_sms(alert: Alert, now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    body = _render(alert, now, alert.detail).rstrip()
    if len(body) <= SMS_MAX_CHARS:
        return body
    overflow = len(body) - SMS_MAX_CHARS
    shortened = alert.detail[: max(0, len(alert.detail) - overflow - 3)] + "..."
    return _render(alert, now, shortened).rstrip()[:SMS_MAX_CHARS]


def _ascii(text: str) -> str:
    return text.encode("ascii", "replace").decode("ascii")


def _short_label(alert: Alert) -> str:
    try:
        return _ascii(_label(alert.key))
    except Exception as exc:
        logger.error("[watchdog] could not label alert %s: %s", alert.key, type(exc).__name__)
        return _ascii(str(alert.key))


def _segment(prefix: str, labels: Sequence[str], budget: int) -> str:
    for shown in range(len(labels), 0, -1):
        rest = len(labels) - shown
        body = prefix + ", ".join(labels[:shown]) + (f" +{rest} more" if rest else "") + "."
        if len(body) <= budget:
            return body
    return (prefix + labels[0] + ".")[:budget]


def format_sms_digest(alerts: Sequence[Alert], now: Optional[datetime] = None) -> str:
    """One text for a whole cycle: a single alert keeps its full wording, several are joined by label."""
    now = now or datetime.now(timezone.utc)
    if len(alerts) == 1:
        return _safe(alerts[0], lambda: format_sms(alerts[0], now))
    down = [_short_label(a) for a in alerts if a.kind != "recovered"]
    recovered = [_short_label(a) for a in alerts if a.kind == "recovered"]
    if not down:
        return _segment("NDL watchdog recovered: ", recovered, SMS_MAX_CHARS)
    tail = _segment(" Recovered: ", recovered, 60) if recovered else ""
    return (_segment("NDL watchdog DOWN: ", down, SMS_MAX_CHARS - len(tail)) + tail)[:SMS_MAX_CHARS]


def _fallback(alert: Alert) -> str:
    return f"NDL watchdog: {alert.key} {alert.kind}"


def _safe(alert: Alert, render: Callable[[], str]) -> str:
    try:
        return render()
    except Exception as exc:
        logger.error("[watchdog] could not format alert %s: %s", alert.key, type(exc).__name__)
        return _fallback(alert)


def format_slack(alerts: Sequence[Alert], now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    return "\n".join(_safe(a, lambda a=a: _render(a, now, a.detail).rstrip()) for a in alerts)


def _mask(phone: str) -> str:
    return phone[-4:]


def send_sms_alert(
    body: str,
    recipients: Sequence[str],
    *,
    ghl: Optional[Callable[[str, str], str]],
    telnyx: Optional[Callable[[str, str], dict]],
) -> dict[str, str]:
    results: dict[str, str] = {}
    for phone in recipients:
        masked = _mask(phone)
        results[masked] = "failed"
        for route, send in (("ghl", ghl), ("telnyx", telnyx)):
            if send is None:
                continue
            try:
                send(phone, body)
            except Exception as exc:  # a duplicate beats silence, so any GHL error (even ambiguous) falls through
                logger.warning("[watchdog] %s alert text to ...%s failed: %s", route, masked, type(exc).__name__)
                continue
            results[masked] = route
            break
        if results[masked] == "failed":
            logger.error("[watchdog] alert text to ...%s could not be sent by any route", masked)
    return results


def post_slack_alert(text: str, *, slack: Any, channel: str) -> bool:
    if slack is None or not channel:
        report_unset("watchdog alert", ("LENDING_OPS_CHANNEL", BOT_TOKEN))
        return False
    try:
        slack.chat_postMessage(channel=channel, text=text)
    except Exception as exc:
        logger.error("[watchdog] Slack alert not posted: %s", type(exc).__name__)
        return False
    return True


def deliver(
    alerts: Sequence[Alert],
    *,
    recipients: Sequence[str],
    ghl: Optional[Callable[[str, str], str]],
    telnyx: Optional[Callable[[str, str], dict]],
    slack: Any,
    channel: str,
) -> bool:
    """True when nothing needed sending or at least one channel (Slack post or any SMS route) got a message out."""
    if not alerts:
        return True
    now = datetime.now(timezone.utc)
    reached = False
    try:
        reached = post_slack_alert(format_slack(alerts, now), slack=slack, channel=channel)
    except Exception as exc:
        logger.error("[watchdog] Slack alert step failed: %s", type(exc).__name__)
    try:
        results = send_sms_alert(format_sms_digest(alerts, now), recipients or (), ghl=ghl, telnyx=telnyx)
        reached = reached or any(route != "failed" for route in results.values())
    except Exception as exc:
        logger.error("[watchdog] SMS step failed: %s", type(exc).__name__)
    return reached
