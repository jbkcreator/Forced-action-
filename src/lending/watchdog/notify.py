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
    return _render(alert, now, alert.detail[: max(0, len(alert.detail) - overflow - 1)] + "…").rstrip()[:SMS_MAX_CHARS]


def format_slack(alerts: Sequence[Alert], now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    return "\n".join(_render(a, now, a.detail).rstrip() for a in alerts)


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
) -> None:
    if not alerts:
        return
    try:
        now = datetime.now(timezone.utc)
        post_slack_alert(format_slack(alerts, now), slack=slack, channel=channel)
        for alert in alerts:
            send_sms_alert(format_sms(alert, now), recipients or (), ghl=ghl, telnyx=telnyx)
    except Exception as exc:
        logger.error("[watchdog] alert delivery failed: %s", type(exc).__name__)
