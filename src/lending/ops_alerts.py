"""Short operational alerts with the lending bot: lead alerts to ``LENDING_OPS_CHANNEL`` (#lead-alerts) and
housekeeping notices (opt-out mailbox, webhook failures) to ``LENDING_OPS_NOTICE_CHANNEL``.

Best effort and independent of the database, so an alert still goes out when the database is the
problem. Callers must keep phone numbers and message text out of the alert: ids, masked emails and
error classes only.
"""
from __future__ import annotations

import logging

from config.settings import get_settings

logger = logging.getLogger(__name__)


def post_ops_alert(message: str) -> bool:
    """Post ``message`` to #lead-alerts. False (logged) when unconfigured or Slack refuses; never raises."""
    return _post(get_settings().lending_ops_channel, "LENDING_OPS_CHANNEL", message)


def post_ops_notice(message: str) -> bool:
    """Post ``message`` to the ops notice channel. False (logged) when unconfigured or Slack refuses; never raises."""
    return _post(get_settings().lending_ops_notice_channel, "LENDING_OPS_NOTICE_CHANNEL", message)


def _post(channel: str, setting: str, message: str) -> bool:
    settings = get_settings()
    if not (channel and settings.lending_slack_bot_token):
        logger.warning("[lending-ops-alert] not posted: %s or LENDING_SLACK_BOT_TOKEN is not set", setting)
        return False
    try:
        from slack_sdk import WebClient

        WebClient(token=settings.lending_slack_bot_token.get_secret_value()).chat_postMessage(
            channel=channel, text=message)
    except Exception as exc:  # class only: Slack errors can echo the message
        logger.error("[lending-ops-alert] Slack post failed: %s", type(exc).__name__)
        return False
    return True
