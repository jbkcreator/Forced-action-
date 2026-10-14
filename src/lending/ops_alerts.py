"""Short operational alerts to the lending ops channel (``LENDING_OPS_CHANNEL``) with the lending bot.

Best effort and independent of the database, so an alert still goes out when the database is the
problem. Callers must keep phone numbers and message text out of the alert: ids, masked emails and
error classes only.
"""
from __future__ import annotations

import logging

from config.settings import get_settings

logger = logging.getLogger(__name__)


def post_ops_alert(message: str) -> bool:
    """Post ``message`` to the ops channel. False (logged) when unconfigured or Slack refuses; never raises."""
    settings = get_settings()
    channel = settings.lending_ops_channel
    if not (channel and settings.lending_slack_bot_token):
        logger.warning("[lending-ops-alert] not posted: LENDING_OPS_CHANNEL or LENDING_SLACK_BOT_TOKEN is not set")
        return False
    try:
        from slack_sdk import WebClient

        WebClient(token=settings.lending_slack_bot_token.get_secret_value()).chat_postMessage(
            channel=channel, text=message)
    except Exception as exc:  # class only: Slack errors can echo the message
        logger.error("[lending-ops-alert] Slack post failed: %s", type(exc).__name__)
        return False
    return True
