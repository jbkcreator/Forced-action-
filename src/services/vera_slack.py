"""
Slack output for Vera's daily standing jobs.

Posts to the channel configured by VERA_SLACK_CHANNEL (client-supplied:
#vera-verification, ID C0BMLTUTQTA) using the same WebClient pattern as
lifecycle_slack.py. Falls back to send_alert email when Slack is unconfigured
— an alert always goes somewhere.

Usage:
    from src.services.vera_slack import post_vera_report
    post_vera_report(subject="Vera — Live State", body="...", blocks=[...])
"""

from __future__ import annotations

import logging
from typing import Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)


def _default_blocks(subject: str, body: str) -> list:
    """Minimal block-kit layout: header + body text + Vera sign-off.

    Per Vera's constitution: numbers first, then evidence, then unresolved.
    Callers may pass richer blocks; this is the safe fallback.
    """
    # Slack block text is capped at 3000 chars per section.
    body_truncated = body[:2900] + "\n…(truncated)" if len(body) > 2900 else body
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": subject[:150]},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": body_truncated},
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "— Vera."}],
        },
    ]


def post_vera_report(
    subject: str,
    body: str,
    blocks: Optional[list] = None,
    fallback_to_email: bool = True,
) -> Optional[str]:
    """Post a Vera report to #vera-verification.

    Returns the Slack message timestamp on success, "email" when the email
    fallback fired, or None when nothing could be sent. Never raises.

    Pass fallback_to_email=False when the caller has already emailed all
    recipients — prevents a duplicate alert email when Slack is unconfigured.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.vera_slack_channel

    if blocks is None:
        blocks = _default_blocks(subject, body)

    if token and channel:
        try:
            from slack_sdk import WebClient
            from slack_sdk.errors import SlackApiError
        except ImportError:
            logger.warning("[vera_slack] slack_sdk not installed — falling back to email")
        else:
            try:
                client = WebClient(token=token.get_secret_value())
                resp = client.chat_postMessage(
                    channel=channel,
                    text=subject,  # fallback plain text for notifications
                    blocks=blocks,
                )
                return resp.get("ts")
            except SlackApiError as exc:
                logger.warning(
                    "[vera_slack] Slack post failed (%s) — falling back to email",
                    exc.response.get("error") if exc.response else exc,
                )
            except Exception:
                logger.warning("[vera_slack] Slack post raised — falling back to email", exc_info=True)

    if not fallback_to_email:
        logger.debug("[vera_slack] Slack unavailable and fallback_to_email=False — skipping alert email")
        return None

    # Email fallback — same path as heartbeat_monitor / lifecycle_slack.
    try:
        from src.services.email import send_alert
        ok = send_alert(subject=subject[:80], body=body)
        if not ok:
            logger.warning("[vera_slack] email fallback returned falsy — no notification sent")
            return None
        return "email"
    except Exception:
        logger.warning("[vera_slack] email fallback raised — no notification sent", exc_info=True)
        return None
