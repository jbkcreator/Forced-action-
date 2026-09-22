"""Resolve the Command Center bot's own Slack user ID.

Every inbound listener must discard messages the bot itself posted. Without
that filter each answer is re-ingested as a new question and the bot holds a
conversation with itself, consuming API credit on every lap. A hardcoded ID
goes stale the moment the Slack app is recreated and fails open, which is how
that loop started, so the ID is read from Slack at startup and only falls back
to configuration.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _configured_bot_user_id() -> Optional[str]:
    try:
        from config.settings import get_settings
        return (get_settings().fa_max_slack_bot_user_id or "").strip() or None
    except Exception:
        logger.debug("cc.bot_identity: settings lookup failed", exc_info=True)
        return None


def resolve_bot_user_id(web_client) -> Optional[str]:
    """
    Ask Slack which user this bot token belongs to, falling back to config.

    Returns None when neither source yields an ID. Callers must treat that as
    fatal rather than listening unfiltered.
    """
    try:
        user_id = web_client.auth_test()["user_id"]
        logger.info("cc.bot_identity: resolved bot user_id from auth.test: %s", user_id)
        return user_id
    except Exception as exc:
        logger.warning(
            "cc.bot_identity: auth.test failed (%s) — falling back to configured ID", exc
        )

    configured = _configured_bot_user_id()
    if configured:
        logger.info("cc.bot_identity: using configured bot user_id: %s", configured)
        return configured

    logger.error(
        "cc.bot_identity: bot user ID unresolved — auth.test failed and "
        "FA_MAX_SLACK_BOT_USER_ID is unset"
    )
    return None
