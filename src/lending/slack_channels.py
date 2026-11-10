"""One structured error when a lending Slack post is dropped because its channel or bot token is not set (P0.3).

Since #391 every post type has its own channel setting with no fallback, so an unset setting drops that post. Each
post path calls ``report_unset`` at that point instead of its own log line: one ERROR in the same shape everywhere
(``event=lending_slack_channel_unset``, the post, the missing setting names) and, when ``SENTRY_DSN`` is set, one
Sentry event grouped per post and setting. Setting names only: never a channel ID, a token or message text.
"""
from __future__ import annotations

import time
from typing import Iterable

from config.settings import get_settings
from src.utils.logger import get_logger
from src.utils.sentry import capture_error

logger = get_logger(__name__)

EVENT = "lending_slack_channel_unset"
BOT_TOKEN = "LENDING_SLACK_BOT_TOKEN"
CORA_BOT_TOKEN = "LENDING_CORA_SLACK_BOT_TOKEN"
# Sweeps hit the same unset channel for every row they process; Sentry gets one event per post and setting per
# window (the log line still fires every time), so one misconfiguration cannot use up the Sentry quota.
SENTRY_REPEAT_SECONDS = 15 * 60
_last_sentry_event: dict[tuple[str, str], float] = {}


def missing_settings(names: Iterable[str]) -> list[str]:
    """The names among ``names`` whose setting is empty (an env name ``LENDING_X`` is the ``lending_x`` field)."""
    settings = get_settings()
    missing = []
    for name in names:
        value = getattr(settings, name.lower(), None)
        if hasattr(value, "get_secret_value"):
            value = value.get_secret_value()
        if not (value.strip() if isinstance(value, str) else value):
            missing.append(name)
    return missing


def report_unset(post: str, needed: Iterable[str]) -> list[str]:
    """Log (and send to Sentry) that ``post`` was not posted because of the unset settings among ``needed``.
    Returns the missing names; nothing is reported when none of them is actually missing."""
    missing = missing_settings(needed)
    if not missing:
        return missing
    names = ", ".join(missing)
    logger.error("[lending-slack] %s not posted: %s not set", post, names,
                 extra={"event": EVENT, "post": post, "missing_settings": missing})
    key, now = (post, names), time.monotonic()
    last = _last_sentry_event.get(key)
    if last is None or now - last >= SENTRY_REPEAT_SECONDS:
        if capture_error(f"Lending Slack post dropped: {names} not set",
                         tags={"event": EVENT, "post": post, "missing_settings": names},
                         fingerprint=[EVENT, post, names]):
            _last_sentry_event[key] = now
    return missing
