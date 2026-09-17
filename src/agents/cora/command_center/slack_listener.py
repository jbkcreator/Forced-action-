"""
Command Center Slack listener — polls a Slack channel for new messages and
publishes each one to the cc:events Redis stream for the CC worker to process.

Pattern mirrors src/agents/cora/ingestion/reply_mailbox_poller.py:
  - Watermark (latest ts) stored in Redis — only new messages each poll
  - Bot's own messages filtered out (U0BNFHF5STT) to prevent reply loops
  - publish_query() → cc:events → CommandCenterWorker picks up and answers
  - Session ID: cc:{user_id}:{channel_id} — one conversation per user per channel

Channel to listen on is controlled by CC_SLACK_CHANNEL env var (or the
FA_MAX_SLACK_CHANNEL_RELATIONSHIPS setting). Falls back to the shared test
channel C0BLD6BG6TS when nothing is set.

Run as a daemon thread alongside the CC worker:
    from src.agents.cora.command_center.slack_listener import run_periodic
    thread = threading.Thread(target=run_periodic, args=(stop_event,), daemon=True)
    thread.start()
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import List, Optional

logger = logging.getLogger(__name__)

# The bot's own Slack user ID — messages from this user are skipped
# to prevent the bot from replying to its own answers.
_BOT_USER_ID = "U0BNFHF5STT"

_WATERMARK_KEY_PREFIX = "cc:slack:watermark:"  # + channel_id
_SEEN_KEY_PREFIX = "cc:slack:seen:"             # + message ts
_SEEN_TTL_SECONDS = 6 * 3600                    # 6 hours

DEFAULT_INTERVAL_SECONDS = 2
DEFAULT_FALLBACK_CHANNEL = "C0BLD6BG6TS"        # shared test channel


def _listen_channel() -> str:
    """Return the Slack channel ID to poll. Env var overrides settings."""
    ch = os.environ.get("CC_SLACK_CHANNEL", "").strip()
    if ch:
        return ch
    try:
        from config.settings import get_settings
        s = get_settings()
        ch = getattr(s, "fa_max_slack_channel_relationships", "") or ""
        if ch:
            return ch
    except Exception:
        pass
    return DEFAULT_FALLBACK_CHANNEL


def _get_client():
    from config.settings import get_settings
    from slack_sdk import WebClient
    s = get_settings()
    if not s.slack_bot_token:
        return None
    return WebClient(token=s.slack_bot_token.get_secret_value())


# ── Watermark ─────────────────────────────────────────────────────────────────

def _get_watermark(channel: str) -> Optional[str]:
    from src.core.redis_client import get_redis, redis_available
    if not redis_available():
        return None
    val = get_redis().get(f"{_WATERMARK_KEY_PREFIX}{channel}")
    return val if val else None


def _save_watermark(channel: str, ts: str) -> None:
    from src.core.redis_client import get_redis, redis_available
    if not redis_available():
        return
    get_redis().set(f"{_WATERMARK_KEY_PREFIX}{channel}", ts)


# ── Seen-cache ────────────────────────────────────────────────────────────────

def _already_seen(ts: str) -> bool:
    from src.core.redis_client import get_redis, redis_available
    if not redis_available():
        return False
    return bool(get_redis().exists(f"{_SEEN_KEY_PREFIX}{ts}"))


def _mark_seen(ts: str) -> None:
    from src.core.redis_client import get_redis, redis_available
    if not redis_available():
        return
    get_redis().set(f"{_SEEN_KEY_PREFIX}{ts}", "1", ex=_SEEN_TTL_SECONDS)


# ── Core poll ─────────────────────────────────────────────────────────────────

def poll_once(channel: Optional[str] = None) -> int:
    """
    Fetch new messages from the channel since the last watermark, publish each
    one to cc:events. Returns the count of queries published.
    """
    channel = channel or _listen_channel()
    client = _get_client()
    if client is None:
        logger.warning("cc.slack_listener: SLACK_BOT_TOKEN not set — skipping poll")
        return 0

    from src.agents.cora.command_center.worker import publish_query

    oldest = _get_watermark(channel)
    kwargs: dict = {"channel": channel, "limit": 50}
    if oldest:
        kwargs["oldest"] = oldest

    try:
        resp = client.conversations_history(**kwargs)
    except Exception as exc:
        logger.warning("cc.slack_listener: conversations_history failed: %s", exc)
        return 0

    messages: List[dict] = resp.get("messages", [])
    if not messages:
        return 0

    # Slack returns newest-first; reverse to process chronologically
    messages = list(reversed(messages))

    published = 0
    newest_ts = oldest

    for msg in messages:
        ts = msg.get("ts", "")
        user_id = msg.get("user", "")
        subtype = msg.get("subtype", "")
        text = (msg.get("text") or "").strip()

        # Skip bot messages, system subtypes (joins, leaves), and empty text
        if not ts or not text:
            continue
        if subtype:
            continue
        if user_id == _BOT_USER_ID:
            continue
        if _already_seen(ts):
            newest_ts = ts
            continue

        # One session per user per channel — preserves multi-turn history
        session_id = f"cc:{user_id}:{channel}"

        mid = publish_query(
            session_id=session_id,
            question=text,
            slack_user_id=user_id,
            slack_channel=channel,
            slack_thread_ts=None,  # reply in main channel, not a thread
        )

        if mid is not None:
            _mark_seen(ts)
            published += 1
            logger.info(
                "cc.slack_listener: published query session=%s user=%s ts=%s text=%r",
                session_id, user_id, ts, text[:80],
            )
        else:
            logger.warning(
                "cc.slack_listener: publish_query failed for ts=%s — will retry next poll",
                ts,
            )
            # Don't advance watermark past a failed message
            break

        newest_ts = ts

    if newest_ts and newest_ts != oldest:
        _save_watermark(channel, newest_ts)

    return published


def run_periodic(
    stop_event: threading.Event,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    channel: Optional[str] = None,
) -> None:
    channel = channel or _listen_channel()
    logger.info(
        "cc.slack_listener: starting — channel=%s poll_interval=%ds",
        channel, interval_seconds,
    )
    # Wait until Redis is reachable before first poll (tunnel may take a moment).
    from src.core.redis_client import redis_available
    while not stop_event.is_set() and not redis_available():
        stop_event.wait(1)
    if stop_event.is_set():
        return
    while not stop_event.is_set():
        try:
            n = poll_once(channel)
            if n:
                logger.info("cc.slack_listener: %d new message(s) queued", n)
        except Exception:
            logger.exception("cc.slack_listener: poll iteration failed — continuing")
        stop_event.wait(interval_seconds)
    logger.info("cc.slack_listener: stopped")
