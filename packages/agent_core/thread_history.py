"""Rebuild a Slack thread as Messages API turns, so the agent remembers the conversation so far.

Slack is the record: nothing is stored here. The agent's own replies become ``assistant`` turns,
people's messages become ``user`` turns (prefixed with who said it), other bots and the agent's
interim placeholders are skipped, and only the most recent messages are kept.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

DEFAULT_HISTORY_LIMIT = 20


def _ts_value(ts: str | None) -> float:
    try:
        return float(ts or 0)
    except ValueError:
        return 0.0


def build_history(replies: Iterable[Mapping[str, Any]], *, bot_user_id: str, before_ts: str,
                  skip_texts: frozenset[str] = frozenset(), limit: int = DEFAULT_HISTORY_LIMIT) -> list[dict[str, Any]]:
    """Turns for every message in the thread older than ``before_ts`` (the message being answered)."""
    cutoff = _ts_value(before_ts)
    turns: list[dict[str, Any]] = []
    for message in sorted(replies, key=lambda item: _ts_value(item.get("ts"))):
        if _ts_value(message.get("ts")) >= cutoff:
            continue
        text_value = (message.get("text") or "").strip()
        if not text_value or message.get("subtype"):
            continue
        author = message.get("user") or ""
        if bot_user_id and author == bot_user_id:
            if text_value in skip_texts:
                continue
            turns.append({"role": "assistant", "content": text_value})
        elif message.get("bot_id"):
            continue
        elif author:
            turns.append({"role": "user", "content": f"<@{author}>: {text_value}"})
    turns = turns[-limit:]
    # The Messages API needs the conversation to open with a user turn.
    while turns and turns[0]["role"] != "user":
        turns.pop(0)
    return turns
