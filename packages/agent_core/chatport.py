"""ChatPort: the one Slack surface. Domain code depends on the protocol, never on Slack directly."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PostResult:
    ok: bool
    channel: str | None = None
    ts: str | None = None


class ChatPort(Protocol):
    def post(self, *, text: str, blocks: list[dict] | None = None, channel: str | None = None,
             thread_ts: str | None = None) -> PostResult: ...

    def update(self, *, channel: str, ts: str, text: str, blocks: list[dict] | None = None) -> PostResult: ...

    def replies(self, *, channel: str, thread_ts: str, limit: int) -> list[dict[str, Any]]: ...

    def history(self, *, channel: str, limit: int) -> list[dict[str, Any]]: ...


@dataclass
class FakeChatPort:
    """Records every post and update in memory; sends nothing."""

    default_channel: str = "C_TEST"
    posts: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    threads: dict[tuple[str, str], list[dict[str, Any]]] = field(default_factory=dict)
    channels: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def replies(self, *, channel: str, thread_ts: str, limit: int) -> list[dict[str, Any]]:
        return list(self.threads.get((channel, thread_ts), []))[-limit:]

    def history(self, *, channel: str, limit: int) -> list[dict[str, Any]]:
        return list(self.channels.get(channel, []))[-limit:]

    def post(self, *, text: str, blocks: list[dict] | None = None, channel: str | None = None,
             thread_ts: str | None = None) -> PostResult:
        target = channel or self.default_channel
        ts = f"{len(self.posts) + 1:.6f}"
        self.posts.append({"channel": target, "ts": ts, "text": text, "blocks": blocks, "thread_ts": thread_ts})
        return PostResult(ok=True, channel=target, ts=ts)

    def update(self, *, channel: str, ts: str, text: str, blocks: list[dict] | None = None) -> PostResult:
        self.updates.append({"channel": channel, "ts": ts, "text": text, "blocks": blocks})
        return PostResult(ok=True, channel=channel, ts=ts)


class SlackChatPort:
    """Live Slack via ``slack_sdk``. Posts default to the agent's own channel."""

    def __init__(self, web_client: Any, default_channel: str) -> None:
        self._web = web_client
        self._default_channel = default_channel

    def post(self, *, text: str, blocks: list[dict] | None = None, channel: str | None = None,
             thread_ts: str | None = None) -> PostResult:
        target = channel or self._default_channel
        kwargs: dict[str, Any] = {"channel": target, "text": text, "unfurl_links": False, "unfurl_media": False}
        if blocks is not None:
            kwargs["blocks"] = blocks
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        try:
            response = self._web.chat_postMessage(**kwargs)
        except Exception as exc:
            logger.error("slack chat.postMessage to %s failed (%s)", target, type(exc).__name__)
            return PostResult(ok=False, channel=target)
        return PostResult(ok=bool(response.get("ok")), channel=target, ts=response.get("ts"))

    def update(self, *, channel: str, ts: str, text: str, blocks: list[dict] | None = None) -> PostResult:
        kwargs: dict[str, Any] = {"channel": channel, "ts": ts, "text": text}
        if blocks is not None:
            kwargs["blocks"] = blocks
        try:
            response = self._web.chat_update(**kwargs)
        except Exception as exc:
            logger.error("slack chat.update on %s failed (%s)", channel, type(exc).__name__)
            return PostResult(ok=False, channel=channel, ts=ts)
        return PostResult(ok=bool(response.get("ok")), channel=channel, ts=ts)

    def replies(self, *, channel: str, thread_ts: str, limit: int) -> list[dict[str, Any]]:
        """The thread's latest messages, oldest first. Empty on failure: the agent answers without history."""
        try:
            response = self._web.conversations_replies(channel=channel, ts=thread_ts, limit=200)
        except Exception as exc:
            logger.warning("slack conversations.replies on %s failed (%s)", channel, type(exc).__name__)
            return []
        return list(response.get("messages") or [])[-limit:]

    def history(self, *, channel: str, limit: int) -> list[dict[str, Any]]:
        """The channel's latest top-level messages, oldest first. Empty on failure."""
        try:
            response = self._web.conversations_history(channel=channel, limit=limit)
        except Exception as exc:
            logger.warning("slack conversations.history on %s failed (%s)", channel, type(exc).__name__)
            return []
        return list(reversed(response.get("messages") or []))
