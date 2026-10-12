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


@dataclass
class FakeChatPort:
    """Records every post and update in memory; sends nothing."""

    default_channel: str = "C_TEST"
    posts: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)

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
