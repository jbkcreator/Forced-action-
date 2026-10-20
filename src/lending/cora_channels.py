"""The Slack channels Cora's own bot has been invited to: listing, reading and posting.

Inviting Cora's bot to a channel is how a channel is added; there is no channel list in settings. The list
comes from Slack (``users.conversations``, public and private) and is kept for ``CACHE_SECONDS``; when Slack
fails, the last list is used. A channel is found by its name (``#replies`` or ``replies``), its id, or an
inbox channel key (``replies``, ``ops``, ...) whose id comes from the lending settings.

Needs the bot scopes ``channels:read``, ``groups:read``, ``channels:history``, ``groups:history`` and
``chat:write``. Messages are read top level only (no thread replies), bot posts included: most of what the
lending channels hold is alerts posted by the jobs.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

CACHE_SECONDS = 10 * 60
MAX_READ_MESSAGES = 50
MAX_READ_HOURS = 24
MESSAGE_TEXT_LIMIT = 500
_PAGE_SIZE = 200


class ChannelUnavailable(Exception):
    """Slack could not list the channels and there is no earlier list to fall back on."""


@dataclass(frozen=True)
class SlackChannel:
    id: str
    name: str
    purpose: str
    is_private: bool


class ChannelDirectory:
    def __init__(self, client: Any, *, own_channel_id: Optional[str], key_ids: Optional[Mapping[str, str]] = None,
                 cache_seconds: float = CACHE_SECONDS, clock: Callable[[], float] = time.monotonic) -> None:
        """``client`` is a slack_sdk WebClient for Cora's bot; ``key_ids`` maps inbox channel keys to ids."""
        self._client = client
        self._own_channel_id = own_channel_id
        self._key_ids = dict(key_ids or {})
        self._cache_seconds = cache_seconds
        self._clock = clock
        self._channels: Optional[list[SlackChannel]] = None
        self._fetched_at = 0.0

    @property
    def own_channel_id(self) -> Optional[str]:
        return self._own_channel_id

    def key_id(self, key: Optional[str]) -> Optional[str]:
        return self._key_ids.get(key) if key else None

    def _fetch(self) -> list[SlackChannel]:
        channels: list[SlackChannel] = []
        cursor: Optional[str] = None
        while True:
            response = self._client.users_conversations(types="public_channel,private_channel",
                                                        exclude_archived=True, limit=_PAGE_SIZE, cursor=cursor)
            for item in response.get("channels") or []:
                channels.append(SlackChannel(id=item["id"], name=item.get("name") or item["id"],
                                             purpose=((item.get("purpose") or {}).get("value") or "").strip(),
                                             is_private=bool(item.get("is_private"))))
            cursor = (response.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                return sorted(channels, key=lambda channel: channel.name)

    def channels(self) -> list[SlackChannel]:
        """Every channel Cora's bot is in, at most ``cache_seconds`` old."""
        now = self._clock()
        if self._channels is not None and now - self._fetched_at < self._cache_seconds:
            return self._channels
        try:
            self._channels, self._fetched_at = self._fetch(), now
        except Exception as error:  # class only: Slack errors can echo request detail
            if self._channels is None:
                raise ChannelUnavailable(type(error).__name__) from error
            logger.warning("[cora-channels] listing failed (%s); using the list from earlier", type(error).__name__)
        return self._channels

    def resolve(self, reference: str) -> Optional[SlackChannel]:
        """A channel Cora's bot is in, by name, id or inbox channel key. None when it is not one of them."""
        wanted = reference.strip().lstrip("#").lower()
        if not wanted:
            return None
        by_id = {channel.id.lower(): channel for channel in self.channels()}
        key_id = self._key_ids.get(wanted)
        if key_id and key_id.lower() in by_id:
            return by_id[key_id.lower()]
        if wanted in by_id:
            return by_id[wanted]
        return next((channel for channel in self.channels() if channel.name.lower() == wanted), None)

    def read(self, channel: SlackChannel, *, hours: int = MAX_READ_HOURS,
             limit: int = MAX_READ_MESSAGES) -> list[dict[str, Any]]:
        """Top-level messages from the last ``hours``, oldest first: ts, author and text (trimmed)."""
        hours, limit = min(max(hours, 1), MAX_READ_HOURS), min(max(limit, 1), MAX_READ_MESSAGES)
        oldest = time.time() - hours * 3600
        response = self._client.conversations_history(channel=channel.id, oldest=f"{oldest:.6f}", limit=limit)
        messages = []
        for item in reversed(response.get("messages") or []):
            if item.get("subtype") in ("channel_join", "channel_leave", "message_deleted"):
                continue
            body = (item.get("text") or "").strip()
            messages.append({"ts": item.get("ts"), "author": _author(item),
                             "text": body if len(body) <= MESSAGE_TEXT_LIMIT else body[:MESSAGE_TEXT_LIMIT] + "…"})
        return messages

    def post(self, channel: SlackChannel, text: str) -> None:
        """Post as Cora's bot. Raises when Slack refuses."""
        self._client.chat_postMessage(channel=channel.id, text=text, unfurl_links=False, unfurl_media=False)


def _author(item: Mapping[str, Any]) -> str:
    """A person as a Slack mention; a bot by its name, since its posts carry no user to mention."""
    if item.get("bot_id") or item.get("subtype") == "bot_message":
        name = (item.get("bot_profile") or {}).get("name") or item.get("username")
        return f"bot: {name}" if name else "bot"
    user = item.get("user")
    return f"<@{user}>" if user else "unknown"
