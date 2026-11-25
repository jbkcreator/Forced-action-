"""``@Cora resume sms|email|instantly``: an approver reopens a tripped channel breaker (R1 item 0.4).

Run by Cora's Slack session before anything else sees the message, like the emergency halt. Only the exact command
(with an optional mention, "cora", "please" or "now") counts, so a question about it does not. Cards held for the
breaker go back to pending with a fresh approval card; they are never sent without a person approving them again.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Callable, Collection, Optional

from src.lending.dispatch import circuit_breaker

logger = logging.getLogger(__name__)

_MENTION = re.compile(r"<@[^>]+>")
_WORD = re.compile(r"[a-z_]+")
_FILLER = frozenset({"cora", "please", "now", "ghl"})


def resume_channel(message_text: str) -> Optional[str]:
    """The breaker channel named by "@Cora resume sms" / "resume email" / "resume instantly", else None."""
    words = [word for word in _WORD.findall(_MENTION.sub(" ", message_text or "").lower()) if word not in _FILLER]
    if len(words) != 2 or words[0] != "resume":
        return None
    return circuit_breaker.parse_resume_word(words[1])


class ChannelResume:
    """Command hook: True when the message was a channel resume (handled, whoever sent it)."""

    def __init__(self, *, chat: Any, approver_ids: Collection[str],
                 reset: Callable[[str, str], circuit_breaker.ResetResult] = circuit_breaker.reset) -> None:
        self._chat = chat
        self._approvers = frozenset(approver_ids)
        self._reset = reset

    def __call__(self, message: Any) -> bool:
        channel = resume_channel(message.text)
        if channel is None:
            return False
        if message.user_id not in self._approvers:
            self._reply(message, "Only an approver can reopen a paused channel.")
            return True
        label = circuit_breaker.LABELS[channel]
        try:
            result = self._reset(channel, f"slack:{message.user_id}")
        except Exception as exc:  # class only: SQL errors embed bound parameters
            logger.error("[channel-breaker] resume of %s failed (%s)", channel, type(exc).__name__)
            self._reply(message, f":warning: {label} could not be reopened; it is still paused. Try again.")
            return True
        if not result.was_open and not result.reopened_cards:
            self._reply(message, f"{label} was not paused; nothing changed.")
        else:
            self._reply(message, f":white_check_mark: {label} sending reopened. {result.reopened_cards} held card(s) "
                                 "are back for approval; nothing sends until each is approved again.")
        return True

    def _reply(self, message: Any, reply_text: str) -> None:
        self._chat.post(text=reply_text, channel=message.channel, thread_ts=message.reply_thread_ts)


def first_handled(*hooks: Callable[[Any], bool]) -> Callable[[Any], bool]:
    """One command hook that runs ``hooks`` in order and stops at the first that handled the message."""

    def run(message: Any) -> bool:
        return any(hook(message) for hook in hooks)

    return run
