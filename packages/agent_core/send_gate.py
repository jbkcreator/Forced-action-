"""Universal human send gate: the only thing an external-egress tool call can do.

It freezes the draft into ``pending_actions`` and posts the Approve / Revise / Reject card.
Sending happens later, in the relay, and only after an approver's click.

A draft whose card could not be posted is reported as such (never as "awaiting approval") and is
picked up again by :meth:`SendGate.retry_missing_cards`, which the host runs on its sweep, until
the card posts or the draft expires.
"""
from __future__ import annotations

import logging

from .approval import render_action_card
from .chatport import ChatPort
from .pending_actions import PendingActionQueue
from .tools import EgressDraft, QueuedDraft, ToolContext

logger = logging.getLogger(__name__)


class SendGate:
    def __init__(self, queue: PendingActionQueue, chat: ChatPort, card_channel: str | None) -> None:
        self._queue = queue
        self._chat = chat
        self._card_channel = card_channel

    def submit(self, draft: EgressDraft, *, tool_name: str, context: ToolContext) -> QueuedDraft:
        action_id = self._queue.enqueue(
            tool_name=tool_name, channel=draft.channel, payload=draft.payload, summary=draft.summary,
            requested_by=context.user_id, source_channel=context.channel, source_thread_ts=context.thread_ts,
            recipient_phone=draft.recipient_phone, recipient_email=draft.recipient_email,
            contact_ref=draft.contact_ref, deal_ref=draft.deal_ref, idempotency_key=context.tool_use_id,
        )
        return QueuedDraft(action_id, self.post_card(action_id))

    def post_card(self, action_id: int) -> bool:
        """Make sure the draft has an approval card. True once it has one (already, or posted now)."""
        action = self._queue.get(action_id)
        if action is None:
            return False
        if action.card_ts:
            return True
        posted = self._chat.post(text=f"Approval needed: {action.tool_name} (action {action_id})",
                                 blocks=render_action_card(action), channel=self._card_channel)
        if not (posted.ok and posted.channel and posted.ts):
            logger.error("send gate: approval card for action %s could not be posted; will retry", action_id)
            return False
        self._queue.attach_card(action_id, posted.channel, posted.ts)
        return True

    def retry_missing_cards(self) -> int:
        """Post cards for open drafts that have none (a failed post, or a failed repost after a revise)."""
        posted = 0
        for action_id in self._queue.awaiting_card():
            if self.post_card(action_id):
                posted += 1
        return posted
