"""Universal human send gate: the only thing an external-egress tool call can do.

It freezes the draft into ``pending_actions`` and posts the Approve / Revise / Reject card.
Sending happens later, in the relay, and only after an approver's click.
"""
from __future__ import annotations

import logging

from .approval import render_action_card
from .chatport import ChatPort
from .pending_actions import PendingActionQueue
from .tools import EgressDraft, ToolContext

logger = logging.getLogger(__name__)


class SendGate:
    def __init__(self, queue: PendingActionQueue, chat: ChatPort, card_channel: str | None) -> None:
        self._queue = queue
        self._chat = chat
        self._card_channel = card_channel

    def submit(self, draft: EgressDraft, *, tool_name: str, context: ToolContext) -> int:
        action_id = self._queue.enqueue(
            tool_name=tool_name, channel=draft.channel, payload=draft.payload, summary=draft.summary,
            requested_by=context.user_id, source_channel=context.channel, source_thread_ts=context.thread_ts,
            recipient_phone=draft.recipient_phone, recipient_email=draft.recipient_email,
            contact_ref=draft.contact_ref, deal_ref=draft.deal_ref, idempotency_key=context.tool_use_id,
        )
        self.post_card(action_id)
        return action_id

    def post_card(self, action_id: int) -> bool:
        """Post the approval card once per draft version. A repeated submit does not repost it."""
        action = self._queue.get(action_id)
        if action is None or action.card_ts:
            return False
        posted = self._chat.post(text=f"Approval needed: {action.tool_name} (action {action_id})",
                                 blocks=render_action_card(action), channel=self._card_channel)
        if not (posted.ok and posted.channel and posted.ts):
            logger.error("send gate: approval card for action %s could not be posted", action_id)
            return False
        self._queue.attach_card(action_id, posted.channel, posted.ts)
        return True
