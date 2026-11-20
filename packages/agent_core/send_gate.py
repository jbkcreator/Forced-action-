"""Universal human send gate: the only thing an external-egress tool call can do.

It freezes the draft into ``pending_actions`` and posts the Approve / Revise / Reject card.
Sending happens later, in the relay, and only after an approver's click.

A draft whose card could not be posted is reported as such (never as "awaiting approval") and is
picked up again by :meth:`SendGate.retry_missing_cards`, which the host runs on its sweep, until
the card posts or the draft expires.

A draft that breaks the lane deny list (:mod:`.lane_guard`) is stored as ``blocked`` and gets no
card; the model is told it was refused.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

from .approval import render_action_card
from .chatport import ChatPort
from .lane_guard import LaneViolation, lane_block_reason, lane_violation
from .pending_actions import PendingActionQueue
from .tools import EgressDraft, QueuedDraft, ToolContext

logger = logging.getLogger(__name__)

# chat.update errors meaning the card can never carry this app's buttons again.
_CARD_NOT_OURS = frozenset({"cant_update_message", "message_not_found"})


def reject_if_lane_violation(queue: PendingActionQueue, action_id: int, payload: Mapping[str, Any],
                             tool_name: str) -> None:
    """Block a stored draft that names Backflip and raise :class:`LaneViolation`; no card is ever posted for it."""
    rule = lane_violation(payload)
    if rule is None:
        return
    queue.block_pending(action_id, lane_block_reason(rule))
    logger.error("send gate SECURITY: action %s (%s) blocked by lane deny rule %s; no card posted",
                 action_id, tool_name, rule)
    raise LaneViolation(f"Action #{action_id} was blocked: it names Backflip (a Backflip name, number or "
                        "domain). Cora sends for Next Deal Lending only. Nothing was sent and no card was "
                        "posted; redraft without it.")


def lane_checked_enqueue(queue: PendingActionQueue) -> Callable[..., int]:
    """``queue.enqueue`` with the send gate's Backflip check, for a host that queues drafts outside ``submit`` (the
    card is posted later by the sweep). A draft that names Backflip is stored as ``blocked`` (so it never gets a card)
    and :class:`LaneViolation` is raised."""
    def enqueue(**kwargs: Any) -> int:
        action_id = queue.enqueue(**kwargs)
        reject_if_lane_violation(queue, action_id, kwargs.get("payload") or {}, str(kwargs.get("tool_name")))
        return action_id

    return enqueue


class SendGate:
    def __init__(self, queue: PendingActionQueue, chat: ChatPort, card_channel: str | None) -> None:
        self._queue = queue
        self._chat = chat
        self._card_channel = card_channel

    def submit(self, draft: EgressDraft, *, tool_name: str, context: ToolContext,
               ttl: timedelta | None = None, post_card: bool = True) -> QueuedDraft:
        """``ttl`` overrides the queue's default expiry (a host's job may carry its own deadline).

        ``post_card=False`` stores the draft without an approval card, for a host that approves it itself in the
        same step (a graduated standing send): the lane check still runs, a card is never posted for it.
        """
        action_id = self._queue.enqueue(
            tool_name=tool_name, channel=draft.channel, payload=draft.payload, summary=draft.summary,
            requested_by=context.user_id, source_channel=context.channel, source_thread_ts=context.thread_ts,
            recipient_phone=draft.recipient_phone, recipient_email=draft.recipient_email,
            contact_ref=draft.contact_ref, deal_ref=draft.deal_ref, idempotency_key=context.tool_use_id, ttl=ttl,
        )
        reject_if_lane_violation(self._queue, action_id, draft.payload, tool_name)
        return QueuedDraft(action_id, self.post_card(action_id) if post_card else False)

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

    def adopt_cards(self) -> int:
        """Give every open draft a card this app can act on. Returns how many were reposted.

        Run once at startup. Each open card is re-rendered in place; a card Slack will not let
        this app edit (posted by another app, e.g. before a move to a dedicated bot) or that no
        longer exists is replaced by a fresh card, since clicks on it can never reach this app.
        Any other failure leaves the card alone, so a Slack outage cannot double-post.
        """
        reposted = 0
        for action_id in self._queue.carded_open_ids():
            action = self._queue.get(action_id)
            if action is None or not (action.card_channel and action.card_ts):
                continue
            refreshed = self._chat.update(channel=action.card_channel, ts=action.card_ts,
                                          text=f"Approval needed: {action.tool_name} (action {action_id})",
                                          blocks=render_action_card(action))
            if refreshed.ok or refreshed.error not in _CARD_NOT_OURS:
                continue
            posted = self._chat.post(text=f"Approval needed: {action.tool_name} (action {action_id})",
                                     blocks=render_action_card(action), channel=self._card_channel)
            if posted.ok and posted.channel and posted.ts:
                self._queue.attach_card(action_id, posted.channel, posted.ts)
                reposted += 1
                logger.info("send gate: action %s had a card this app cannot use; posted a new one", action_id)
            else:
                logger.error("send gate: replacement card for action %s could not be posted", action_id)
        return reposted

    def retry_missing_cards(self) -> int:
        """Post cards for open drafts that have none (a failed post, or a failed repost after a revise)."""
        posted = 0
        for action_id in self._queue.awaiting_card():
            if self.post_card(action_id):
                posted += 1
        return posted
