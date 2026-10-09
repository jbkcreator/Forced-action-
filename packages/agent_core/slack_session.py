"""Slack Socket Mode session manager.

Opens an outbound WebSocket with the app-level token, so interactivity needs no public endpoint.
Every envelope is acknowledged first, then routed:

* Messages (direct messages, or channel messages that mention the bot) from an allowed user:
  1. halt / resume commands, always first and approvers only: a shadowed kill switch is a
     broken kill switch;
  2. a reply from a user with an open revision is the revision instruction (or ``cancel``);
  3. anything else goes to the agent's message handler.
* Button clicks on approval cards: applied through :func:`approval.apply_click`, the card is
  updated in place, and an approved action is handed straight to the relay.

``handle_request`` is the testable seam; :func:`run_socket_mode` is the live wiring.
"""
from __future__ import annotations

import logging
import re
import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .approval import ApprovalAction, apply_click, is_approver, parse_action_id, render_decided_card
from .chatport import ChatPort
from .config import AgentCoreConfig
from .halt import AgentHalted, HaltSwitch
from .pending_actions import PendingAction, PendingActionQueue
from .redaction import slack_safe
from .relay import DispatchOutcome, Relay

logger = logging.getLogger(__name__)

_DEDUP_CAPACITY = 4096
_IGNORED_SUBTYPES = frozenset({"bot_message", "message_changed", "message_deleted", "channel_join",
                               "channel_leave", "thread_broadcast"})

# Backing out of an open revision. Anchored to the whole message, so "no more than three
# sentences" is an instruction, not a cancel. When unsure it is an instruction: a wrong cancel
# loses the typed edit, a wrong revision is visible and still needs approval.
_CANCEL_REVISION = re.compile(
    r"^\s*(?:(?:nah|no|nope|actually|ok|okay)?[\s,]*"
    r"(?:cancel|never\s?mind|forget\s+it|scratch\s+that|leave\s+it(?:\s+as\s+is)?|keep\s+it|"
    r"no\s+change|stop\s+revising|undo)"
    r"|(?:actually\s+)?(?:no|nope|nah))[\s.!,]*$",
    re.IGNORECASE,
)


def is_cancel_revision(message: str) -> bool:
    return bool(_CANCEL_REVISION.match(message or ""))


@dataclass(frozen=True)
class InboundMessage:
    user_id: str
    channel: str
    text: str
    ts: str
    thread_ts: str | None
    is_direct: bool

    @property
    def reply_thread_ts(self) -> str:
        return self.thread_ts or self.ts


MessageHandler = Callable[[InboundMessage], None]
RevisionHandler = Callable[[PendingAction, InboundMessage], None]


class _RecentKeys:
    """Bounded memory of recently seen envelope keys; Slack redelivers on slow acks."""

    def __init__(self, capacity: int = _DEDUP_CAPACITY) -> None:
        self._keys: OrderedDict[str, None] = OrderedDict()
        self._capacity = capacity
        self._lock = threading.Lock()

    def first_sighting(self, key: str) -> bool:
        if not key:
            return True
        with self._lock:
            if key in self._keys:
                return False
            self._keys[key] = None
            if len(self._keys) > self._capacity:
                self._keys.popitem(last=False)
            return True


def strip_mention(message: str, bot_user_id: str) -> str:
    if not bot_user_id:
        return (message or "").strip()
    return re.sub(rf"<@{re.escape(bot_user_id)}(?:\|[^>]*)?>", "", message or "").strip()


def mentions_bot(event: dict[str, Any], bot_user_id: str) -> bool:
    if not bot_user_id:
        return False
    if f"<@{bot_user_id}" in (event.get("text") or ""):
        return True
    for block in event.get("blocks") or []:
        for element in block.get("elements") or []:
            for inner in element.get("elements") or [element]:
                if inner.get("type") == "user" and inner.get("user_id") == bot_user_id:
                    return True
    return False


class SlackSession:
    def __init__(self, *, config: AgentCoreConfig, chat: ChatPort, queue: PendingActionQueue,
                 halt: HaltSwitch, relay: Relay, on_message: MessageHandler,
                 on_revision: RevisionHandler, bot_user_id: str = "") -> None:
        self._config = config
        self._chat = chat
        self._queue = queue
        self._halt = halt
        self._relay = relay
        self._on_message = on_message
        self._on_revision = on_revision
        self.bot_user_id = bot_user_id
        self._seen = _RecentKeys()

    # -- entry point -------------------------------------------------------------------------

    def handle_request(self, request_type: str, payload: dict[str, Any], envelope_id: str = "") -> None:
        """Route one Socket Mode envelope. Never raises: a failed handler must not kill the socket."""
        try:
            if request_type == "events_api":
                self._handle_event(payload.get("event") or {})
            elif request_type == "interactive" and self._seen.first_sighting(f"envelope:{envelope_id}"):
                self._handle_interactive(payload)
        except Exception:
            logger.exception("%s session: unhandled error routing %s", self._config.agent_name, request_type)

    # -- messages ----------------------------------------------------------------------------

    def _to_inbound(self, event: dict[str, Any]) -> InboundMessage | None:
        if event.get("type") not in ("message", "app_mention"):
            return None
        if event.get("bot_id") or event.get("subtype") in _IGNORED_SUBTYPES:
            return None
        user_id = event.get("user") or ""
        if not user_id or user_id == self.bot_user_id:
            return None
        is_direct = event.get("channel_type") == "im"
        if event["type"] == "message" and not is_direct and not mentions_bot(event, self.bot_user_id):
            return None
        channel, ts = event.get("channel") or "", event.get("ts") or ""
        # A mention arrives as both a message and an app_mention event with the same (channel, ts).
        if not self._seen.first_sighting(f"message:{channel}:{ts}"):
            return None
        return InboundMessage(user_id=user_id, channel=channel, text=strip_mention(event.get("text") or "",
                              self.bot_user_id), ts=ts, thread_ts=event.get("thread_ts"), is_direct=is_direct)

    def _reply(self, message: InboundMessage, reply_text: str) -> None:
        self._chat.post(text=reply_text, channel=message.channel, thread_ts=message.reply_thread_ts)

    def _handle_event(self, event: dict[str, Any]) -> None:
        message = self._to_inbound(event)
        if message is None:
            return
        if message.user_id not in self._config.allowed_user_ids:
            logger.info("%s session: ignored message from non-operator %s", self._config.agent_name, message.user_id)
            return
        if self._handle_halt_commands(message):
            return
        revising = self._queue.revising_for(message.user_id)
        try:
            if revising is not None:
                self._handle_revision_reply(revising, message)
            else:
                self._on_message(message)
        except Exception:
            logger.exception("%s session: handler failed for message %s", self._config.agent_name, message.ts)
            self._reply(message, "Something went wrong handling that. Nothing was sent; please try again.")

    def _handle_halt_commands(self, message: InboundMessage) -> bool:
        is_halt = self._halt.is_halt_command(message.text)
        is_resume = not is_halt and self._halt.is_resume_command(message.text)
        if not (is_halt or is_resume):
            return False
        if not is_approver(self._config.approver_user_ids, message.user_id):
            self._reply(message, "Only an approver can halt or resume me.")
            return True
        name = self._config.agent_name
        if is_halt:
            self._halt.set(reason=f"halted from Slack by {message.user_id}", user_id=message.user_id)
            self._reply(message, f":octagonal_sign: {name} is halted. Nothing will be sent until an approver says `resume`.")
        else:
            self._halt.clear(user_id=message.user_id)
            self._reply(message, f":arrow_forward: {name} resumed. Approved actions will send again.")
        return True

    def _handle_revision_reply(self, action: PendingAction, message: InboundMessage) -> None:
        if is_cancel_revision(message.text):
            self._queue.cancel_revision(action.action_id)
            self._reply(message, f"Revision cancelled; action `{action.action_id}` is back awaiting approval.")
            return
        self._on_revision(action, message)

    # -- button clicks -----------------------------------------------------------------------

    def _handle_interactive(self, payload: dict[str, Any]) -> None:
        if payload.get("type") != "block_actions":
            return
        clicked = (payload.get("actions") or [{}])[0]
        try:
            button = ApprovalAction(clicked.get("action_id"))
        except ValueError:
            return
        action_id = parse_action_id(clicked.get("value"))
        if action_id is None:
            return
        user_id = (payload.get("user") or {}).get("id", "")
        channel = (payload.get("channel") or {}).get("id")
        ts = (payload.get("message") or {}).get("ts")

        outcome = apply_click(self._queue, button, action_id, user_id, self._config.approver_user_ids)
        logger.info("%s session: %s on action %s by %s accepted=%s", self._config.agent_name,
                    button.value, action_id, user_id, outcome.accepted)
        if not outcome.accepted:
            if channel and user_id:
                self._chat.post(text=outcome.status_text, channel=channel, thread_ts=ts)
            return
        # Acknowledge on the card before the send, so a slow send never looks like a dead button.
        self._update_card(action_id, channel, ts, outcome.status_text)
        if outcome.dispatch:
            self._update_card(action_id, channel, ts, self._dispatch(action_id))

    def _dispatch(self, action_id: int) -> str:
        try:
            result = self._relay.dispatch(action_id)
        except AgentHalted:
            return ":double_vertical_bar: *Approved, held*: the agent is halted. It sends once an approver resumes."
        if result.outcome is DispatchOutcome.SENT:
            return ":white_check_mark: *Approved and sent.*"
        if result.outcome is DispatchOutcome.BLOCKED:
            return f":no_entry: *Approved, blocked at send*: {slack_safe(result.reason or 'not allowed')}. Nothing was sent."
        if result.outcome is DispatchOutcome.FAILED:
            return ":warning: *Approved, send failed.* Nothing was retried; check the action before re-queuing."
        return "Already handled elsewhere; nothing more was sent."

    def expire_stale_actions(self) -> int:
        """Expire overdue drafts and strip the buttons from their cards. Returns how many expired."""
        expired = self._queue.expire_stale()
        for action in expired:
            if action.card_channel and action.card_ts:
                status_text = ":hourglass: *Expired* before approval. Nothing was sent; ask Cora to redraft."
                self._chat.update(channel=action.card_channel, ts=action.card_ts, text=status_text,
                                  blocks=render_decided_card(action, status_text))
        return len(expired)

    def _update_card(self, action_id: int, channel: str | None, ts: str | None, status_text: str) -> None:
        if not (channel and ts):
            return
        action = self._queue.get(action_id)
        if action is None:
            return
        self._chat.update(channel=channel, ts=ts, text=status_text, blocks=render_decided_card(action, status_text))


def run_socket_mode(session: SlackSession, *, app_token: str, web_client: Any) -> None:
    """Connect over Socket Mode and block forever. Ack first (Slack expects it within 3 s), then route."""
    from slack_sdk.socket_mode import SocketModeClient
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.socket_mode.response import SocketModeResponse

    client = SocketModeClient(app_token=app_token, web_client=web_client)

    def on_request(socket_client: SocketModeClient, request: SocketModeRequest) -> None:
        socket_client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))
        session.handle_request(request.type, request.payload or {}, request.envelope_id or "")

    client.socket_mode_request_listeners.append(on_request)
    client.connect()
    logger.info("socket mode session connected")
    threading.Event().wait()
