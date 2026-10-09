from __future__ import annotations

from datetime import timedelta

import pytest

from packages.agent_core.chatport import FakeChatPort
from packages.agent_core.config import AgentCoreConfig
from packages.agent_core.halt import HaltSwitch
from packages.agent_core.pending_actions import ActionStatus, PendingAction, PendingActionQueue
from packages.agent_core.relay import Relay
from packages.agent_core.slack_session import InboundMessage, SlackSession, is_cancel_revision

from .conftest import APPROVER, BOT, OPERATOR, STRANGER, Clock, RecordingExecutor, enqueue_sms


class Harness:
    def __init__(self, config: AgentCoreConfig, queue: PendingActionQueue, halt: HaltSwitch, relay: Relay,
                 fail_handler: bool = False) -> None:
        self.chat = FakeChatPort(default_channel="C_CORA")
        self.messages: list[InboundMessage] = []
        self.revisions: list[tuple[PendingAction, InboundMessage]] = []
        self.fail_handler = fail_handler
        self.session = SlackSession(config=config, chat=self.chat, queue=queue, halt=halt, relay=relay,
                                    on_message=self._on_message, on_revision=self._on_revision, bot_user_id=BOT)

    def _on_message(self, message: InboundMessage) -> None:
        if self.fail_handler:
            raise RuntimeError("boom")
        self.messages.append(message)

    def _on_revision(self, action: PendingAction, message: InboundMessage) -> None:
        self.revisions.append((action, message))

    def dm(self, user: str, message_text: str, ts: str = "100.1") -> None:
        self.session.handle_request("events_api", {"event": {
            "type": "message", "channel_type": "im", "channel": "D_JOSH", "user": user, "text": message_text, "ts": ts}})

    def click(self, user: str, action_id_value: str, button: str = "agent_core_approve",
              envelope_id: str = "env-1") -> None:
        self.session.handle_request("interactive", {
            "type": "block_actions", "user": {"id": user}, "channel": {"id": "C_CORA"},
            "message": {"ts": "555.5"}, "actions": [{"action_id": button, "value": action_id_value}],
        }, envelope_id)


@pytest.fixture
def harness(config, queue, halt, relay) -> Harness:
    return Harness(config, queue, halt, relay)


def test_direct_message_from_operator_reaches_the_agent(harness: Harness) -> None:
    harness.dm(OPERATOR, "How many leads today?")
    assert [message.text for message in harness.messages] == ["How many leads today?"]
    assert harness.messages[0].is_direct


def test_strangers_and_bots_are_ignored(harness: Harness) -> None:
    harness.dm(STRANGER, "hello")
    harness.session.handle_request("events_api", {"event": {
        "type": "message", "channel_type": "im", "channel": "D1", "user": OPERATOR, "bot_id": "B1",
        "text": "echo", "ts": "1.0"}})
    harness.dm(BOT, "my own post", ts="2.0")
    assert harness.messages == []


def test_channel_message_needs_a_mention_and_is_routed_once(harness: Harness) -> None:
    plain = {"type": "message", "channel_type": "channel", "channel": "C1", "user": OPERATOR, "text": "chatter", "ts": "1.1"}
    mention = {"channel": "C1", "user": OPERATOR, "text": f"<@{BOT}> pipeline status?", "ts": "1.2",
               "channel_type": "channel"}
    harness.session.handle_request("events_api", {"event": plain})
    harness.session.handle_request("events_api", {"event": {**mention, "type": "message"}})
    harness.session.handle_request("events_api", {"event": {**mention, "type": "app_mention"}})
    assert [message.text for message in harness.messages] == ["pipeline status?"]


def test_approver_halts_and_resumes(harness: Harness, halt: HaltSwitch) -> None:
    harness.dm(APPROVER, "stop all", ts="1")
    assert halt.is_halted()
    harness.dm(APPROVER, "resume", ts="2")
    assert not halt.is_halted()
    assert harness.messages == []


def test_operator_cannot_halt(harness: Harness, halt: HaltSwitch) -> None:
    harness.dm(OPERATOR, "stop all")
    assert not halt.is_halted()
    assert "Only an approver" in harness.chat.posts[-1]["text"]


def test_reply_during_revision_goes_to_the_revision_handler(harness: Harness, queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.request_revision(action_id, APPROVER)
    harness.dm(APPROVER, "make it shorter")
    assert [(action.action_id, message.text) for action, message in harness.revisions] == [(action_id, "make it shorter")]
    assert harness.messages == []


def test_cancel_during_revision_restores_pending(harness: Harness, queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.request_revision(action_id, APPROVER)
    harness.dm(APPROVER, "never mind")
    assert queue.get(action_id).status is ActionStatus.PENDING
    assert harness.revisions == []


@pytest.mark.parametrize("message_text,expected", [("cancel", True), ("Cancel.", True), ("nah, forget it", True),
                                                   ("actually no", True), ("no more than three sentences", False),
                                                   ("make it shorter", False)])
def test_cancel_detection(message_text: str, expected: bool) -> None:
    assert is_cancel_revision(message_text) is expected


def test_handler_error_gets_a_reply_not_a_crash(config, queue, halt, relay) -> None:
    harness = Harness(config, queue, halt, relay, fail_handler=True)
    harness.dm(OPERATOR, "hi")
    assert "Nothing was sent" in harness.chat.posts[-1]["text"]


def test_approve_click_sends_once_and_updates_the_card(harness: Harness, queue: PendingActionQueue,
                                                        executor: RecordingExecutor) -> None:
    action_id = enqueue_sms(queue)
    harness.click(APPROVER, str(action_id), envelope_id="env-1")
    harness.click(APPROVER, str(action_id), envelope_id="env-1")  # Slack redelivery
    harness.click(APPROVER, str(action_id), envelope_id="env-2")  # second tap
    assert len(executor.sent) == 1
    assert queue.get(action_id).status is ActionStatus.SENT
    assert "Approved and sent" in harness.chat.updates[-1]["text"]


def test_non_approver_click_sends_nothing(harness: Harness, queue: PendingActionQueue,
                                          executor: RecordingExecutor) -> None:
    action_id = enqueue_sms(queue)
    harness.click(OPERATOR, str(action_id))
    assert executor.sent == []
    assert queue.get(action_id).status is ActionStatus.PENDING
    assert harness.chat.updates == []


def test_approval_while_halted_is_held(harness: Harness, queue: PendingActionQueue, halt: HaltSwitch,
                                       executor: RecordingExecutor) -> None:
    action_id = enqueue_sms(queue)
    halt.set("stop", APPROVER)
    harness.click(APPROVER, str(action_id))
    assert executor.sent == []
    assert queue.get(action_id).status is ActionStatus.APPROVED
    assert "held" in harness.chat.updates[-1]["text"]


def test_approval_blocked_at_send_says_why(config, queue, halt, executor: RecordingExecutor) -> None:
    relay = Relay(queue, halt, {"ghl_sms": executor}, send_check=lambda action: "no text consent on record for this number")
    harness = Harness(config, queue, halt, relay)
    action_id = enqueue_sms(queue)
    harness.click(APPROVER, str(action_id))
    assert executor.sent == []
    assert queue.get(action_id).status is ActionStatus.BLOCKED
    assert "blocked at send" in harness.chat.updates[-1]["text"]
    assert "no text consent" in harness.chat.updates[-1]["text"]


def test_expired_drafts_lose_their_buttons(harness: Harness, queue: PendingActionQueue, clock: Clock) -> None:
    with_card = enqueue_sms(queue, ttl=timedelta(hours=1))
    queue.attach_card(with_card, "C_CORA", "777.7")
    enqueue_sms(queue, ttl=timedelta(hours=1))  # never got a card
    clock.advance(timedelta(hours=2))
    assert harness.session.expire_stale_actions() == 2
    assert [update["ts"] for update in harness.chat.updates] == ["777.7"]
    assert "Expired" in harness.chat.updates[0]["text"]
    assert all(block["type"] != "actions" for block in harness.chat.updates[0]["blocks"])


@pytest.mark.parametrize("value,button", [("not-a-number", "agent_core_approve"), ("1", "someone_elses_button")])
def test_foreign_or_malformed_clicks_are_ignored(harness: Harness, queue: PendingActionQueue, value: str,
                                                 button: str) -> None:
    enqueue_sms(queue)
    harness.click(APPROVER, value, button=button)
    assert harness.chat.updates == [] and harness.chat.posts == []
