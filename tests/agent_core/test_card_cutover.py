"""Moving to a new Slack app: open drafts whose cards the old app posted get usable replacement cards."""
from __future__ import annotations

from packages.agent_core.chatport import FakeChatPort, PostResult
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.send_gate import SendGate

from .conftest import APPROVER, enqueue_sms


def _gate(queue: PendingActionQueue, chat: FakeChatPort) -> SendGate:
    return SendGate(queue, chat, "C_CORA")


def test_a_card_posted_by_the_old_app_is_replaced_under_this_app(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.attach_card(action_id, "C_CORA", "111.1")  # posted by the lending app before the move
    chat = FakeChatPort(default_channel="C_CORA", foreign_ts={"111.1"})

    assert _gate(queue, chat).adopt_cards() == 1
    action = queue.get(action_id)
    assert action.card_ts == chat.posts[0]["ts"] != "111.1"
    assert action.status is ActionStatus.PENDING
    assert any(block["type"] == "actions" for block in chat.posts[0]["blocks"])  # usable buttons


def test_the_replacement_card_is_approvable(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.attach_card(action_id, "C_CORA", "111.1")
    _gate(queue, FakeChatPort(default_channel="C_CORA", foreign_ts={"111.1"})).adopt_cards()
    assert queue.approve(action_id, APPROVER)


def test_this_apps_own_card_is_refreshed_in_place_not_reposted(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.attach_card(action_id, "C_CORA", "222.2")
    chat = FakeChatPort(default_channel="C_CORA")

    assert _gate(queue, chat).adopt_cards() == 0
    assert chat.posts == [] and [update["ts"] for update in chat.updates] == ["222.2"]
    assert queue.get(action_id).card_ts == "222.2"


def test_a_slack_outage_never_double_posts(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.attach_card(action_id, "C_CORA", "333.3")

    class DownChat(FakeChatPort):
        def update(self, *, channel, ts, text, blocks=None) -> PostResult:
            return PostResult(ok=False, channel=channel, ts=ts, error=None)  # network error: no Slack code

    chat = DownChat(default_channel="C_CORA")
    assert _gate(queue, chat).adopt_cards() == 0
    assert chat.posts == [] and queue.get(action_id).card_ts == "333.3"


def test_decided_drafts_are_left_alone(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.attach_card(action_id, "C_CORA", "444.4")
    queue.reject(action_id, APPROVER)
    chat = FakeChatPort(default_channel="C_CORA", foreign_ts={"444.4"})
    assert _gate(queue, chat).adopt_cards() == 0 and chat.posts == [] and chat.updates == []
