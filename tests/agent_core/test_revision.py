from __future__ import annotations

import json

import pytest

from packages.agent_core.chatport import FakeChatPort
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.revision import DraftReviser, RevisionFailed
from packages.agent_core.send_gate import SendGate

from .conftest import APPROVER, FakeMessages, enqueue_sms, response, text_block


def _reviser(queue: PendingActionQueue, client: FakeMessages, chat: FakeChatPort) -> DraftReviser:
    return DraftReviser(client=client, model="claude-sonnet-5-5", queue=queue, gate=SendGate(queue, chat, "C_CORA"),
                        base_prompt="Revise drafts.", rules_provider=lambda: ["Sign texts as Josh"],
                        editable_fields={"ghl_sms": ("body",)})


def _revising(queue: PendingActionQueue) -> int:
    action_id = enqueue_sms(queue, "Hi Sam, Josh here. Want to talk funding?")
    queue.attach_card(action_id, "C_CORA", "9.9")
    queue.request_revision(action_id, APPROVER)
    return action_id


def test_revision_changes_only_editable_fields_and_reposts_the_card(queue: PendingActionQueue) -> None:
    chat = FakeChatPort(default_channel="C_CORA")
    client = FakeMessages(response(text_block(json.dumps({"body": "Hi Sam, quick one: free to talk funding?"}))))
    action_id = _revising(queue)
    _reviser(queue, client, chat).revise(queue.get(action_id), "shorter", APPROVER)

    action = queue.get(action_id)
    assert action.status is ActionStatus.PENDING
    assert action.payload == {"contact_id": "c-42", "body": "Hi Sam, quick one: free to talk funding?"}
    assert action.recipient_phone == "+17275550100"
    assert action.revisions[0]["payload"]["body"] == "Hi Sam, Josh here. Want to talk funding?"
    assert action.revision_note == "shorter"
    assert action.card_ts == chat.posts[-1]["ts"]

    request = client.requests[0]
    assert "contact_id" not in request["messages"][0]["content"]
    assert request["output_config"]["format"]["schema"]["required"] == ["body"]
    assert "Sign texts as Josh" in request["system"]


@pytest.mark.parametrize("model_text", ["not json", json.dumps({"body": ""}), json.dumps({"body": "x", "to": "+1999"})])
def test_bad_redraft_leaves_the_draft_untouched(queue: PendingActionQueue, model_text: str) -> None:
    action_id = _revising(queue)
    with pytest.raises(RevisionFailed):
        _reviser(queue, FakeMessages(response(text_block(model_text))), FakeChatPort()).revise(
            queue.get(action_id), "shorter", APPROVER)
    action = queue.get(action_id)
    assert action.status is ActionStatus.REVISING
    assert action.payload["body"] == "Hi Sam, Josh here. Want to talk funding?"


def test_refusal_and_unknown_channels_fail_cleanly(queue: PendingActionQueue) -> None:
    action_id = _revising(queue)
    with pytest.raises(RevisionFailed):
        _reviser(queue, FakeMessages(response(text_block(""), stop_reason="refusal")), FakeChatPort()).revise(
            queue.get(action_id), "x", APPROVER)
    email_id = queue.enqueue(tool_name="draft_email", channel="ghl_email", payload={"body": "x"}, summary="e")
    with pytest.raises(RevisionFailed):
        _reviser(queue, FakeMessages(), FakeChatPort()).revise(queue.get(email_id), "x", APPROVER)
