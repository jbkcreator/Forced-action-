"""Lane deny list: no draft naming Backflip, its numbers or its domain is carded, revised in, or sent."""
from __future__ import annotations

import json
import logging

import pytest

from packages.agent_core.chatport import FakeChatPort
from packages.agent_core.lane_guard import lane_block_reason, lane_violation
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.relay import DispatchOutcome, Relay
from packages.agent_core.revision import DraftReviser, RevisionFailed
from packages.agent_core.send_gate import SendGate

from .conftest import APPROVER, FakeMessages, RecordingExecutor, enqueue_sms, response, text_block
from .test_tools_and_gate import Harness, _context


@pytest.fixture
def harness(queue: PendingActionQueue) -> Harness:
    return Harness(queue)


@pytest.mark.parametrize(("text", "rule"), [
    ("Ask Backflip about it", "blocked_name"),
    ("BACKFLIP", "blocked_name"),
    ("talk to back flip", "blocked_name"),
    ("a back-flip deal", "blocked_name"),
    ("two backflips", "blocked_name"),
    ("see backflip.com", "blocked_domain"),
    ("https://www.backflip.com/apply", "blocked_domain"),
    ("write to josh@BackFlip.com", "blocked_domain"),
    ("call (656) 209-6700", "blocked_number"),
    ("call 656-209-6700", "blocked_number"),
    ("call 656.209.6700", "blocked_number"),
    ("call +1 (656) 209-6700", "blocked_number"),
    ("call 6562096700", "blocked_number"),
    ("call (656) 207-6500 today", "blocked_number"),
])
def test_each_deny_rule_and_format_is_caught(text: str, rule: str) -> None:
    assert lane_violation({"body": text}) == rule


@pytest.mark.parametrize("text", [
    "Hi Sam, Josh from Next Deal Lending. Free to talk funding?",
    "flip the back door",
    "call (656) 221-1100",
    "call 656-209-6701",
    "ref 96562096700",
    "ref 65620967001",
    "",
])
def test_clean_text_passes(text: str) -> None:
    assert lane_violation({"body": text}) is None


def test_every_string_in_the_payload_is_scanned() -> None:
    assert lane_violation({"to_email": "josh@backflip.com", "body": "Hi"}) == "blocked_domain"
    assert lane_violation({"to_phone": "+16562096700", "body": "Hi"}) == "blocked_number"
    assert lane_violation({"meta": {"notes": ["fine", "via Backflip"]}}) == "blocked_name"
    assert lane_violation({"lead_ref": "web_lead:12", "ghl_contact_id": "abc123", "stage": "Application"}) is None


def test_digits_from_separate_fields_never_join_into_a_blocked_number() -> None:
    assert lane_violation({"a": "656", "b": "2096700"}) is None


def test_gate_stores_the_draft_as_blocked_and_posts_no_card(harness: Harness, queue: PendingActionQueue,
                                                            caplog: pytest.LogCaptureFixture) -> None:
    body = "Hi Sam, Backflip can fund this, call (656) 209-6700"
    with caplog.at_level(logging.ERROR, logger="packages.agent_core.send_gate"):
        outcome = harness.registry.execute("draft_sms", {"body": body}, _context(tool_use_id="toolu_bf"))

    assert outcome.is_error
    assert "blocked" in outcome.content and "Nothing was sent" in outcome.content
    assert harness.chat.posts == []

    action = queue.get(1)
    assert action.status is ActionStatus.BLOCKED
    assert action.error == lane_block_reason("blocked_number")
    assert action.card_ts is None

    security_logs = [record.getMessage() for record in caplog.records if "SECURITY" in record.getMessage()]
    assert security_logs and "blocked_number" in security_logs[0]
    assert all("Sam" not in line and "209" not in line and "17275550100" not in line for line in security_logs)


def test_blocked_draft_is_never_carded_by_the_retry_sweep(harness: Harness, queue: PendingActionQueue) -> None:
    harness.registry.execute("draft_sms", {"body": "via backflip.com"}, _context())
    assert harness.gate.retry_missing_cards() == 0
    assert harness.chat.posts == []


def test_clean_draft_still_gets_its_card(harness: Harness, queue: PendingActionQueue) -> None:
    outcome = harness.registry.execute("draft_sms", {"body": "Hi Sam, free to talk funding?"}, _context())
    assert not outcome.is_error
    assert queue.get(outcome.action_id).status is ActionStatus.PENDING
    assert len(harness.chat.posts) == 1


def test_redraft_naming_backflip_is_refused_and_the_draft_kept(queue: PendingActionQueue) -> None:
    chat = FakeChatPort(default_channel="C_CORA")
    action_id = enqueue_sms(queue, "Hi Sam, Josh here.")
    queue.attach_card(action_id, "C_CORA", "9.9")
    queue.request_revision(action_id, APPROVER)
    reviser = DraftReviser(client=FakeMessages(response(text_block(json.dumps({"body": "Backflip says hi"})))),
                           model="claude-sonnet-5-5", queue=queue, gate=SendGate(queue, chat, "C_CORA"),
                           base_prompt="Revise drafts.", rules_provider=lambda: [],
                           editable_fields={"ghl_sms": ("body",)})

    with pytest.raises(RevisionFailed, match="Backflip"):
        reviser.revise(queue.get(action_id), "mention our partner", APPROVER)

    action = queue.get(action_id)
    assert action.status is ActionStatus.REVISING
    assert action.payload["body"] == "Hi Sam, Josh here."
    assert chat.posts == []


def test_relay_blocks_an_approved_payload_naming_backflip(queue: PendingActionQueue, relay: Relay,
                                                          executor: RecordingExecutor) -> None:
    action_id = enqueue_sms(queue, "Reply to backflip.com")
    queue.approve(action_id, APPROVER)

    result = relay.dispatch(action_id)

    assert result.outcome is DispatchOutcome.BLOCKED
    assert result.reason == lane_block_reason("blocked_domain")
    assert executor.sent == []
    assert queue.get(action_id).status is ActionStatus.BLOCKED


def test_relay_still_sends_a_clean_approved_payload(queue: PendingActionQueue, relay: Relay,
                                                    executor: RecordingExecutor) -> None:
    action_id = enqueue_sms(queue, "Hi Sam, Josh here.")
    queue.approve(action_id, APPROVER)
    assert relay.dispatch(action_id).outcome is DispatchOutcome.SENT
    assert len(executor.sent) == 1


def test_block_pending_moves_only_pending_rows(queue: PendingActionQueue) -> None:
    action_id = enqueue_sms(queue)
    queue.approve(action_id, APPROVER)
    assert queue.block_pending(action_id, "BACKFLIP_LANE: blocked_name") is False
    assert queue.get(action_id).status is ActionStatus.APPROVED
