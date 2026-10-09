from __future__ import annotations

import pytest
from pydantic import BaseModel, ConfigDict, Field

from packages.agent_core.chatport import FakeChatPort
from packages.agent_core.governance import SafetyLevel
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.send_gate import SendGate
from packages.agent_core.tools import EgressDraft, Tool, ToolContext, ToolInputError, ToolRegistry

from .conftest import APPROVER, OPERATOR


class LookupInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(..., min_length=2)


class SmsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    body: str


def _context(user: str = OPERATOR, tool_use_id: str = "toolu_1") -> ToolContext:
    return ToolContext(user_id=user, is_approver=user == APPROVER, channel="C_CORA", thread_ts="1.0",
                       tool_use_id=tool_use_id)


class Harness:
    def __init__(self, queue: PendingActionQueue) -> None:
        self.chat = FakeChatPort(default_channel="C_CORA")
        self.gate = SendGate(queue, self.chat, "C_CORA")
        self.sms_handler_calls = 0

        def lookup(args: LookupInput, _: ToolContext) -> dict:
            if args.name == "nobody":
                raise ToolInputError("no lead matched")
            if args.name == "boom":
                raise RuntimeError("db exploded with phone 727-555-0100")
            return {"name": args.name, "phone": "+17275550100"}

        def draft_sms(args: SmsInput, _: ToolContext) -> EgressDraft:
            self.sms_handler_calls += 1
            return EgressDraft(channel="ghl_sms", payload={"to_phone": "+17275550100", "body": args.body},
                               summary="Text to Sam", recipient_phone="+17275550100", contact_ref="web_lead:1")

        self.registry = ToolRegistry([
            Tool("lookup", "Look someone up.", LookupInput, SafetyLevel.READ_ONLY, lookup),
            Tool("draft_sms", "Draft a text.", SmsInput, SafetyLevel.EXTERNAL_EGRESS, draft_sms),
            Tool("save_rule", "Save a rule.", LookupInput, SafetyLevel.INTERNAL_WRITE, lambda a, c: "saved",
                 approver_only=True),
        ], self.gate)


@pytest.fixture
def harness(queue: PendingActionQueue) -> Harness:
    return Harness(queue)


def test_definitions_are_sorted_and_carry_pydantic_schemas(harness: Harness) -> None:
    definitions = harness.registry.definitions()
    assert [d["name"] for d in definitions] == ["draft_sms", "lookup", "save_rule"]
    assert definitions[1]["input_schema"]["required"] == ["name"]
    assert definitions[1]["input_schema"]["additionalProperties"] is False


def test_duplicate_tool_names_are_rejected(harness: Harness) -> None:
    tool = Tool("x", "x", LookupInput, SafetyLevel.READ_ONLY, lambda a, c: "")
    with pytest.raises(ValueError):
        ToolRegistry([tool, tool], harness.gate)


def test_read_tool_runs_and_masks_long_numbers(harness: Harness) -> None:
    outcome = harness.registry.execute("lookup", {"name": "Sam"}, _context())
    assert not outcome.is_error
    assert "+17275550100" not in outcome.content and "[…0100]" in outcome.content


@pytest.mark.parametrize("raw,expected", [({"name": "S"}, "invalid input: name"), ({"nme": "Sam"}, "invalid input"),
                                          (None, "invalid input: name")])
def test_invalid_input_comes_back_as_an_error_result(harness: Harness, raw, expected: str) -> None:
    outcome = harness.registry.execute("lookup", raw, _context())
    assert outcome.is_error and outcome.content.startswith(expected)


def test_unknown_tool_and_handler_failures_are_error_results(harness: Harness) -> None:
    assert harness.registry.execute("nope", {}, _context()).is_error
    assert harness.registry.execute("lookup", {"name": "nobody"}, _context()).content == "no lead matched"
    crashed = harness.registry.execute("lookup", {"name": "boom"}, _context())
    assert crashed.is_error and crashed.content == "lookup failed (RuntimeError); nothing was changed"


def test_approver_only_tool_refuses_operators(harness: Harness) -> None:
    assert harness.registry.execute("save_rule", {"name": "rule"}, _context(OPERATOR)).is_error
    assert harness.registry.execute("save_rule", {"name": "rule"}, _context(APPROVER)).content == "saved"


def test_egress_tool_is_queued_with_a_card_and_never_sent(harness: Harness, queue: PendingActionQueue) -> None:
    outcome = harness.registry.execute("draft_sms", {"body": "Hi Sam"}, _context(tool_use_id="toolu_9"))
    assert not outcome.is_error and outcome.action_id is not None
    assert "Nothing has been sent" in outcome.content
    action = queue.get(outcome.action_id)
    assert action.status is ActionStatus.PENDING
    assert action.idempotency_key == "toolu_9"
    assert (action.recipient_phone, action.contact_ref, action.requested_by) == ("+17275550100", "web_lead:1", OPERATOR)
    assert action.card_ts == harness.chat.posts[-1]["ts"]
    assert harness.chat.posts[-1]["channel"] == "C_CORA"


def test_repeated_tool_call_queues_one_action_and_one_card(harness: Harness, queue: PendingActionQueue) -> None:
    first = harness.registry.execute("draft_sms", {"body": "Hi"}, _context(tool_use_id="toolu_same"))
    second = harness.registry.execute("draft_sms", {"body": "Hi again"}, _context(tool_use_id="toolu_same"))
    assert first.action_id == second.action_id
    assert len(harness.chat.posts) == 1
    assert queue.get(first.action_id).payload["body"] == "Hi"


def test_egress_handler_returning_the_wrong_type_is_an_error(queue: PendingActionQueue) -> None:
    gate = SendGate(queue, FakeChatPort(), "C_CORA")
    registry = ToolRegistry([Tool("bad", "x", SmsInput, SafetyLevel.EXTERNAL_EGRESS, lambda a, c: "sent!")], gate)
    outcome = registry.execute("bad", {"body": "x"}, _context())
    assert outcome.is_error and queue.approved_ids() == []
