from __future__ import annotations

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict

from packages.agent_core.agent_loop import REFUSAL_REPLY, ROUND_LIMIT_REPLY, AgentLoop
from packages.agent_core.chatport import FakeChatPort
from packages.agent_core.governance import SafetyLevel
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.send_gate import SendGate
from packages.agent_core.tools import EgressDraft, Tool, ToolContext, ToolRegistry

from .conftest import APPROVER, FakeMessages, response, text_block, tool_block


class CountInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    day: str


class SmsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    body: str


def _context(tool_use_id: str) -> ToolContext:
    return ToolContext(user_id=APPROVER, is_approver=True, channel="C_CORA", thread_ts="1.0", tool_use_id=tool_use_id)


def _loop(queue: PendingActionQueue, client: FakeMessages, rules: Sequence[str] = (), max_rounds: int = 8) -> AgentLoop:
    def draft_sms(args: SmsInput, _: ToolContext) -> EgressDraft:
        return EgressDraft(channel="ghl_sms", payload={"body": args.body}, summary="Text", recipient_phone="+17275550100")

    registry = ToolRegistry([
        Tool("count_leads", "Count leads on a day.", CountInput, SafetyLevel.READ_ONLY, lambda a, c: {"leads": 3}),
        Tool("draft_sms", "Draft a text.", SmsInput, SafetyLevel.EXTERNAL_EGRESS, draft_sms),
    ], SendGate(queue, FakeChatPort(), "C_CORA"))
    return AgentLoop(client=client, model="claude-sonnet-5-5", registry=registry, base_prompt="You are Cora.",
                     rules_provider=lambda: list(rules), max_rounds=max_rounds)


def test_plain_answer_needs_one_round(queue: PendingActionQueue) -> None:
    client = FakeMessages(response(text_block("Hello Josh.")))
    reply = _loop(queue, client).run(history=[], user_text="hi", context_for=_context)
    assert (reply.text, reply.rounds, reply.tool_calls) == ("Hello Josh.", 1, ())
    request = client.requests[0]
    assert request["model"] == "claude-sonnet-5-5"
    assert request["messages"] == [{"role": "user", "content": "hi"}]
    assert request["output_config"] == {"effort": "medium"}
    assert "tool_choice" not in request


def test_tool_result_goes_back_and_answer_is_grounded(queue: PendingActionQueue) -> None:
    client = FakeMessages(
        response(tool_block("toolu_1", "count_leads", {"day": "2026-10-09"}), stop_reason="tool_use"),
        response(text_block("3 leads today.")),
    )
    reply = _loop(queue, client).run(history=[{"role": "user", "content": "earlier"},
                                              {"role": "assistant", "content": "ok"}],
                                     user_text="how many leads?", context_for=_context)
    assert reply.text == "3 leads today." and reply.tool_calls == ("count_leads",)
    second = client.requests[1]["messages"]
    assert second[0]["content"] == "earlier"
    assert second[-2]["role"] == "assistant"
    assert second[-1] == {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": "toolu_1", "content": '{"leads": 3}', "is_error": False}]}


def test_parallel_tool_calls_return_in_one_message(queue: PendingActionQueue) -> None:
    client = FakeMessages(
        response(tool_block("t1", "count_leads", {"day": "a"}), tool_block("t2", "count_leads", {"wrong": 1}),
                 stop_reason="tool_use"),
        response(text_block("done")),
    )
    _loop(queue, client).run(history=[], user_text="q", context_for=_context)
    results = client.requests[1]["messages"][-1]["content"]
    assert [r["tool_use_id"] for r in results] == ["t1", "t2"]
    assert [r["is_error"] for r in results] == [False, True]


def test_egress_tool_call_only_queues_a_draft(queue: PendingActionQueue) -> None:
    client = FakeMessages(
        response(tool_block("toolu_sms", "draft_sms", {"body": "Hi Sam"}), stop_reason="tool_use"),
        response(text_block("Drafted; waiting for approval.")),
    )
    reply = _loop(queue, client).run(history=[], user_text="text Sam", context_for=_context)
    assert len(reply.queued_action_ids) == 1
    action = queue.get(reply.queued_action_ids[0])
    assert action.status is ActionStatus.PENDING and action.idempotency_key == "toolu_sms"
    assert "Nothing has been sent" in client.requests[1]["messages"][-1]["content"][0]["content"]


def test_standing_rules_are_in_every_system_prompt(queue: PendingActionQueue) -> None:
    client = FakeMessages(response(tool_block("t1", "count_leads", {"day": "a"}), stop_reason="tool_use"),
                          response(text_block("ok")))
    _loop(queue, client, rules=["Always emphasize 100% rehab funding for flippers"]).run(
        history=[], user_text="q", context_for=_context)
    for request in client.requests:
        assert "<standing_rules>\n- Always emphasize 100% rehab funding for flippers\n</standing_rules>" in request["system"]


def test_round_limit_stops_the_loop(queue: PendingActionQueue) -> None:
    client = FakeMessages(*[response(tool_block(f"t{i}", "count_leads", {"day": "a"}), stop_reason="tool_use")
                            for i in range(3)])
    reply = _loop(queue, client, max_rounds=3).run(history=[], user_text="q", context_for=_context)
    assert reply.text == ROUND_LIMIT_REPLY and reply.stop_reason == "round_limit"
    assert len(client.requests) == 3


def test_refusal_and_truncation_are_reported(queue: PendingActionQueue) -> None:
    refused = _loop(queue, FakeMessages(response(text_block("..."), stop_reason="refusal"))).run(
        history=[], user_text="q", context_for=_context)
    assert refused.text == REFUSAL_REPLY
    cut = _loop(queue, FakeMessages(response(text_block("Partial answer"), stop_reason="max_tokens"))).run(
        history=[], user_text="q", context_for=_context)
    assert cut.text.startswith("Partial answer") and "cut off" in cut.text
