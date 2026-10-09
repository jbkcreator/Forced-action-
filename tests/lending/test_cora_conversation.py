"""Cora end to end without Slack, Claude or the shared database: a scripted model, an in-memory chat
and SQLite tables. Covers the acceptance flow: grounded answer, a rule given in one thread followed
in a fresh one, and an external draft halting at the send gate."""
from __future__ import annotations

import json

import anthropic
import httpx
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from packages.agent_core.chatport import FakeChatPort
from packages.agent_core.config import AgentCoreConfig
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.store import AgentStore
from src.lending import cora_agent
from src.lending.cora_tools import CoraToolkit, _Lead
from tests.agent_core.conftest import SQLITE_DDL, FakeMessages, response, text_block, tool_block

JOSH, OPS, BOT = "U_JOSH", "U_OPS", "U_BOT"
SAM = _Lead("web_lead:7", "Sam Smith", "+17275550100", "sam@example.com", "ghl-123")


@pytest.fixture
def store() -> AgentStore:
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        for statement in SQLITE_DDL:
            conn.execute(text(statement))
    yield AgentStore(engine, schema=None)
    engine.dispose()


def _config() -> AgentCoreConfig:
    return AgentCoreConfig(agent_name="Cora", slack_channel_id="C_CORA", approver_user_ids=frozenset({JOSH}),
                           operator_user_ids=frozenset({OPS}), model="claude-sonnet-5-5")


def _toolkit_factory(engine, memory) -> CoraToolkit:
    toolkit = CoraToolkit(engine, memory)
    toolkit._lead = lambda lead_ref: SAM  # type: ignore[method-assign]
    return toolkit


class Cora:
    def __init__(self, store: AgentStore, client: FakeMessages) -> None:
        self.chat = FakeChatPort(default_channel="C_CORA")
        self.client = client
        self.session, self.relay = cora_agent.build_session(_config(), store, self.chat, BOT, messages_client=client,
                                                            toolkit_factory=_toolkit_factory)
        self.queue = PendingActionQueue(store)

    def say(self, user: str, message_text: str, ts: str, thread_ts: str | None = None) -> None:
        event = {"type": "message", "channel_type": "channel", "channel": "C_CORA", "user": user,
                 "text": message_text, "ts": ts}
        if thread_ts:
            event["thread_ts"] = thread_ts
        self.session.handle_request("events_api", {"event": event})

    def last_answer(self) -> str:
        return self.chat.updates[-1]["text"]


def test_answer_replaces_the_placeholder_and_uses_thread_history(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(response(text_block("Yesterday had 4 web leads."))))
    cora.chat.threads[("C_CORA", "10.0")] = [
        {"ts": "10.0", "user": JOSH, "text": "how many web leads today?"},
        {"ts": "10.2", "user": BOT, "bot_id": "B1", "text": "3 web leads today."},
        {"ts": "10.5", "user": JOSH, "text": "and yesterday?"},
    ]
    cora.say(JOSH, "and yesterday?", ts="10.5", thread_ts="10.0")

    assert cora.chat.posts[0]["text"] == cora_agent.PLACEHOLDER_TEXT
    assert cora.chat.updates[-1]["ts"] == cora.chat.posts[0]["ts"]
    assert cora.last_answer() == "Yesterday had 4 web leads."
    sent_messages = cora.client.requests[0]["messages"]
    assert sent_messages == [
        {"role": "user", "content": f"<@{JOSH}>: how many web leads today?"},
        {"role": "assistant", "content": "3 web leads today."},
        {"role": "user", "content": f"<@{JOSH}>: and yesterday?"},
    ]


def test_top_level_message_gets_an_inline_reply_with_channel_history(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(response(text_block("2 of them reached GHL."))))
    cora.chat.channels["C_CORA"] = [
        {"ts": "11.0", "user": JOSH, "text": "how many web leads this month?"},
        {"ts": "11.1", "user": BOT, "bot_id": "B1", "text": "2 web leads this month."},
        {"ts": "11.2", "user": "U_CC", "bot_id": "B_CC", "text": "Command Center: here's what I found"},
        {"ts": "11.5", "user": JOSH, "text": "which reached GHL?"},
    ]
    cora.say(JOSH, "which reached GHL?", ts="11.5")

    assert cora.chat.posts[0]["thread_ts"] is None  # placeholder sits inline under the question
    assert cora.last_answer() == "2 of them reached GHL."
    assert cora.client.requests[0]["messages"] == [
        {"role": "user", "content": f"<@{JOSH}>: how many web leads this month?"},
        {"role": "assistant", "content": "2 web leads this month."},
        {"role": "user", "content": f"<@{JOSH}>: which reached GHL?"},
    ]


def test_message_in_a_thread_is_answered_in_that_thread(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(response(text_block("Sure."))))
    cora.chat.channels["C_CORA"] = [{"ts": "12.0", "user": JOSH, "text": "unrelated channel chatter"}]
    cora.say(JOSH, "side question", ts="13.1", thread_ts="13.0")
    assert cora.chat.posts[0]["thread_ts"] == "13.0"
    assert cora.client.requests[0]["messages"] == [{"role": "user", "content": f"<@{JOSH}>: side question"}]


def test_rule_from_one_thread_governs_a_fresh_thread(store: AgentStore) -> None:
    rule = "Always emphasize 100% rehab funding for flippers"
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_r", "save_standing_rule", {"category": "messaging", "rule_text": rule}),
                 stop_reason="tool_use"),
        response(text_block("Saved: I'll always emphasize 100% rehab funding for flippers.")),
        response(text_block("Here's a pitch that leads with 100% rehab funding.")),
    ))
    cora.say(JOSH, f"From now on: {rule.lower()}", ts="20.0")
    cora.say(JOSH, "write me a flipper pitch", ts="30.1", thread_ts="30.0")  # a fresh thread: no shared history

    fresh = cora.client.requests[-1]
    assert fresh["messages"] == [{"role": "user", "content": f"<@{JOSH}>: write me a flipper pitch"}]
    assert f"<standing_rules>\n- {rule}\n</standing_rules>" in fresh["system"]


def test_operator_cannot_set_a_standing_rule(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_r", "save_standing_rule", {"category": "messaging", "rule_text": "Never call"}),
                 stop_reason="tool_use"),
        response(text_block("Only an approver can set standing rules.")),
    ))
    cora.say(OPS, "from now on never call anyone", ts="40.0")
    result = cora.client.requests[1]["messages"][-1]["content"][0]
    assert result["is_error"] and "approver" in result["content"]
    assert "(none)" in cora.client.requests[0]["system"]


def test_external_draft_halts_at_the_send_gate(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_s", "draft_sms", {"lead_ref": "web_lead:7", "body": "Hi Sam, Josh here."}),
                 stop_reason="tool_use"),
        response(text_block("Drafted as action #1; it's waiting for your approval.")),
    ))
    cora.say(JOSH, "text Sam Smith a hello", ts="50.0")

    card = next(post for post in cora.chat.posts if post["blocks"])
    assert card["channel"] == "C_CORA"
    buttons = next(block for block in card["blocks"] if block["type"] == "actions")["elements"]
    assert [button["text"]["text"] for button in buttons] == ["Approve", "Revise", "Reject"]
    action = cora.queue.get(1)
    assert action.status is ActionStatus.PENDING
    assert action.payload["to_phone"] == "+17275550100" and action.idempotency_key == "toolu_s"
    assert "waiting for your approval" in cora.last_answer()


def test_approved_draft_is_not_sent_without_a_registered_sender(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_s", "draft_sms", {"lead_ref": "web_lead:7", "body": "Hi"}), stop_reason="tool_use"),
        response(text_block("Drafted.")),
    ))
    cora.say(JOSH, "text Sam", ts="60.0")
    card_ts = cora.queue.get(1).card_ts
    cora.session.handle_request("interactive", {
        "type": "block_actions", "user": {"id": JOSH}, "channel": {"id": "C_CORA"}, "message": {"ts": card_ts},
        "actions": [{"action_id": "agent_core_approve", "value": "1"}]}, "env-1")
    assert cora.queue.get(1).status is ActionStatus.FAILED
    assert "send failed" in cora.chat.updates[-1]["text"]


def test_revise_reply_redrafts_and_posts_a_fresh_card(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_s", "draft_sms", {"lead_ref": "web_lead:7", "body": "Hi Sam, Josh here."}),
                 stop_reason="tool_use"),
        response(text_block("Drafted.")),
        response(text_block(json.dumps({"body": "Sam, Josh here: 100% rehab funding for flips. Free to talk?"}))),
    ))
    cora.say(JOSH, "text Sam", ts="70.0")
    first_card = cora.queue.get(1).card_ts
    cora.session.handle_request("interactive", {
        "type": "block_actions", "user": {"id": JOSH}, "channel": {"id": "C_CORA"}, "message": {"ts": first_card},
        "actions": [{"action_id": "agent_core_revise", "value": "1"}]}, "env-2")
    cora.say(JOSH, "mention 100% rehab funding", ts="71.0", thread_ts="70.0")

    action = cora.queue.get(1)
    assert action.status is ActionStatus.PENDING
    assert action.payload["body"].startswith("Sam, Josh here: 100% rehab")
    assert action.payload["to_phone"] == "+17275550100"
    assert action.card_ts and action.card_ts != first_card
    assert "Revised action #1" in cora.chat.posts[-1]["text"]


def test_model_outage_gets_a_plain_reply(store: AgentStore) -> None:
    outage = anthropic.APIConnectionError(request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"))
    cora = Cora(store, FakeMessages(outage))
    cora.say(JOSH, "how many leads?", ts="80.0")
    assert cora.last_answer() == cora_agent._MODEL_UNAVAILABLE
