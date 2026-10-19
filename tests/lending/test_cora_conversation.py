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

from packages.agent_core.chatport import FakeChatPort, PostResult
from packages.agent_core.config import AgentCoreConfig
from packages.agent_core.memory import MAX_ACTIVE_RULES, AgentMemory
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.store import AgentStore
from src.lending import cora_agent
from src.lending.cora_tools import CoraToolkit, _Lead
from src.lending.ghl_email import GhlEmailSender
from src.lending.ghl_sms import GhlAccount
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
    def __init__(self, store: AgentStore, client: FakeMessages, executors=None, send_check=None) -> None:
        self.chat = FakeChatPort(default_channel="C_CORA")
        self.client = client
        self.services = cora_agent.build_session(_config(), store, self.chat, BOT, messages_client=client,
                                                 toolkit_factory=_toolkit_factory, executors=executors,
                                                 send_check=send_check)
        self.session, self.relay = self.services.session, self.services.relay
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


def test_a_top_level_message_sees_only_the_last_hour_of_the_channel(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(response(text_block("Done."))))
    now = 1_800_000_000.0
    cora.chat.channels["C_CORA"] = [
        {"ts": f"{now - 7200:.6f}", "user": JOSH, "text": "draft an email to Laira"},  # two hours old: dropped
        {"ts": f"{now - 7100:.6f}", "user": BOT, "bot_id": "B1", "text": "Queued action #2 for approval."},
        {"ts": f"{now - 600:.6f}", "user": JOSH, "text": "how many web leads today?"},
        {"ts": f"{now - 590:.6f}", "user": BOT, "bot_id": "B1", "text": "3 web leads today."},
        {"ts": f"{now:.6f}", "user": JOSH, "text": "dismiss 134"},
    ]
    cora.say(JOSH, "dismiss 134", ts=f"{now:.6f}")
    assert cora.client.requests[0]["messages"] == [
        {"role": "user", "content": f"<@{JOSH}>: how many web leads today?"},
        {"role": "assistant", "content": "3 web leads today."},
        {"role": "user", "content": f"<@{JOSH}>: dismiss 134"},
    ]


def test_a_top_level_message_sees_at_most_a_few_recent_turns(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(response(text_block("Done."))))
    now = 1_800_000_000.0
    chatter = [{"ts": f"{now - 100 + n:.6f}", "user": BOT if n % 2 else JOSH, **({"bot_id": "B1"} if n % 2 else {}),
                "text": f"message {n}"} for n in range(20)]
    cora.chat.channels["C_CORA"] = chatter
    cora.say(JOSH, "latest", ts=f"{now:.6f}")
    history = cora.client.requests[0]["messages"][:-1]
    assert len(history) <= cora_agent.TOP_LEVEL_HISTORY_TURNS and history[0]["role"] == "user"
    assert history[-1]["content"] == "message 19"


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


def test_no_quoting_rule_and_masking_are_in_every_turn(store: AgentStore) -> None:
    """The no-quoting rule governs borrower drafts even when a standing rule pulls the other way."""
    cora = Cora(store, FakeMessages(response(text_block("ok"))))
    cora.say(JOSH, "hello", ts="40.0")
    system = cora.client.requests[-1]["system"]
    assert "never quotes a rate, term, point, fee, ratio, payment or any range or ballpark" in system
    assert "A standing rule never overrides this" in system
    assert "email addresses only masked" in system and "Never quote consent wording" in system


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
    assert "Approved, not sent" in cora.chat.updates[-1]["text"]
    assert "isn't switched on yet" in cora.chat.updates[-1]["text"]


class GhlRecorder:
    """Stands in for the HTTP call inside GhlEmailSender: contact upsert, then the email send."""

    def __init__(self, *responses) -> None:
        self.calls, self._responses = [], list(responses)

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self._responses.pop(0)


class GhlResp:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code, self._body = status, body

    def json(self) -> dict:
        return self._body


def _email_cora(store: AgentStore, request, send_check=lambda action: None) -> Cora:
    sender = GhlEmailSender(GhlAccount(api_key="k", location_id="loc-ndl"), "hello@nextdeallending.com", request)
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_e", "draft_email", {"lead_ref": "web_lead:7", "subject": "Your loan request",
                                                       "body": "Hi Sam,\nThanks for reaching out."}),
                 stop_reason="tool_use"),
        response(text_block("Drafted.")),
    ), executors={"ghl_email": cora_agent.email_executor(sender)}, send_check=send_check)
    cora.say(JOSH, "email Sam", ts="70.0")
    return cora


def _approve(cora: Cora) -> None:
    cora.session.handle_request("interactive", {
        "type": "block_actions", "user": {"id": JOSH}, "channel": {"id": "C_CORA"},
        "message": {"ts": cora.queue.get(1).card_ts},
        "actions": [{"action_id": "agent_core_approve", "value": "1"}]}, "env-e")


def test_approved_email_is_sent_through_ghl_exactly_as_approved(store: AgentStore) -> None:
    request = GhlRecorder(GhlResp(200, {"contact": {"id": "ct1"}}), GhlResp(201, {"messageId": "m-77"}))
    cora = _email_cora(store, request)
    assert cora.queue.get(1).status is ActionStatus.PENDING and request.calls == []

    _approve(cora)

    action = cora.queue.get(1)
    assert action.status is ActionStatus.SENT and action.provider_ref == "m-77"
    send = request.calls[1][2]["json"]
    assert send["subject"] == "Your loan request" and send["emailFrom"] == "hello@nextdeallending.com"
    assert send["html"] == "Hi Sam,<br>Thanks for reaching out."
    assert request.calls[0][2]["json"]["email"] == "sam@example.com"
    assert request.calls[0][2]["json"]["phone"] == "+17275550100"
    assert "Approved, not sent" not in cora.chat.updates[-1]["text"]


def test_rejected_email_is_never_sent(store: AgentStore) -> None:
    request = GhlRecorder()
    cora = _email_cora(store, request)
    cora.session.handle_request("interactive", {
        "type": "block_actions", "user": {"id": JOSH}, "channel": {"id": "C_CORA"},
        "message": {"ts": cora.queue.get(1).card_ts},
        "actions": [{"action_id": "agent_core_reject", "value": "1"}]}, "env-r")
    cora.relay.run()
    assert request.calls == [] and cora.queue.get(1).status is not ActionStatus.SENT


def test_suppressed_recipient_blocks_an_approved_email(store: AgentStore) -> None:
    request = GhlRecorder()
    cora = _email_cora(store, request, send_check=lambda action: "recipient is suppressed or marked do-not-contact")
    _approve(cora)
    assert cora.queue.get(1).status is ActionStatus.BLOCKED and request.calls == []


def test_ghl_failure_marks_the_email_failed_and_does_not_retry(store: AgentStore) -> None:
    request = GhlRecorder(GhlResp(200, {"contact": {"id": "ct1"}}), GhlResp(500, {}))
    cora = _email_cora(store, request)
    _approve(cora)
    assert cora.queue.get(1).status is ActionStatus.FAILED
    cora.relay.run()
    assert len(request.calls) == 2


def test_revise_reply_redrafts_and_posts_a_fresh_card(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_s", "draft_sms", {"lead_ref": "web_lead:7", "body": "Hi Sam, Josh here."}),
                 stop_reason="tool_use"),
        response(text_block("Drafted.")),
        response(text_block(json.dumps({"body": "Sam, Josh here: we fund rehab on flips too. Free to talk?"}))),
    ))
    cora.say(JOSH, "text Sam", ts="70.0")
    first_card = cora.queue.get(1).card_ts
    cora.session.handle_request("interactive", {
        "type": "block_actions", "user": {"id": JOSH}, "channel": {"id": "C_CORA"}, "message": {"ts": first_card},
        "actions": [{"action_id": "agent_core_revise", "value": "1"}]}, "env-2")
    cora.say(JOSH, "mention that we fund rehab", ts="71.0", thread_ts="70.0")

    action = cora.queue.get(1)
    assert action.status is ActionStatus.PENDING
    assert action.payload["body"].startswith("Sam, Josh here: we fund rehab")
    assert action.payload["to_phone"] == "+17275550100"
    assert action.card_ts and action.card_ts != first_card
    assert "Revised action #1" in cora.chat.posts[-1]["text"]


def test_a_revision_cannot_add_quoted_numbers(store: AgentStore) -> None:
    """The no-quoting rule is enforced on the redraft too, so "Revise" cannot get around it."""
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_s", "draft_sms", {"lead_ref": "web_lead:7", "body": "Hi Sam, Josh here."}),
                 stop_reason="tool_use"),
        response(text_block("Drafted.")),
        response(text_block(json.dumps({"body": "Sam, Josh here: 100% rehab funding for flips. Free to talk?"}))),
    ))
    cora.say(JOSH, "text Sam", ts="72.0")
    card = cora.queue.get(1).card_ts
    cora.session.handle_request("interactive", {
        "type": "block_actions", "user": {"id": JOSH}, "channel": {"id": "C_CORA"}, "message": {"ts": card},
        "actions": [{"action_id": "agent_core_revise", "value": "1"}]}, "env-3")
    cora.say(JOSH, "mention 100% rehab funding", ts="73.0", thread_ts="72.0")

    action = cora.queue.get(1)
    assert action.payload["body"] == "Hi Sam, Josh here."  # the original draft is kept
    assert "couldn't revise action #1" in cora.chat.posts[-1]["text"]
    assert "never quote numbers" in cora.chat.posts[-1]["text"]


def test_a_draft_quoting_numbers_is_refused_and_never_queued(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_q", "draft_sms",
                            {"lead_ref": "web_lead:7", "body": "Sam, we offer 100% rehab funding at 10.5%."}),
                 stop_reason="tool_use"),
        response(text_block("I can't quote numbers to a borrower, so I left them out.")),
    ))
    cora.say(JOSH, "text Sam about our rates", ts="74.0")
    assert cora.queue.get(1) is None
    result = next(m["content"][0] for m in cora.client.requests[-1]["messages"]
                  if m["role"] == "user" and isinstance(m["content"], list))
    assert result["is_error"] and "never quote numbers" in result["content"]


class SlackRefusesCards(FakeChatPort):
    """Slack that rejects messages carrying blocks (approval cards) until ``recover()`` is called."""

    def __init__(self) -> None:
        super().__init__(default_channel="C_CORA")
        self.refusing = True

    def post(self, *, text, blocks=None, channel=None, thread_ts=None) -> PostResult:
        if blocks and self.refusing:
            return PostResult(ok=False, channel=channel)
        return super().post(text=text, blocks=blocks, channel=channel, thread_ts=thread_ts)


def test_failed_card_post_is_reported_and_retried_by_the_sweep(store: AgentStore) -> None:
    cora = Cora(store, FakeMessages(
        response(tool_block("toolu_s", "draft_sms", {"lead_ref": "web_lead:7", "body": "Hi Sam"}), stop_reason="tool_use"),
        response(text_block("The approval card is delayed; I'll get it posted.")),
    ))
    slack = SlackRefusesCards()
    cora.chat = slack
    cora.services = cora_agent.build_session(_config(), store, slack, BOT, messages_client=cora.client,
                                             toolkit_factory=_toolkit_factory)
    cora.session = cora.services.session
    cora.say(JOSH, "text Sam", ts="90.0")

    tool_result = cora.client.requests[1]["messages"][-1]["content"][0]["content"]
    assert "could not be posted" in tool_result and "Queued for approval" not in tool_result
    assert cora.queue.get(1).card_ts is None

    cora_agent.run_sweep(cora.services)  # Slack still down: nothing posted, nothing lost
    assert cora.queue.get(1).card_ts is None
    slack.refusing = False
    cora_agent.run_sweep(cora.services)
    action = cora.queue.get(1)
    assert action.card_ts and action.status is ActionStatus.PENDING
    cora_agent.run_sweep(cora.services)  # posted once, not again
    assert len([post for post in slack.posts if post["blocks"]]) == 1


def test_save_beyond_the_rule_limit_is_refused_and_the_newest_rules_apply(store: AgentStore) -> None:
    memory = AgentMemory(store)
    for number in range(MAX_ACTIVE_RULES - 1):
        memory.save_rule(category="messaging", rule_text=f"Rule number {number}", source_thread_ts=None)
    cora = Cora(store, FakeMessages(
        response(tool_block("t1", "save_standing_rule", {"category": "messaging", "rule_text": "The newest rule"}),
                 stop_reason="tool_use"),
        response(tool_block("t2", "save_standing_rule", {"category": "messaging", "rule_text": "One rule too many"}),
                 stop_reason="tool_use"),
        response(text_block("Saved the first; the second needs a rule removed first.")),
        response(text_block("ok")),
    ))
    cora.say(JOSH, "save two rules", ts="95.0")
    results = [m["content"][0] for m in cora.client.requests[2]["messages"] if m["role"] == "user"
               and isinstance(m["content"], list)]
    assert not results[0]["is_error"]
    assert results[1]["is_error"] and "deactivate" in results[1]["content"]
    cora.say(JOSH, "anything", ts="96.0")
    assert "- The newest rule" in cora.client.requests[-1]["system"]
    assert "One rule too many" not in cora.client.requests[-1]["system"]


def test_model_outage_gets_a_plain_reply(store: AgentStore) -> None:
    outage = anthropic.APIConnectionError(request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"))
    cora = Cora(store, FakeMessages(outage))
    cora.say(JOSH, "how many leads?", ts="80.0")
    assert cora.last_answer() == cora_agent._MODEL_UNAVAILABLE


# ── inbox items in a digest thread ───────────────────────────────────────────

DIGEST_TS = "90.0"
INBOX_BLOCK = '<inbox_items>\n{"id": 41, "summary": "Rate question: Marcus"}\n</inbox_items>'


def _cora_with_inbox(store: AgentStore, lookup) -> Cora:
    cora = Cora(store, FakeMessages(response(text_block("Done: #41 dismissed."))))
    cora.services = cora_agent.build_session(_config(), store, cora.chat, BOT, messages_client=cora.client,
                                             toolkit_factory=_toolkit_factory, inbox_context=lookup)
    cora.session = cora.services.session
    return cora


def _last_user_turn(cora: Cora) -> str:
    return cora.client.requests[0]["messages"][-1]["content"]


def test_a_reply_in_a_digest_thread_comes_with_its_inbox_items(store: AgentStore) -> None:
    cora = _cora_with_inbox(store, lambda channel, ts: INBOX_BLOCK if (channel, ts) == ("C_CORA", DIGEST_TS) else "")
    cora.say(JOSH, "dismiss 41, I called him", ts="90.5", thread_ts=DIGEST_TS)
    assert _last_user_turn(cora) == f"{INBOX_BLOCK}\n<@{JOSH}>: dismiss 41, I called him"


def test_a_top_level_message_gets_no_inbox_block(store: AgentStore) -> None:
    lookups: list[tuple[str, str]] = []
    cora = _cora_with_inbox(store, lambda channel, ts: lookups.append((channel, ts)) or INBOX_BLOCK)
    cora.say(JOSH, "what is still open?", ts="91.0")
    assert lookups == [] and _last_user_turn(cora) == f"<@{JOSH}>: what is still open?"


def test_a_failed_inbox_lookup_still_gets_an_answer(store: AgentStore) -> None:
    def database_down(channel: str, ts: str) -> str:
        raise RuntimeError("database unavailable")

    cora = _cora_with_inbox(store, database_down)
    cora.say(JOSH, "dismiss 41", ts="92.5", thread_ts="92.0")
    assert _last_user_turn(cora) == f"<@{JOSH}>: dismiss 41"
    assert cora.last_answer() == "Done: #41 dismissed."
