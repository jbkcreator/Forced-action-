"""The channels Cora's bot is in: listing, caching, lookup, reading, and the list/read tools. No Slack."""
from __future__ import annotations

import time

import pytest

from packages.agent_core.tools import ToolContext, ToolRegistry
from src.lending.cora_channels import MESSAGE_TEXT_LIMIT, ChannelDirectory, ChannelUnavailable
from src.lending.cora_tools import CoraToolkit

CORA, REPLIES, OPS = "C_CORA", "C_REPLIES", "C_OPS"


class FakeSlack:
    """users.conversations over two pages, conversations.history and chat.postMessage, in memory."""

    def __init__(self):
        self.pages = [
            {"channels": [{"id": OPS, "name": "lending-ops", "purpose": {"value": "Urgent lead alarms"}}],
             "response_metadata": {"next_cursor": "page2"}},
            {"channels": [{"id": CORA, "name": "cora", "is_private": True},
                          {"id": REPLIES, "name": "lending-replies", "purpose": {"value": "Handoffs for Josh "}}],
             "response_metadata": {"next_cursor": ""}},
        ]
        self.history: dict[str, list[dict]] = {}
        self.list_calls, self.history_calls, self.posts = 0, [], []
        self.fail_listing = False

    def users_conversations(self, *, types, exclude_archived, limit, cursor=None):
        assert types == "public_channel,private_channel" and exclude_archived
        if self.fail_listing:
            raise RuntimeError("slack down")
        if cursor is None:
            self.list_calls += 1
        return self.pages[1] if cursor == "page2" else self.pages[0]

    def conversations_history(self, *, channel, oldest, limit):
        self.history_calls.append({"channel": channel, "oldest": float(oldest), "limit": limit})
        return {"messages": list(self.history.get(channel, []))}

    def chat_postMessage(self, *, channel, text, unfurl_links, unfurl_media):
        self.posts.append((channel, text))
        return {"ok": True, "ts": "1.0"}


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _directory(slack=None, clock=None) -> ChannelDirectory:
    return ChannelDirectory(slack or FakeSlack(), own_channel_id=CORA,
                            key_ids={"replies": REPLIES, "ops": OPS, "dial_tasks": "C_ELSEWHERE"},
                            clock=clock or Clock())


def test_lists_every_page_of_channels_sorted_by_name():
    channels = _directory().channels()
    assert [(c.id, c.name, c.purpose, c.is_private) for c in channels] == [
        (CORA, "cora", "", True), (OPS, "lending-ops", "Urgent lead alarms", False),
        (REPLIES, "lending-replies", "Handoffs for Josh", False)]


def test_the_list_is_kept_for_ten_minutes_then_refreshed():
    slack, clock = FakeSlack(), Clock()
    directory = _directory(slack, clock)
    directory.channels()
    clock.now += 599
    directory.channels()
    assert slack.list_calls == 1
    clock.now += 2
    directory.channels()
    assert slack.list_calls == 2


def test_a_failed_refresh_keeps_the_earlier_list_and_a_first_failure_is_reported():
    slack, clock = FakeSlack(), Clock()
    directory = _directory(slack, clock)
    assert len(directory.channels()) == 3
    slack.fail_listing, clock.now = True, clock.now + 3600
    assert len(directory.channels()) == 3
    with pytest.raises(ChannelUnavailable):
        _directory(slack).channels()


@pytest.mark.parametrize("reference, expected", [
    ("lending-ops", OPS), ("#Lending-Ops", OPS), ("C_REPLIES", REPLIES), ("replies", REPLIES), ("ops", OPS),
    ("dial_tasks", None),  # a key whose channel Cora is not in
    ("general", None), ("#", None),
])
def test_a_channel_is_found_by_name_id_or_inbox_key(reference, expected):
    channel = _directory().resolve(reference)
    assert (channel.id if channel else None) == expected


def test_reading_returns_oldest_first_with_authors_and_trimmed_text():
    slack = FakeSlack()
    slack.history[OPS] = [  # Slack returns newest first
        {"ts": "300.0", "user": "U_JOSH", "text": "x" * (MESSAGE_TEXT_LIMIT + 50)},
        {"ts": "200.0", "subtype": "channel_join", "user": "U_NEW", "text": "joined"},
        {"ts": "100.0", "bot_id": "B1", "bot_profile": {"name": "Lending Ops"}, "text": "URGENT: lead uncalled"},
    ]
    directory = _directory(slack)
    messages = directory.read(directory.resolve("ops"), hours=6)
    assert [m["author"] for m in messages] == ["bot: Lending Ops", "<@U_JOSH>"]
    assert messages[0]["text"] == "URGENT: lead uncalled"
    assert len(messages[1]["text"]) == MESSAGE_TEXT_LIMIT + 1 and messages[1]["text"].endswith("…")
    call = slack.history_calls[0]
    assert call["limit"] == 50 and time.time() - call["oldest"] == pytest.approx(6 * 3600, abs=60)


def test_reading_never_goes_past_the_caps():
    slack = FakeSlack()
    directory = _directory(slack)
    directory.read(directory.resolve("ops"), hours=500, limit=500)
    call = slack.history_calls[0]
    assert call["limit"] == 50 and time.time() - call["oldest"] == pytest.approx(24 * 3600, abs=60)


# ── the tools ────────────────────────────────────────────────────────────────

def _run(directory, name, raw):
    registry = ToolRegistry(CoraToolkit(engine=None, memory=None, channels=directory).tools(),  # type: ignore[arg-type]
                            gate=None)  # type: ignore[arg-type]
    context = ToolContext(user_id="U_OPS", is_approver=False, channel=CORA, thread_ts="1.0", tool_use_id="t1")
    return registry.execute(name, raw, context)


def test_anyone_can_list_the_channels_and_cora_marks_her_own():
    outcome = _run(_directory(), "list_channels", {})
    assert not outcome.is_error
    assert '"name": "#cora"' in outcome.content and '"is_my_channel": true' in outcome.content


def test_anyone_can_read_a_channel_with_phones_and_emails_masked():
    slack = FakeSlack()
    slack.history[REPLIES] = [{"ts": "1792145692.5", "user": "U_JOSH",
                               "text": "call Marcus at +18135550147 or marcus@example.com"}]
    outcome = _run(_directory(slack), "read_channel", {"channel": "#lending-replies", "hours": 2})
    assert not outcome.is_error
    assert "+18135550147" not in outcome.content and "marcus@example.com" not in outcome.content
    assert '"thread_replies_included": false' in outcome.content and "2026-10-16T" in outcome.content


def test_reading_a_channel_cora_is_not_in_says_she_needs_an_invite():
    outcome = _run(_directory(), "read_channel", {"channel": "general"})
    assert outcome.is_error and "invite me" in outcome.content


def test_without_slack_the_channel_tools_say_so():
    assert "not configured" in _run(None, "list_channels", {}).content
    assert "not configured" in _run(None, "read_channel", {"channel": "ops"}).content
