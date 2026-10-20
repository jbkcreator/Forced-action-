"""Cora's inbox table and add_to_inbox, against Postgres inside a rolled-back transaction."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from migrations.apply_lending_cora_inbox import apply_to
from packages.agent_core.chatport import FakeChatPort, PostResult
from packages.agent_core.tools import ToolContext, ToolRegistry
from src.lending.cora_inbox import (
    add_to_inbox,
    add_to_inbox_in_own_session,
    add_to_inbox_safely,
    format_digest,
    format_inbox_context,
    items_on_post,
    surface_new_items,
)
from src.lending.cora_channels import ChannelDirectory
from src.lending.cora_tools import CoraToolkit
from tests.lending.test_cora_channels import FakeSlack


@pytest.fixture
def inbox_db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def _row(db, row_id):
    return db.execute(text("SELECT * FROM lending.cora_inbox WHERE id = :id"), {"id": row_id}).mappings().one()


def test_an_event_is_queued_as_new_with_its_payload(inbox_db):
    row_id = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:42", summary="New partner deal",
                          payload={"partner": "Acme", "close_date": date(2026, 11, 2)}, target_channel="routing")

    row = _row(inbox_db, row_id)
    assert row["status"] == "new"
    assert row["target_channel"] == "routing"
    assert row["summary"] == "New partner deal"
    assert row["payload"] == {"partner": "Acme", "close_date": "2026-11-02"}
    assert row["surfaced_ts"] is None


def test_the_same_event_twice_is_queued_once(inbox_db):
    first = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:42")
    again = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:42")

    assert first is not None
    assert again is None
    count = inbox_db.execute(text("SELECT count(*) FROM lending.cora_inbox WHERE dedup_key = 'partner:42'")).scalar()
    assert count == 1


def test_the_same_key_under_another_event_type_is_a_separate_event(inbox_db):
    assert add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="lead:7") is not None
    assert add_to_inbox(inbox_db, event_type="rate_handoff", dedup_key="lead:7") is not None


@pytest.mark.parametrize("event_type, dedup_key", [("", "partner:1"), ("partner_deal", "  ")])
def test_a_blank_event_type_or_key_is_refused(inbox_db, event_type, dedup_key):
    with pytest.raises(ValueError):
        add_to_inbox(inbox_db, event_type=event_type, dedup_key=dedup_key)


def test_an_unknown_status_is_refused_by_the_table(inbox_db):
    row_id = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:9")
    with pytest.raises(IntegrityError):
        with inbox_db.begin_nested():
            inbox_db.execute(text("UPDATE lending.cora_inbox SET status = 'sent' WHERE id = :id"), {"id": row_id})


def test_one_digest_post_can_list_many_events(inbox_db):
    first = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:1")
    second = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:2")
    surface = text("UPDATE lending.cora_inbox SET status = 'surfaced', surfaced_channel = 'D1', "
                   "surfaced_ts = '1700000000.000100', surfaced_at = now() WHERE id = :id")
    inbox_db.execute(surface, {"id": first})
    inbox_db.execute(surface, {"id": second})
    assert _row(inbox_db, second)["surfaced_ts"] == "1700000000.000100"


# ── the safe ways to add an event ────────────────────────────────────────────

def test_channel_posted_records_when_the_job_posted_it_itself(inbox_db):
    posted = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:3", channel_posted=True)
    held = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:4")
    assert _row(inbox_db, posted)["channel_posted_at"] is not None
    assert _row(inbox_db, held)["channel_posted_at"] is None


def test_a_failed_safe_add_is_rolled_back_alone_and_keeps_the_callers_work(inbox_db):
    kept = add_to_inbox(inbox_db, event_type="partner_deal", dedup_key="partner:5")
    assert add_to_inbox_safely(inbox_db, event_type="x" * 41, dedup_key="too-long") is None  # varchar(40) refuses it
    assert _row(inbox_db, kept)["status"] == "new"  # the session is still usable and the earlier row is there


def test_an_own_session_add_writes_on_its_own_session(inbox_writes):
    assert add_to_inbox_in_own_session(event_type="dnc_not_propagated", dedup_key="c9", target_channel="dial_tasks",
                                       channel_posted=True) == 1
    assert inbox_writes[0]["event_type"] == "dnc_not_propagated" and inbox_writes[0]["channel_posted"] is True


def test_an_own_session_add_never_raises():
    def database_down():
        raise RuntimeError("database unavailable")

    assert add_to_inbox_in_own_session(session_factory=database_down, event_type="x", dedup_key="y") is None


# ── the digest ───────────────────────────────────────────────────────────────

CHANNEL_IDS = {"replies": "C_REPLIES", "ops": "C_OPS"}


class SlackRefuses(FakeChatPort):
    def post(self, **kwargs):
        return PostResult(ok=False, error="channel_not_found")


@pytest.fixture
def digest_db(inbox_db):
    """Only this test's events are 'new': any real ones are set aside inside the rolled-back transaction."""
    inbox_db.execute(text("UPDATE lending.cora_inbox SET status = 'dismissed' WHERE status = 'new'"))
    return inbox_db


def _status(db, *ids):
    return [_row(db, row_id)["status"] for row_id in ids]


def test_the_digest_lists_new_events_oldest_first_and_marks_them_surfaced(digest_db):
    first = add_to_inbox(digest_db, event_type="reply_handoff", dedup_key="m1", summary="Rate question: Marcus",
                         target_channel="replies", channel_posted=True)
    second = add_to_inbox(digest_db, event_type="partner_deal", dedup_key="p1", summary="New partner deal")
    chat = FakeChatPort(default_channel="C_CORA")

    assert surface_new_items(digest_db, chat, channel="C_CORA", channel_ids=CHANNEL_IDS) == 2
    assert len(chat.posts) == 1
    post = chat.posts[0]
    assert post["channel"] == "C_CORA"
    assert post["text"].splitlines() == ["*2 new items*",
                                         f"#{first}  Rate question: Marcus  _(already in <#C_REPLIES>)_",
                                         f"#{second}  New partner deal"]
    for row_id in (first, second):
        row = _row(digest_db, row_id)
        assert (row["status"], row["surfaced_channel"], row["surfaced_ts"]) == ("surfaced", "C_CORA", post["ts"])
        assert row["surfaced_at"] is not None


def test_a_surfaced_event_is_never_listed_again(digest_db):
    add_to_inbox(digest_db, event_type="partner_deal", dedup_key="p2", summary="New partner deal")
    chat = FakeChatPort(default_channel="C_CORA")
    surface_new_items(digest_db, chat, channel="C_CORA", channel_ids=CHANNEL_IDS)
    assert surface_new_items(digest_db, chat, channel="C_CORA", channel_ids=CHANNEL_IDS) == 0
    assert len(chat.posts) == 1


def test_a_refused_digest_leaves_its_events_new_for_the_next_sweep(digest_db):
    row_id = add_to_inbox(digest_db, event_type="partner_deal", dedup_key="p3", summary="New partner deal")
    assert surface_new_items(digest_db, SlackRefuses(), channel="C_CORA", channel_ids=CHANNEL_IDS) == 0
    assert _status(digest_db, row_id) == ["new"]
    assert surface_new_items(digest_db, FakeChatPort(), channel="C_CORA", channel_ids=CHANNEL_IDS) == 1


def test_a_digest_lists_at_most_the_limit_and_the_rest_lead_the_next(digest_db):
    ids = [add_to_inbox(digest_db, event_type="partner_deal", dedup_key=f"p-limit-{n}", summary=f"Deal {n}")
           for n in range(3)]
    chat = FakeChatPort(default_channel="C_CORA")
    assert surface_new_items(digest_db, chat, channel="C_CORA", channel_ids=CHANNEL_IDS, limit=2) == 2
    assert _status(digest_db, *ids) == ["surfaced", "surfaced", "new"]
    assert surface_new_items(digest_db, chat, channel="C_CORA", channel_ids=CHANNEL_IDS, limit=2) == 1
    assert chat.posts[1]["text"].splitlines()[0] == "*1 new item*"


def test_the_digest_escapes_slack_markup_and_names_unknown_channels_by_key():
    text_out = format_digest([
        {"id": 7, "summary": "<!channel> & <http://x|y>", "target_channel": None, "channel_posted": False},
        {"id": 8, "summary": "DNC removal pending", "target_channel": "dial_tasks", "channel_posted": True},
    ], CHANNEL_IDS)
    assert "<!channel>" not in text_out and "&lt;!channel&gt; &amp; &lt;http://x|y&gt;" in text_out
    assert text_out.splitlines()[2] == "#8  DNC removal pending  _(already in dial_tasks)_"


# ── acting on items: what Cora sees, and the dismiss / route tools ───────────

JOSH, OPS = "U_JOSH", "U_OPS"


def _surfaced(db, chat=None):
    """Two items in one digest: a reply handoff already in #replies, and a deal not posted anywhere."""
    handoff = add_to_inbox(db, event_type="reply_handoff", dedup_key="act-m1", summary="Rate question: Marcus",
                           target_channel="replies", channel_posted=True, payload={"phone_last4": "0147"})
    deal = add_to_inbox(db, event_type="partner_deal", dedup_key="act-p1", summary="New partner deal",
                        target_channel="routing")
    chat = chat or FakeChatPort(default_channel="C_CORA")
    surface_new_items(db, chat, channel="C_CORA", channel_ids=CHANNEL_IDS)
    return handoff, deal, chat.posts[-1]["ts"]


class SlackRefusesPosts(FakeSlack):
    def chat_postMessage(self, **kwargs):
        raise RuntimeError("not_in_channel")


def _toolkit(db, slack=None) -> CoraToolkit:
    @contextmanager
    def scope():
        with db.begin_nested():  # a savepoint stands in for the tool's own transaction
            yield db

    channels = None if slack is None else ChannelDirectory(
        slack, own_channel_id="C_CORA", key_ids={"replies": "C_REPLIES", "ops": "C_OPS"})
    return CoraToolkit(engine=None, memory=None, channels=channels, inbox_session=scope)  # type: ignore[arg-type]


def _run(toolkit: CoraToolkit, name: str, raw: dict, user: str = JOSH):
    registry = ToolRegistry(toolkit.tools(), gate=None)  # type: ignore[arg-type]
    context = ToolContext(user_id=user, is_approver=user == JOSH, channel="C_CORA", thread_ts="1.0",
                          tool_use_id="t1")
    return registry.execute(name, raw, context)


def test_a_digest_threads_items_are_found_by_its_post(digest_db):
    handoff, deal, ts = _surfaced(digest_db)
    items = items_on_post(digest_db, "C_CORA", ts)
    assert [item["id"] for item in items] == [handoff, deal]
    assert items_on_post(digest_db, "C_CORA", "999.000000") == []
    context = format_inbox_context(items)
    assert context.startswith("<inbox_items>\n") and context.endswith("\n</inbox_items>")
    assert f'"id": {handoff}' in context and '"already_posted_there": true' in context


def test_an_approver_dismisses_an_item_with_a_note(digest_db):
    handoff, _, _ = _surfaced(digest_db)
    outcome = _run(_toolkit(digest_db), "dismiss_inbox_item", {"item_id": handoff, "note": "called him"})
    assert not outcome.is_error
    row = _row(digest_db, handoff)
    assert (row["status"], row["decided_by"], row["decision_note"]) == ("dismissed", JOSH, "called him")
    assert row["decided_at"] is not None


def test_only_an_approver_can_dismiss_or_route(digest_db):
    handoff, deal, _ = _surfaced(digest_db)
    slack = FakeSlack()
    toolkit = _toolkit(digest_db, slack)
    assert _run(toolkit, "dismiss_inbox_item", {"item_id": handoff}, user=OPS).is_error
    assert _run(toolkit, "route_inbox_item", {"item_id": deal, "channel": "lending-ops"}, user=OPS).is_error
    assert _status(digest_db, handoff, deal) == ["surfaced", "surfaced"] and slack.posts == []
    assert not _run(toolkit, "list_inbox_items", {}, user=OPS).is_error  # anyone may look


def test_an_approver_routes_an_item_to_a_channel_cora_is_in(digest_db):
    _, deal, _ = _surfaced(digest_db)
    slack = FakeSlack()
    outcome = _run(_toolkit(digest_db, slack), "route_inbox_item",
                   {"item_id": deal, "channel": "#lending-ops", "note": "Sarah, please call today"})
    assert not outcome.is_error and "#lending-ops" in outcome.content
    assert slack.posts == [("C_OPS", f"*New partner deal*\nSarah, please call today\n"
                                     f"_Sent by <@{JOSH}> via Cora (inbox #{deal})_")]
    row = _row(digest_db, deal)
    assert (row["status"], row["routed_channel"], row["decided_by"]) == ("routed", "lending-ops", JOSH)


@pytest.mark.parametrize("event", [
    # the payloads booking_webhook and disposition_delivery store: the ids live only here, not in the summary
    {"event_type": "slot_release_failed", "dedup_key": "route-appt-1", "target_channel": "replies",
     "summary": "GHL slot not released after a failed check: cancel it by hand",
     "payload": {"ghl_appointment_id": "route-appt-1"}, "expected": ["ghl appointment id: route-appt-1"]},
    {"event_type": "dnc_removal_pending", "dedup_key": "route-call-9", "target_channel": "dial_tasks",
     "summary": "DNC recorded but not yet removed from the dialer: remove the contact in the dialer by hand",
     "payload": {"dialer_call_id": "route-call-9", "caller_seat": "seat-3"},
     "expected": ["dialer call id: route-call-9", "caller seat: seat-3"]},
], ids=["slot_release_failed", "dnc_removal_pending"])
def test_a_routed_item_carries_the_ids_from_its_payload_without_a_note(digest_db, event):
    expected = event.pop("expected")
    item_id = add_to_inbox(digest_db, **event)
    slack = FakeSlack()
    outcome = _run(_toolkit(digest_db, slack), "route_inbox_item", {"item_id": item_id, "channel": "lending-ops"})
    assert not outcome.is_error
    lines = slack.posts[0][1].splitlines()
    assert lines[0] == f"*{event['summary']}*" and sorted(lines[1:-1]) == sorted(expected)  # jsonb reorders keys
    assert lines[-1] == f"_Sent by <@{JOSH}> via Cora (inbox #{item_id})_"
    assert _row(digest_db, item_id)["status"] == "routed"


def test_routed_details_skip_empty_values_and_escape_slack_markup():
    from src.lending.cora_tools import _routed_post

    item = {"id": 7, "summary": "Opt-out email not recorded",
            "payload": {"message_key": "<!channel>", "sender_masked": "a***@example.com", "error": None,
                        "attempts": 5, "phones": ["…0147"]}}
    assert _routed_post(item, None, JOSH).splitlines() == [
        "*Opt-out email not recorded*", "message key: &lt;!channel&gt;", "sender masked: a***@example.com",
        "attempts: 5", 'phones: ["…0147"]', f"_Sent by <@{JOSH}> via Cora (inbox #7)_"]


def test_an_inbox_channel_key_still_works_as_a_channel_name(digest_db):
    _, deal, _ = _surfaced(digest_db)
    slack = FakeSlack()
    assert not _run(_toolkit(digest_db, slack), "route_inbox_item", {"item_id": deal, "channel": "ops"}).is_error
    assert slack.posts[0][0] == "C_OPS"


def test_a_refused_post_leaves_the_item_open(digest_db):
    _, deal, _ = _surfaced(digest_db)
    outcome = _run(_toolkit(digest_db, SlackRefusesPosts()), "route_inbox_item",
                   {"item_id": deal, "channel": "lending-ops"})
    assert outcome.is_error and "still open" in outcome.content
    assert _status(digest_db, deal) == ["surfaced"]


def test_a_closed_item_cannot_be_acted_on_again(digest_db):
    handoff, _, _ = _surfaced(digest_db)
    toolkit = _toolkit(digest_db, FakeSlack())
    _run(toolkit, "dismiss_inbox_item", {"item_id": handoff})
    again = _run(toolkit, "route_inbox_item", {"item_id": handoff, "channel": "lending-ops"})
    assert again.is_error and "already dismissed" in again.content
    assert _row(digest_db, handoff)["routed_channel"] is None


def test_routing_is_refused_to_the_channel_the_item_is_already_in(digest_db):
    handoff, _, _ = _surfaced(digest_db)  # already posted to the replies channel by its job
    slack = FakeSlack()
    outcome = _run(_toolkit(digest_db, slack), "route_inbox_item", {"item_id": handoff, "channel": "lending-replies"})
    assert outcome.is_error and "already in #lending-replies" in outcome.content and slack.posts == []


def test_routing_is_refused_to_cora_s_own_channel_one_she_is_not_in_or_without_slack(digest_db):
    _, deal, _ = _surfaced(digest_db)
    slack = FakeSlack()
    own = _run(_toolkit(digest_db, slack), "route_inbox_item", {"item_id": deal, "channel": "cora"})
    assert own.is_error and "my own channel" in own.content
    absent = _run(_toolkit(digest_db, slack), "route_inbox_item", {"item_id": deal, "channel": "general"})
    assert absent.is_error and "invite me" in absent.content
    unconfigured = _run(_toolkit(digest_db, None), "route_inbox_item", {"item_id": deal, "channel": "ops"})
    assert unconfigured.is_error and "not configured" in unconfigured.content
    assert _status(digest_db, deal) == ["surfaced"] and slack.posts == []


def test_an_unknown_item_number_is_reported(digest_db):
    outcome = _run(_toolkit(digest_db), "list_inbox_items", {"item_id": 987654321})
    assert outcome.is_error and "no inbox item #987654321" in outcome.content
