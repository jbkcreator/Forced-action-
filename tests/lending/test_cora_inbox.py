"""Cora's inbox table and add_to_inbox, against Postgres inside a rolled-back transaction."""
from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from migrations.apply_lending_cora_inbox import apply_to
from packages.agent_core.chatport import FakeChatPort, PostResult
from src.lending.cora_inbox import (
    add_to_inbox,
    add_to_inbox_in_own_session,
    add_to_inbox_safely,
    format_digest,
    surface_new_items,
)


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
