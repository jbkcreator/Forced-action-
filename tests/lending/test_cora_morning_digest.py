"""Cora's 7:30 AM morning digest: when it is due, posting once a day, retry on a failed post, and what it shows.
SQLite with an attached ``lending`` schema and an in-memory chat: no Slack, no shared database."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from packages.agent_core.chatport import FakeChatPort, PostResult
from src.lending import cora_agent
from src.lending.cora_morning_digest import DigestData, digest_day, post_morning_digest, render

# 2026-10-19 is EDT (UTC-4): 11:30 UTC is 7:30 AM ET.
AT_7_29 = datetime(2026, 10, 19, 11, 29, tzinfo=timezone.utc)
AT_7_30 = datetime(2026, 10, 19, 11, 30, tzinfo=timezone.utc)
AT_11_59 = datetime(2026, 10, 19, 15, 59, tzinfo=timezone.utc)
AT_NOON = datetime(2026, 10, 19, 16, 0, tzinfo=timezone.utc)

_DDL = (
    "CREATE TABLE lending.cora_morning_digests (digest_date DATE PRIMARY KEY, slack_channel TEXT, slack_ts TEXT,"
    " posted_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE lending.pending_actions (action_id INTEGER PRIMARY KEY, tool_name TEXT, summary TEXT,"
    " contact_ref TEXT, status TEXT, created_at TIMESTAMP)",
    "CREATE TABLE lending.booking_messages (booking_ref TEXT, first_name TEXT, booked_by TEXT, slot_start_utc TIMESTAMP)",
    "CREATE TABLE lending.web_leads (id INTEGER PRIMARY KEY, received_at TIMESTAMP, ghl_status TEXT, is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.cora_inbox (id INTEGER PRIMARY KEY, summary TEXT, status TEXT)",
    "CREATE TABLE lending.file_routing (id INTEGER PRIMARY KEY, status TEXT, is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.confirmation_tasks (booking_ref TEXT, due_date DATE, completed_at TIMESTAMP,"
    " cancelled_at TIMESTAMP)",
    "CREATE TABLE lending.lendingflow_leads (id INTEGER PRIMARY KEY, first_name TEXT, loan_type TEXT,"
    " loan_amount_range TEXT, property_city TEXT, received_at TIMESTAMP, suppressed BOOLEAN, is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.lendingflow_enrichment (lead_id INTEGER, routing_tag TEXT, closer_priority BOOLEAN)",
    "CREATE TABLE lending.dialer_load_records (id INTEGER PRIMARY KEY, campaign_tag TEXT, active BOOLEAN)",
)


@pytest.fixture
def engine():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with eng.begin() as conn:
        for statement in _DDL:
            conn.execute(text(statement))
    yield eng
    eng.dispose()


class FailingChat(FakeChatPort):
    def post(self, **kwargs) -> PostResult:
        return PostResult(ok=False, error="channel_not_found")


def _rows(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT digest_date, slack_ts FROM lending.cora_morning_digests")).all()


@pytest.mark.parametrize("now,due", [(AT_7_29, None), (AT_7_30, date(2026, 10, 19)), (AT_11_59, date(2026, 10, 19)),
                                     (AT_NOON, None)])
def test_due_from_7_30_until_noon_et(now, due) -> None:
    assert digest_day(now) == due


def test_due_at_7_30_et_in_winter_time_too() -> None:
    assert digest_day(datetime(2026, 12, 1, 12, 30, tzinfo=timezone.utc)) == date(2026, 12, 1)  # EST, UTC-5
    assert digest_day(datetime(2026, 12, 1, 11, 30, tzinfo=timezone.utc)) is None


def test_posts_once_a_day_to_coras_channel(engine) -> None:
    chat = FakeChatPort(default_channel="C_CORA")
    assert post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_30)
    assert not post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_30)
    assert not post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_11_59)
    assert len(chat.posts) == 1 and chat.posts[0]["channel"] == "C_CORA"
    assert _rows(engine) == [("2026-10-19", chat.posts[0]["ts"])]


def test_nothing_is_posted_before_7_30_or_after_noon(engine) -> None:
    chat = FakeChatPort(default_channel="C_CORA")
    assert not post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_29)
    assert not post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_NOON)
    assert chat.posts == [] and _rows(engine) == []


def test_a_failed_post_is_retried_next_sweep(engine) -> None:
    with pytest.raises(RuntimeError):
        post_morning_digest(engine, FailingChat(default_channel="C_CORA"), "C_CORA", halted=False, now=AT_7_30)
    assert _rows(engine) == []
    chat = FakeChatPort(default_channel="C_CORA")
    assert post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_30)
    assert len(chat.posts) == 1


def test_no_channel_means_no_post(engine) -> None:
    assert not post_morning_digest(engine, FakeChatPort(), None, halted=False, now=AT_7_30)


def _body(blocks) -> str:
    return " ".join(block["text"]["text"] for block in blocks if block["type"] == "section")


def test_digest_follows_the_e5_morning_brief(engine) -> None:
    """Chunk 2 spec, Task E5: overdue actions, new high-score leads, today's calling queues."""
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO lending.pending_actions VALUES"
                          " (4, 'draft_email', 'Email to Maria <maria@example.com>: Your loan', 'web_lead:7', 'pending',"
                          " '2026-10-18 11:30:00'),"
                          " (5, 'draft_sms', 'Text to Sent (1111)', NULL, 'sent', '2026-10-19 01:00:00')"))
        conn.execute(text("INSERT INTO lending.confirmation_tasks VALUES ('bk-1', '2026-10-17', NULL, NULL),"
                          " ('bk-2', '2026-10-20', NULL, NULL), ('bk-3', '2026-10-18', '2026-10-18 10:00:00', NULL)"))
        conn.execute(text("INSERT INTO lending.booking_messages VALUES ('bk-1', 'Dana', 'ai', '2026-10-27 18:00:00'),"
                          " ('bk-1', 'Dana', 'ai', '2026-10-27 18:00:00')"))
        conn.execute(text("INSERT INTO lending.cora_inbox VALUES (1, 'Lead did not reach GoHighLevel', 'new'),"
                          " (2, 'Closed one', 'dismissed')"))
        conn.execute(text("INSERT INTO lending.lendingflow_leads VALUES"
                          " (10, 'Ana', 'Fix & Flip', '$250K - $500K', 'Tampa', '2026-10-19 02:00:00', 0, 0),"
                          " (11, 'Ben', 'DSCR', NULL, NULL, '2026-10-19 03:00:00', 0, 0),"
                          " (12, 'Cy', 'DSCR', NULL, NULL, '2026-10-19 04:00:00', 1, 0),"
                          " (13, 'Old', 'DSCR', NULL, NULL, '2026-10-01 04:00:00', 0, 0),"
                          " (14, 'Qa', 'DSCR', NULL, NULL, '2026-10-19 05:00:00', 0, 1)"))
        conn.execute(text("INSERT INTO lending.lendingflow_enrichment VALUES (10, 'FULL_MACHINE', 0), (11, 'NURTURE', 1)"))
        conn.execute(text("INSERT INTO lending.web_leads VALUES (1, '2026-10-19 02:00:00', 'synced', 0),"
                          " (2, '2026-10-19 03:00:00', 'failed', 0), (3, '2026-10-01 03:00:00', 'synced', 0),"
                          " (4, '2026-10-19 04:00:00', 'synced', 1)"))
        conn.execute(text("INSERT INTO lending.dialer_load_records VALUES (1, 'maturity', 1), (2, 'maturity', 1),"
                          " (3, 'permits', 1), (4, 'permits', 0)"))
        conn.execute(text("INSERT INTO lending.file_routing VALUES (1, 'held', 0), (2, 'held', 0), (3, 'exhausted', 0),"
                          " (4, 'held', 1), (5, 'routed', 0)"))
    chat = FakeChatPort(default_channel="C_CORA")
    post_morning_digest(engine, chat, "C_CORA", halted=True, halt_reason="halted from Slack by U_JOSH", now=AT_7_30,
                        approvals_channel="C_APPROVALS")
    body = _body(chat.posts[0]["blocks"])
    assert chat.posts[0]["text"] == "Next Deal Lending morning update: Monday, October 19"
    assert "Cora is paused (halted from Slack by U_JOSH)" in body
    assert "*Messages waiting for your approval: 1*  → approve in <#C_APPROVALS>" in body
    assert "• Email to Maria: Your loan. Waiting 1 day." in body
    assert "maria@example.com" not in body and "Text to Sent" not in body
    assert "*Confirmation calls to make: 1*" in body
    assert "• Dana: call on Tue, Oct 27 at 2:00 PM. Late by 2 days." in body
    assert "bk-" not in body
    assert "*Open items in Cora's inbox: 1*" in body and "• Lead did not reach GoHighLevel" in body
    assert "New leads, last 24 hours: 4" in body  # 2 LendingFlow (suppressed and old ones left out) + 2 web
    assert body.index("Ben") < body.index("Ana")  # closer priority first
    assert ":star: Ana: Fix &amp; Flip, $250K - $500K, Tampa" in body  # Slack shows "&"
    assert "Cy" not in body and "Old" not in body and "lendingflow_lead" not in body
    assert "• 2 website leads, 1 not yet sent to GoHighLevel" in body
    assert "• Maturity: 2 contacts" in body and "• Permits: 1 contact" in body
    assert "Loan files with no lender yet: 2" in body and "1 more file where every lender declined." in body
    assert "Draft #" not in body and "contact:" not in body


def _empty(**overrides) -> DigestData:
    values = dict(halted=False, halt_reason="", pending_drafts=[], pending_total=0, due_confirmations=[],
                  due_confirmations_total=0, open_inbox=0, new_leads=[], new_leads_total=0, web_leads=0,
                  web_leads_not_in_ghl=0, todays_calls=[], dialer_queues=[])
    values.update(overrides)
    return DigestData(**values)


def test_render_lists_todays_calls_in_et() -> None:
    header, blocks = render(date(2026, 10, 19), _empty(todays_calls=[
        {"booking_ref": "b1", "first_name": "Sam", "booked_by": "ai",
         "slot_start": datetime(2026, 10, 19, 14, 0, tzinfo=timezone.utc)}]))
    body = _body(blocks)
    assert header == "Next Deal Lending morning update: Monday, October 19"
    assert "Today's calls: 1 booked" in body and "• 10:00 AM: Sam (booked by AI)" in body
    assert "Cora is running normally." in body and "All times Eastern." in body


def test_drafts_show_how_long_they_have_waited_and_a_plain_label() -> None:
    now = datetime(2026, 10, 19, 11, 30, tzinfo=timezone.utc)

    def draft(summary, tool, hours):
        return {"action_id": 8, "tool_name": tool, "summary": summary, "total": 3,
                "created_at": now - timedelta(hours=hours)}

    drafts = [draft("After-hours voicemail reply to Maria (…8004), drafted from the caller's voicemail; sends "
                    "8am-8pm borrower time", "after_hours_reply", 30),
              draft("", "after_hours_reply", 0), draft("", "ghl_stage", 5)]
    body = _body(render(date(2026, 10, 19), _empty(pending_drafts=drafts, pending_total=3), now=now)[1])
    assert "• After-hours voicemail reply to Maria (…8004). Waiting 1 day." in body
    assert "• After-hours voicemail reply. Waiting less than an hour." in body
    assert "• Ghl stage. Waiting 5 hours." in body
    assert "after_hours_reply" not in body and "#8" not in body


def test_confirmation_calls_say_due_today_or_how_late() -> None:
    slot = datetime(2026, 10, 19, 18, 30, tzinfo=timezone.utc)
    tasks = [{"booking_ref": "b1", "due_date": date(2026, 10, 19), "first_name": "Sam", "slot_start": slot},
             {"booking_ref": "b2", "due_date": date(2026, 10, 18), "first_name": None, "slot_start": None}]
    body = _body(render(date(2026, 10, 19), _empty(due_confirmations=tasks, due_confirmations_total=2))[1])
    assert "• Sam: call on Mon, Oct 19 at 2:30 PM. Due today." in body
    assert "• Booked caller: confirmation call. Late by 1 day." in body
    assert "b1" not in body and "b2" not in body


def test_approvals_without_a_channel_point_to_the_approvals_channel() -> None:
    drafts = [{"action_id": 1, "tool_name": "draft_sms", "summary": "Text to Sam (1234)", "total": 1,
               "created_at": None}]
    body = _body(render(date(2026, 10, 19), _empty(pending_drafts=drafts, pending_total=1))[1])
    assert "approve in the approvals channel" in body and "• Text to Sam (1234)." in body


def test_inbox_preview_is_three_items_and_the_rest_counted() -> None:
    items = [{"summary": f"Item {n}", "total": 16} for n in range(3)]
    body = _body(render(date(2026, 10, 19), _empty(open_inbox=16, inbox_items=items))[1])
    assert "Open items in Cora's inbox: 16" in body and "• Item 2" in body and "…and 13 more" in body


def test_lead_and_booking_fields_cannot_inject_slack_mentions_or_links() -> None:
    lead = {"id": 12, "first_name": "<!channel>", "loan_type": "<https://evil.example|Fix & Flip>",
            "loan_amount_range": "<!here>", "property_city": "<@U_JOSH>", "routing_tag": None, "closer_priority": False}
    body = _body(render(date(2026, 10, 19), _empty(
        new_leads=[lead], new_leads_total=1,
        due_confirmations=[{"booking_ref": "b1", "due_date": date(2026, 10, 19), "first_name": "<!everyone>",
                            "slot_start": None}], due_confirmations_total=1,
        todays_calls=[{"booking_ref": "b1", "first_name": "<!channel>", "booked_by": "<https://evil.example|ai>",
                       "slot_start": datetime(2026, 10, 19, 14, 0, tzinfo=timezone.utc)}]))[1])
    for token in ("<!channel>", "<!here>", "<!everyone>", "<@U_JOSH>", "<https://evil.example"):
        assert token not in body
    assert "• &lt;!channel&gt;: &lt;https://evil.example|Fix &amp; Flip&gt;" in body


def test_an_empty_morning_says_so() -> None:
    body = _body(render(date(2026, 10, 19), _empty())[1])
    for line in ("Nothing needs your action.", "No new leads.", "No calls booked today.", "No contacts loaded.",
                 "All loan files are routed."):
        assert line in body


def test_sweep_posts_the_digest_and_still_sends_when_it_fails() -> None:
    calls = []

    class Relay:
        def run(self):
            calls.append("relay")
            return type("R", (), {"sent": (), "failed": (), "blocked": ()})()

    def broken_digest():
        calls.append("digest")
        raise RuntimeError("slack down")

    session = type("S", (), {"expire_stale_actions": lambda self: 0})()
    gate = type("G", (), {"retry_missing_cards": lambda self: 0})()
    cora_agent.run_sweep(cora_agent.CoraServices(session=session, relay=Relay(), gate=gate,
                                                 morning_digest=broken_digest))
    assert calls == ["digest", "relay"]


def test_a_missing_table_blanks_only_its_section(engine) -> None:
    """A table whose migration has not run on the server must not stop the whole digest."""
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE lending.lendingflow_enrichment"))
        conn.execute(text("INSERT INTO lending.pending_actions VALUES (4, 'draft_email', NULL, 'web_lead:7', 'pending',"
                          " '2026-10-19 01:00:00')"))
    chat = FakeChatPort(default_channel="C_CORA")
    assert post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_30)
    body = _body(chat.posts[0]["blocks"])
    assert "Not available today: LendingFlow leads" in body
    assert "• Draft email. Waiting" in body
    assert _rows(engine) == [("2026-10-19", chat.posts[0]["ts"])]
