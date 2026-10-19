"""Cora's 7:30 AM morning digest: when it is due, posting once a day, retry on a failed post, and what it shows.
SQLite with an attached ``lending`` schema and an in-memory chat: no Slack, no shared database."""
from __future__ import annotations

from datetime import date, datetime, timezone

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
    "CREATE TABLE lending.pending_actions (action_id INTEGER PRIMARY KEY, tool_name TEXT, contact_ref TEXT,"
    " status TEXT, created_at TIMESTAMP)",
    "CREATE TABLE lending.booking_messages (booking_ref TEXT, first_name TEXT, booked_by TEXT, slot_start_utc TIMESTAMP)",
    "CREATE TABLE lending.web_leads (id INTEGER PRIMARY KEY, received_at TIMESTAMP, ghl_status TEXT)",
    "CREATE TABLE lending.cora_inbox (id INTEGER PRIMARY KEY, status TEXT)",
    "CREATE TABLE lending.confirmation_tasks (booking_ref TEXT, due_date DATE, completed_at TIMESTAMP,"
    " cancelled_at TIMESTAMP)",
    "CREATE TABLE lending.lendingflow_leads (id INTEGER PRIMARY KEY, first_name TEXT, loan_type TEXT,"
    " loan_amount_range TEXT, property_city TEXT, received_at TIMESTAMP, suppressed BOOLEAN)",
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
        conn.execute(text("INSERT INTO lending.pending_actions VALUES (4, 'draft_email', 'web_lead:7', 'pending',"
                          " '2026-10-19 01:00:00'), (5, 'draft_sms', NULL, 'sent', '2026-10-19 01:00:00')"))
        conn.execute(text("INSERT INTO lending.confirmation_tasks VALUES ('bk-1', '2026-10-18', NULL, NULL),"
                          " ('bk-2', '2026-10-20', NULL, NULL), ('bk-3', '2026-10-18', '2026-10-18 10:00:00', NULL)"))
        conn.execute(text("INSERT INTO lending.cora_inbox VALUES (1, 'new'), (2, 'dismissed')"))
        conn.execute(text("INSERT INTO lending.lendingflow_leads VALUES"
                          " (10, 'Ana', 'Fix & Flip', '$250K - $500K', 'Tampa', '2026-10-19 02:00:00', 0),"
                          " (11, 'Ben', 'DSCR', NULL, NULL, '2026-10-19 03:00:00', 0),"
                          " (12, 'Cy', 'DSCR', NULL, NULL, '2026-10-19 04:00:00', 1),"
                          " (13, 'Old', 'DSCR', NULL, NULL, '2026-10-01 04:00:00', 0)"))
        conn.execute(text("INSERT INTO lending.lendingflow_enrichment VALUES (10, 'FULL_MACHINE', 0), (11, 'NURTURE', 1)"))
        conn.execute(text("INSERT INTO lending.web_leads VALUES (1, '2026-10-19 02:00:00', 'synced'),"
                          " (2, '2026-10-19 03:00:00', 'failed'), (3, '2026-10-01 03:00:00', 'synced')"))
        conn.execute(text("INSERT INTO lending.dialer_load_records VALUES (1, 'maturity', 1), (2, 'maturity', 1),"
                          " (3, 'permits', 1), (4, 'permits', 0)"))
    chat = FakeChatPort(default_channel="C_CORA")
    post_morning_digest(engine, chat, "C_CORA", halted=True, halt_reason="halted from Slack by U_JOSH", now=AT_7_30)
    body = _body(chat.posts[0]["blocks"])
    assert "Halted (halted from Slack by U_JOSH)" in body
    assert "Draft #4 draft_email for web_lead:7 waiting for approval" in body and "#5" not in body
    assert "Confirmation call for booking bk-1 due 2026-10-18" in body
    assert "bk-2" not in body and "bk-3" not in body
    assert "1 open inbox item(s)" in body
    assert "New leads, last 24 hours (4)" in body  # 2 LendingFlow (suppressed and old ones left out) + 2 web
    assert body.index("lendingflow_lead:11 Ben") < body.index("lendingflow_lead:10 Ana")  # closer priority first
    assert ":star: lendingflow_lead:10 Ana (Fix & Flip, $250K - $500K, Tampa)" in body
    assert "Cy" not in body and "Old" not in body
    assert "2 web lead(s), 1 not in GHL yet" in body
    assert "Dialer maturity: 2 contact(s) loaded" in body and "Dialer permits: 1 contact(s) loaded" in body


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
    assert header == "Good morning. Cora's digest for Monday October 19"
    assert "Booked calls today: 1" in body and "10:00 AM Sam (ai)" in body and "Running" in body


def test_an_empty_morning_says_so() -> None:
    body = _body(render(date(2026, 10, 19), _empty())[1])
    assert "Nothing overdue." in body and "No new leads." in body and "Booked calls today: 0" in body


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
