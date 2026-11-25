"""R1 1.3: "Today's list" in Cora's morning digest, read from real rows (SQLite with an attached ``lending`` schema,
in-memory chat; no Slack, no shared database)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from packages.agent_core.chatport import FakeChatPort
from src.lending.cora_morning_digest import MorningListConfig, digest_day, post_morning_digest
from src.lending.morning_list.read import LinkConfig

# 2026-11-25 is EST (UTC-5): 12:47 UTC is 7:47 AM ET.
AT_7_46 = datetime(2026, 11, 25, 12, 46, tzinfo=timezone.utc)
AT_7_47 = datetime(2026, 11, 25, 12, 47, tzinfo=timezone.utc)

_DDL = (
    "CREATE TABLE lending.cora_morning_digests (digest_date DATE PRIMARY KEY, slack_channel TEXT, slack_ts TEXT,"
    " posted_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE lending.pending_actions (action_id INTEGER PRIMARY KEY, tool_name TEXT, summary TEXT, status TEXT,"
    " created_at TIMESTAMP)",
    "CREATE TABLE lending.booking_messages (booking_ref TEXT, first_name TEXT, contact_phone TEXT, booked_by TEXT,"
    " slot_start_utc TIMESTAMP)",
    "CREATE TABLE lending.cora_inbox (id INTEGER PRIMARY KEY, summary TEXT, payload TEXT, event_type TEXT,"
    " dedup_key TEXT, status TEXT)",
    "CREATE TABLE lending.file_routing (id INTEGER PRIMARY KEY, status TEXT, is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.confirmation_tasks (booking_ref TEXT, due_date DATE, completed_at TIMESTAMP,"
    " cancelled_at TIMESTAMP)",
    "CREATE TABLE lending.uncalled_alarms (id INTEGER PRIMARY KEY, lendingflow_lead_id INTEGER, web_lead_id INTEGER)",
    "CREATE TABLE lending.lendingflow_leads (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, phone TEXT,"
    " email TEXT, ghl_contact_id TEXT, loan_type TEXT, loan_amount INTEGER, loan_amount_range TEXT, property_city TEXT,"
    " property_state TEXT, received_at TIMESTAMP, handled_in_ghl_at TIMESTAMP, suppressed BOOLEAN NOT NULL DEFAULT 0,"
    " is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.lendingflow_enrichment (lead_id INTEGER, routing_tag TEXT, closer_priority BOOLEAN,"
    " fit TEXT)",
    "CREATE TABLE lending.web_leads (id INTEGER PRIMARY KEY, name TEXT, phone TEXT, email TEXT, ghl_contact_id TEXT, deal_type TEXT,"
    " loan_amount INTEGER, property_city TEXT, property_state TEXT, credit_band TEXT, completed_projects_3y TEXT,"
    " received_at TIMESTAMP, handled_in_ghl_at TIMESTAMP, ghl_status TEXT, suppressed BOOLEAN NOT NULL DEFAULT 0,"
    " is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.dialer_load_records (id INTEGER PRIMARY KEY, pool TEXT, campaign_tag TEXT, phone TEXT,"
    " ghl_contact_id TEXT, dialer_contact_id TEXT,"
    " borrower_name TEXT, entity_name TEXT, property_address TEXT, estimated_loan_value INTEGER, active BOOLEAN)",
    "CREATE TABLE lending.contacts (phone TEXT PRIMARY KEY, line_type TEXT, email TEXT,"
    " do_not_contact BOOLEAN NOT NULL DEFAULT 0, is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.suppression_list (phone TEXT, email TEXT)",
    "CREATE TABLE lending.call_dispositions (id INTEGER PRIMARY KEY, phone TEXT, caller_name TEXT, disposition TEXT,"
    " dialer_contact_id TEXT,"
    " call_started_at TIMESTAMP, created_at TIMESTAMP)",
    "CREATE TABLE lending.loan_files (id INTEGER PRIMARY KEY, borrower_first_name TEXT, borrower_last_name TEXT,"
    " borrower_phone TEXT, borrower_email TEXT, product TEXT, loan_amount INTEGER, property_city TEXT, stage TEXT,"
    " status_at TIMESTAMP, closed_at TIMESTAMP, is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.clay_enrichments (lead_ref TEXT, field TEXT, status TEXT, value TEXT)",
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


def _seed(engine) -> None:
    day_ago = AT_7_47 - timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO lending.lendingflow_leads (id, first_name, last_name, phone, email, loan_type,"
                          " loan_amount_range, property_city, received_at) VALUES"
                          " (1, 'Ann', 'Lee', '+18135551111', 'ann@x.com', 'Fix and flip', '$200k-$300k', 'Tampa', :t),"
                          " (2, 'Old', 'Lead', '+18135552222', NULL, 'DSCR', NULL, 'Miami', :old),"
                          " (3, 'Land', 'Line', '+18135553333', NULL, 'DSCR', NULL, 'Miami', :t)"),
                     {"t": day_ago, "old": AT_7_47 - timedelta(days=20)})
        conn.execute(text("INSERT INTO lending.loan_files (id, borrower_first_name, borrower_last_name, borrower_phone,"
                          " product, loan_amount, stage, status_at) VALUES"
                          " (5, 'Deal', 'Maker', '+18135554444', 'flip', 300000, 'SUBMITTED', :t),"
                          " (6, 'Done', 'Deal', '+18135555555', 'flip', 300000, 'FUNDED', :t)"),
                     {"t": AT_7_47 - timedelta(days=3)})
        conn.execute(text("INSERT INTO lending.call_dispositions (id, phone, caller_name, disposition, call_started_at)"
                          " VALUES (9, '+18135556666', 'David Willis', 'CALLBACK_REQUESTED', :t)"), {"t": day_ago})
        conn.execute(text("INSERT INTO lending.dialer_load_records (id, pool, phone, borrower_name, entity_name,"
                          " property_address, estimated_loan_value, active) VALUES"
                          " (20, 'partners', '+17275550001', NULL, 'Bay Lending LLC', '1 Main St, TAMPA FL 33634', NULL, 1),"
                          " (21, 'verified_maturity', '+17275550002', 'Bo Fox', 'Fox LLC', '2 Oak Ave, MIAMI', 400000, 1),"
                          " (22, 'partners', '+17275550003', NULL, 'Land LLC', NULL, NULL, 1)"))
        conn.execute(text("INSERT INTO lending.contacts (phone, line_type) VALUES ('+18135551111', 'Mobile'),"
                          " ('+18135552222', 'Mobile'), ('+18135553333', 'Landline'), ('+18135556666', 'Mobile'),"
                          " ('+17275550001', 'Mobile'), ('+17275550002', 'Mobile'), ('+17275550003', 'Landline')"))
        conn.execute(text("INSERT INTO lending.clay_enrichments VALUES ('dlr:20', 'email', 'found', 'pat@baylending.com')"))
        conn.execute(text("UPDATE lending.lendingflow_leads SET ghl_contact_id = 'G1' WHERE id = 1"))
        conn.execute(text("UPDATE lending.dialer_load_records SET dialer_contact_id = 'B21' WHERE id = 21"))


def _config(size: int = 50) -> MorningListConfig:
    return MorningListConfig(size=size, lead_days=14, david_user_id="U_DAVID",
                             links=LinkConfig(ghl_app_url="https://app.gohighlevel.com", ghl_location_id="LOC"),
                             stuck_deals=lambda db, now: [
        SimpleNamespace(opportunity_id="o1", stage_name="Submitted to lender", days=5, first_name="Stu",
                        phone="+18135557777", email=None, owner=SimpleNamespace(value="lender"))])


def _text(chat: FakeChatPort) -> str:
    return "\n".join(str(block) for block in chat.posts[0]["blocks"])


def test_digest_is_due_at_7_47_et_by_default() -> None:
    assert digest_day(AT_7_46) is None
    assert digest_day(AT_7_47) == AT_7_47.date()


def test_list_on_ranks_real_rows_and_replaces_the_lead_and_call_sections(engine) -> None:
    _seed(engine)
    chat = FakeChatPort(default_channel="C_CORA")
    listed = []
    assert post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_47, morning_list=_config(),
                               on_listed=listed.extend)
    body = _text(chat)
    assert "Josh · 2" in body and "David · 4" in body and "<@U_DAVID>" in body
    assert "New leads, last 24 hours" not in body and "Today's calls" not in body
    assert [i.ref for i in listed] == ["opp:o1", "loan:5", "cb:9", "lf:1", "dlr:20", "dlr:21"]
    assert "stuck 5 days" in body and "waiting on lender" in body
    thread = chat.posts[1]
    assert thread["thread_ts"] == chat.posts[0]["ts"]
    reply = "\n".join(b["text"]["text"] for b in thread["blocks"])
    assert "*Callbacks · 1*" in reply and "*New leads · 1*" in reply and "*Top of queue · Partners · 1*" in reply
    assert "email found" in reply  # Clay found dlr:20's email
    assert "<https://app.gohighlevel.com/v2/location/LOC/contacts/detail/G1|GHL ↗>" in reply
    assert "<https://app.batchdialer.com/contacts/view/B21|BatchDialer ↗>" in reply
    everything = body + reply
    assert "8135551111" not in everything and "ann@x.com" not in everything
    assert "pat@baylending.com" not in everything


def test_list_size_caps_padding(engine) -> None:
    _seed(engine)
    chat = FakeChatPort(default_channel="C_CORA")
    listed = []
    post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_47, morning_list=_config(size=5),
                        on_listed=listed.extend)
    assert [i.ref for i in listed] == ["opp:o1", "loan:5", "cb:9", "lf:1", "dlr:20"]


def test_list_off_keeps_the_digest_as_it_was(engine) -> None:
    _seed(engine)
    chat = FakeChatPort(default_channel="C_CORA")
    assert post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_47)
    body = _text(chat)
    assert "Today's list" not in body and "New leads, last 24 hours" in body


def test_a_tier_that_cannot_be_read_is_named_and_the_rest_still_posts(engine) -> None:
    _seed(engine)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE lending.loan_files"))
    chat = FakeChatPort(default_channel="C_CORA")
    listed = []
    assert post_morning_digest(engine, chat, "C_CORA", halted=False, now=AT_7_47, morning_list=_config(),
                               on_listed=listed.extend)
    unavailable = next(line for line in _text(chat).split("\\n") if "Not available today" in line)
    assert "active deals" in unavailable
    assert "loan:5" not in [i.ref for i in listed] and "lf:1" in [i.ref for i in listed]


def test_padding_follows_the_dial_order_not_insert_order(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO lending.dialer_load_records (id, pool, phone, borrower_name, active) VALUES"
                          " (1, 'verified_maturity', '+17275550011', 'Mat One', 1),"
                          " (2, 'partners', '+17275550012', 'Pat Two', 1)"))
        conn.execute(text("INSERT INTO lending.contacts (phone, line_type) VALUES ('+17275550011', 'Mobile'),"
                          " ('+17275550012', 'Mobile')"))
    listed = []
    config = MorningListConfig(size=1, lead_days=14, david_user_id="", stuck_deals=lambda db, now: [])
    post_morning_digest(engine, FakeChatPort(default_channel="C"), "C", halted=False, now=AT_7_47,
                        morning_list=config, on_listed=listed.extend)
    assert [i.ref for i in listed] == ["dlr:2"]


def test_batched_text_consent_matches_the_single_check(engine) -> None:
    from src.lending.consent import has_text_consent, text_consented_phones

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE lending.text_consents (phone TEXT, revoked_at TIMESTAMP)"))
        conn.execute(text("INSERT INTO lending.text_consents VALUES ('+18135550001', NULL),"
                          " ('+18135550003', '2026-11-01')"))
        conn.execute(text("INSERT INTO lending.lendingflow_leads (id, phone, received_at) VALUES"
                          " (1, '+18135550002', '2026-11-20'), (2, '+18135550003', '2026-11-20'),"
                          " (3, '+18135550004', '2026-11-20')"))
        conn.execute(text("INSERT INTO lending.suppression_list (phone) VALUES ('+18135550004')"))
    phones = [f"+1813555000{n}" for n in range(1, 6)]
    with engine.connect() as conn:
        single = {p for p in phones if has_text_consent(conn, p)}
        assert text_consented_phones(conn, phones) == single == {"+18135550001", "+18135550002"}
