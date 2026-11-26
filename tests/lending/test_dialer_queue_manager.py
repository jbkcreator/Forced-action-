"""R1 1.6: the 6 AM dialer queue readiness check, on real rows (SQLite with an attached ``lending`` schema)."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from config.lending_dialer_queue import QUEUE_SOURCES
from src.lending.dialer import queue_manager
from src.lending.dialer.queue_manager import (
    QueueReport, SourceCount, compliance_blocks, read_queue, render_queue_post,
)

# 2026-11-25 is EST (UTC-5): 11:00 UTC is 6 AM ET.
NOW = datetime(2026, 11, 25, 11, 0, tzinfo=timezone.utc)
RECENT = NOW - timedelta(days=3)

_DDL = (
    "CREATE TABLE lending.call_dispositions (id INTEGER PRIMARY KEY, phone TEXT, caller_name TEXT, disposition TEXT,"
    " call_started_at TIMESTAMP, created_at TIMESTAMP)",
    "CREATE TABLE lending.lendingflow_leads (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, phone TEXT,"
    " email TEXT, ghl_contact_id TEXT, loan_type TEXT, loan_amount INTEGER, loan_amount_range TEXT, property_city TEXT,"
    " property_state TEXT, received_at TIMESTAMP, handled_in_ghl_at TIMESTAMP, suppressed BOOLEAN NOT NULL DEFAULT 0,"
    " is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.lendingflow_enrichment (lead_id INTEGER, routing_tag TEXT, closer_priority BOOLEAN, fit TEXT)",
    "CREATE TABLE lending.web_leads (id INTEGER PRIMARY KEY, name TEXT, phone TEXT, email TEXT, ghl_contact_id TEXT,"
    " deal_type TEXT, loan_amount INTEGER, property_city TEXT, property_state TEXT, credit_band TEXT,"
    " completed_projects_3y TEXT, received_at TIMESTAMP, handled_in_ghl_at TIMESTAMP, ghl_status TEXT,"
    " suppressed BOOLEAN NOT NULL DEFAULT 0, is_test BOOLEAN NOT NULL DEFAULT 0)",
    "CREATE TABLE lending.dialer_load_records (id INTEGER PRIMARY KEY, pool TEXT, phone TEXT, borrower_name TEXT,"
    " entity_name TEXT, active BOOLEAN, created_at TIMESTAMP)",
)


@pytest.fixture(autouse=True)
def no_staged(monkeypatch):
    monkeypatch.setattr(queue_manager, "staged_pr_records", lambda db: [])


@pytest.fixture
def db():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with eng.begin() as conn:
        for statement in _DDL:
            conn.execute(text(statement))
    with Session(eng) as session:
        yield session
    eng.dispose()


def _pool(db, pool, phone, *, active=True, created=RECENT):
    db.execute(text("INSERT INTO lending.dialer_load_records (pool, phone, active, created_at)"
                    " VALUES (:pool, :phone, :active, :created)"),
               {"pool": pool, "phone": phone, "active": active, "created": created})


def _callback(db, phone, caller="David Willis"):
    db.execute(text("INSERT INTO lending.call_dispositions (phone, caller_name, disposition, call_started_at)"
                    " VALUES (:phone, :caller, 'CALLBACK_REQUESTED', :at)"),
               {"phone": phone, "caller": caller, "at": NOW - timedelta(days=1)})


def _counts(report):
    return {s.source: s for s in report.sources}


def _read(db, blocks=lambda *_: {}):
    return read_queue(db, NOW, lead_days=14, blocks=blocks)


def test_short_queue_names_every_dry_source(db):
    for n in range(3):
        _pool(db, "wholesalers", f"+1727555000{n}")
    _pool(db, "builders", "+17275559999", active=False)  # loaded recently, all used up
    _callback(db, "+18135556666")
    report = _read(db)
    assert report.eligible_total == 4 and report.short
    assert report.dry_sources == ("builders",)
    assert "Only 4 of 50 eligible" in render_queue_post(report)


def test_blocked_phones_are_not_eligible_and_never_padded(db):
    _pool(db, "transaction_ready", "+17275550001")
    _pool(db, "transaction_ready", "+17275550002")
    report = _read(db, blocks=lambda *_: {"+17275550002": "ATTEMPT_HISTORY_EXCEEDED"})
    tr = _counts(report)["transaction_ready"]
    assert (tr.loaded, tr.eligible, dict(tr.blocked)) == (2, 1, {"ATTEMPT_HISTORY_EXCEEDED": 1})
    assert report.eligible_total == 1  # never topped up to 50


def test_phone_in_two_sources_counts_once_in_the_first(db):
    _callback(db, "+17275550001")
    _pool(db, "wholesalers", "+17275550001")
    counts = _counts(_read(db))
    assert counts["callbacks"].eligible == 1 and counts["wholesalers"].loaded == 0


def test_blocks_is_called_once_with_every_phone(db):
    seen = []
    _callback(db, "+17275550001")
    _pool(db, "wholesalers", "+17275550002")
    _pool(db, "builders", "+17275550003")
    _read(db, blocks=lambda _db, phones, now: seen.append(sorted(phones)) or {})
    assert seen == [["+17275550001", "+17275550002", "+17275550003"]]


def test_josh_callbacks_are_not_davids(db):
    _callback(db, "+18135556666", caller="Josh Smith")
    assert _counts(_read(db))["callbacks"].loaded == 0


def test_failed_source_is_not_available_not_dry(db):
    _pool(db, "wholesalers", "+17275550001")
    db.execute(text("DROP TABLE lending.web_leads"))
    report = _read(db)
    web = _counts(report)["web_leads"]
    assert not web.available and "web_leads" not in report.dry_sources
    assert report.eligible_total == 1  # still computed from the sources that did read


def test_zero_eligible_says_no_eligible_work():
    report = QueueReport(day=date(2026, 11, 25), target=50, sources=(SourceCount("builders", 0, 0, {}),))
    assert render_queue_post(report).startswith(":no_entry: No eligible work today")


def test_full_queue_headline(db):
    for n in range(50):
        _pool(db, "wholesalers", f"+172755{n:05d}")
    report = _read(db)
    assert not report.short
    assert render_queue_post(report).startswith("Dialer queue ready: 50 eligible")


def test_call_window_and_daily_cap_do_not_count_as_ineligible(monkeypatch):
    from config.lending_compliance import ReasonCode as R
    from src.lending import compliance
    monkeypatch.setattr(compliance, "dial_blocks", lambda db, phones, now=None: {
        "p1": R.OUTSIDE_CALL_WINDOW, "p2": R.ATTEMPT_CAP_REACHED, "p3": R.NO_FRESH_SCRUB})
    monkeypatch.setattr(compliance, "suppressed_phones", lambda db, phones: set())
    monkeypatch.setattr(compliance, "scrub_verdict_blocks", lambda db, phones: {})
    monkeypatch.setattr(compliance, "lendingflow_lead_phones", lambda db, phones: set())
    assert compliance_blocks(None, ["p1", "p2", "p3"], NOW) == {"p3": "NO_FRESH_SCRUB"}


def test_lendingflow_lead_without_scrub_is_eligible(monkeypatch):
    from config.lending_compliance import ReasonCode as R
    from src.lending import compliance
    monkeypatch.setattr(compliance, "dial_blocks", lambda db, phones, now=None: {
        "p_lf": R.NO_FRESH_SCRUB, "p_cold": R.NO_FRESH_SCRUB})
    monkeypatch.setattr(compliance, "suppressed_phones", lambda db, phones: set())
    monkeypatch.setattr(compliance, "scrub_verdict_blocks", lambda db, phones: {})
    monkeypatch.setattr(compliance, "lendingflow_lead_phones", lambda db, phones: {"p_lf"})
    assert compliance_blocks(None, ["p_lf", "p_cold"], NOW) == {"p_cold": "NO_FRESH_SCRUB"}


def test_suppression_and_stored_verdict_block_even_when_dial_blocks_is_clear(monkeypatch):
    from config.lending_compliance import ReasonCode as R
    from src.lending import compliance
    monkeypatch.setattr(compliance, "dial_blocks", lambda db, phones, now=None: {})
    monkeypatch.setattr(compliance, "suppressed_phones", lambda db, phones: {"p1"})
    monkeypatch.setattr(compliance, "scrub_verdict_blocks", lambda db, phones: {"p2": R.LITIGATOR})
    assert compliance_blocks(None, ["p1", "p2"], NOW) == {"p1": "SUPPRESSED", "p2": "LITIGATOR"}


def test_post_never_contains_a_phone(db):
    _pool(db, "wholesalers", "+17275550123")
    _callback(db, "+18135556789")
    text_ = render_queue_post(_read(db, blocks=lambda *_: {"+17275550123": "SUPPRESSED"}))
    assert "5550123" not in text_ and "5556789" not in text_


def test_pool_with_no_recent_loads_is_not_in_use_not_dry(db):
    _pool(db, "builders", "+17275550001", active=False, created=NOW - timedelta(days=30))
    report = _read(db)
    assert "builders" not in report.dry_sources and not _counts(report)["builders"].in_use
    assert "not in use" in render_queue_post(report)


def test_pool_with_staged_records_is_loading_not_dry(db, monkeypatch):
    _pool(db, "transaction_ready", "+17275550001", active=False)
    monkeypatch.setattr(queue_manager, "staged_pr_records", lambda db: [{"pool": "transaction_ready"}] * 5)
    report = _read(db)
    assert _counts(report)["transaction_ready"].staged == 5 and "transaction_ready" not in report.dry_sources
    assert "+5 load at 9:05" in render_queue_post(report)


def test_zero_callbacks_is_never_dry(db):
    report = _read(db)
    assert "callbacks" not in report.dry_sources and "lendingflow_leads" not in report.dry_sources


def test_every_source_is_reported_in_order(db):
    assert tuple(s.source for s in _read(db).sources) == QUEUE_SOURCES
