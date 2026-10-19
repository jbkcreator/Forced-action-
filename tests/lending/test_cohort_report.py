"""90-day cohort and batch tracking (Runbook § 4.6, Item 6): batches filled in arrival order, funded counts at
30 / 60 / 90 days, fees and Revenue Per Lead, replacement requests. Against the database, rolled back.

Every lead and batch is dated 2031, after any real row, and the fixture hides real batches inside the rolled-back
transaction, so the ranking only sees the rows a test creates."""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import text

from migrations.apply_lending_cohorts import apply_to as apply_to_cohorts
from src.lending.cohort_report import (
    ReplacementRefused,
    add_batch,
    cohorts,
    decide_replacement,
    main,
    request_replacement,
)

ET_MORNING = datetime(2031, 1, 6, 15, 0, tzinfo=timezone.utc)  # Mon 10:00 AM ET


@pytest.fixture
def db(lending_db):
    apply_to_cohorts(lending_db.connection())
    lending_db.execute(text("DELETE FROM lending.lendingflow_replacements"))
    lending_db.execute(text("DELETE FROM lending.lendingflow_batches"))
    return lending_db


def _phone() -> str:
    return f"+1813556{uuid.uuid4().int % 10_000:04d}"


def _lead(db, received: datetime, phone: str | None = None) -> int:
    unique = uuid.uuid4().hex
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, received_at, phone) "
        "VALUES (:v, :h, '{}'::jsonb, :r, :p) RETURNING id"),
        {"v": f"cohort-{unique}", "h": unique + unique, "r": received, "p": phone or _phone()}).scalar()


def _phone_of(db, lead_id: int) -> str:
    return db.execute(text("SELECT phone FROM lending.lendingflow_leads WHERE id = :id"), {"id": lead_id}).scalar()


def _stage(db, phone: str, at: datetime, stage: str = "Funded", fee=None) -> None:
    body = {"stage_name": stage, "phone": phone}
    if fee is not None:
        body["monetary_value"] = fee
    db.execute(text(
        "INSERT INTO lending.ghl_stage_events (ghl_opportunity_id, stage_name, stage_key, phone, event_at, raw_event) "
        "VALUES (:opp, :stage, :key, :phone, :at, CAST(:raw AS jsonb))"),
        {"opp": f"cohort-opp-{uuid.uuid4().hex}", "stage": stage, "key": stage.lower(), "phone": phone, "at": at,
         "raw": json.dumps(body)})


def _calls(db, phone: str, starts: list[datetime]) -> None:
    for started in starts:
        db.execute(text(
            "INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_started_at, call_ended_at, "
            "raw_event) VALUES (:id, 'outbound', :p, :s, :s, '{}')"),
            {"id": f"cohort-{uuid.uuid4().hex}", "p": phone, "s": started})


def _only(rows, name):
    return next(c for c in rows if c.name == name)


# ── batches and funded windows ──────────────────────────────────────────────

def test_funded_counts_fees_and_revenue_per_lead_for_a_full_batch(db):
    add_batch(db, name="A", purchased_at=date(2031, 1, 6), lead_count=3)
    l1, l2, l3 = (_lead(db, ET_MORNING + timedelta(hours=h)) for h in range(3))
    _stage(db, _phone_of(db, l1), ET_MORNING + timedelta(days=20), fee="$1,500.00")
    _stage(db, _phone_of(db, l1), ET_MORNING + timedelta(days=40), fee="900")  # second funded deal: first one counts
    _stage(db, _phone_of(db, l2), ET_MORNING + timedelta(days=45), fee="call me")  # not an amount: ignored
    _stage(db, _phone_of(db, l3), ET_MORNING + timedelta(days=100))

    last = ET_MORNING + timedelta(hours=2)
    rows, unplaced = cohorts(db, now=last + timedelta(days=91))
    a = _only(rows, "A")
    assert (a.leads_bought, a.leads_delivered) == (3, 3)
    assert a.funded == {30: 1, 60: 2, 90: 2} and a.funded_to_date == 3
    assert a.fees_recorded == 1 and a.fees_total == Decimal("1500.00")
    assert a.revenue_per_lead == Decimal("500.00")
    assert unplaced == 0


def test_a_window_stays_open_until_every_lead_is_that_old(db):
    add_batch(db, name="A", purchased_at=date(2031, 1, 6), lead_count=2)
    first = _lead(db, ET_MORNING)
    _lead(db, ET_MORNING + timedelta(days=10))
    _stage(db, _phone_of(db, first), ET_MORNING + timedelta(days=5))

    a = _only(cohorts(db, now=ET_MORNING + timedelta(days=10 + 61))[0], "A")
    assert a.funded == {30: 1, 60: 1, 90: None}


def test_a_batch_not_yet_filled_reports_no_window_and_no_fee(db):
    add_batch(db, name="A", purchased_at=date(2031, 1, 6), lead_count=5)
    _lead(db, ET_MORNING)
    a = _only(cohorts(db, now=ET_MORNING + timedelta(days=365))[0], "A")
    assert a.leads_delivered == 1 and a.funded == {30: None, 60: None, 90: None}
    assert a.fees_total is None and a.revenue_per_lead is None and a.funded_to_date == 0


def test_leads_fill_batches_in_arrival_order_and_the_overflow_is_reported(db):
    add_batch(db, name="B", purchased_at=date(2031, 1, 20), lead_count=2)
    add_batch(db, name="A", purchased_at=date(2031, 1, 6), lead_count=2)
    _lead(db, ET_MORNING - timedelta(days=1))  # before the first purchase: not counted
    for h in range(5):
        _lead(db, ET_MORNING + timedelta(days=h))

    rows, unplaced = cohorts(db, now=ET_MORNING + timedelta(days=200))
    assert [(c.name, c.leads_delivered) for c in rows] == [("A", 2), ("B", 2)]
    assert unplaced == 1


def test_only_funded_stages_on_the_leads_phone_count(db):
    add_batch(db, name="A", purchased_at=date(2031, 1, 6), lead_count=1)
    lead = _lead(db, ET_MORNING)
    _stage(db, _phone_of(db, lead), ET_MORNING + timedelta(days=3), stage="Showed")
    _stage(db, _phone(), ET_MORNING + timedelta(days=3))  # another phone
    _stage(db, _phone_of(db, lead), ET_MORNING - timedelta(days=1))  # before the lead arrived
    a = _only(cohorts(db, now=ET_MORNING + timedelta(days=100))[0], "A")
    assert a.funded_to_date == 0 and a.funded == {30: 0, 60: 0, 90: 0}


def test_a_funded_loan_is_credited_to_the_latest_lead_on_that_phone(db):
    add_batch(db, name="A", purchased_at=date(2031, 1, 6), lead_count=1)
    add_batch(db, name="B", purchased_at=date(2031, 1, 20), lead_count=1)
    phone = _phone()
    _lead(db, ET_MORNING, phone=phone)
    _lead(db, ET_MORNING + timedelta(days=15), phone=phone)
    _stage(db, phone, ET_MORNING + timedelta(days=20), fee="2000")

    rows, _ = cohorts(db, now=ET_MORNING + timedelta(days=200))
    assert _only(rows, "A").funded_to_date == 0
    b = _only(rows, "B")
    assert b.funded == {30: 1, 60: 1, 90: 1} and b.fees_total == Decimal("2000")


def test_no_batches_means_an_empty_report(db):
    _lead(db, ET_MORNING)
    assert cohorts(db, now=ET_MORNING) == ([], 0)


def test_a_batch_needs_a_positive_lead_count(db):
    with pytest.raises(ValueError):
        add_batch(db, name="A", purchased_at=date(2031, 1, 6), lead_count=0)


# ── replacements ────────────────────────────────────────────────────────────

def _eligible_lead(db) -> int:
    lead = _lead(db, ET_MORNING)
    _calls(db, _phone_of(db, lead), [ET_MORNING + timedelta(days=d, minutes=5) for d in range(5)])
    return lead


def test_a_replacement_needs_the_replacement_rule(db):
    lead = _lead(db, ET_MORNING)
    _calls(db, _phone_of(db, lead), [ET_MORNING + timedelta(days=d, minutes=5) for d in range(4)] * 2)
    with pytest.raises(ReplacementRefused, match="does not meet"):
        request_replacement(db, lead)
    with pytest.raises(ReplacementRefused, match="not found"):
        request_replacement(db, -1)


def test_replacement_requests_and_credits_are_counted_per_batch(db):
    add_batch(db, name="A", purchased_at=date(2031, 1, 6), lead_count=3)
    credited, denied, open_ = (_eligible_lead(db) for _ in range(3))
    for lead in (credited, denied, open_):
        request_replacement(db, lead)
    decide_replacement(db, credited, "credited")
    decide_replacement(db, denied, "denied", note="answered on day 2")

    a = _only(cohorts(db, now=ET_MORNING + timedelta(days=200))[0], "A")
    assert (a.replacements_requested, a.replacements_credited, a.replacements_denied) == (3, 1, 1)


def test_a_replacement_is_requested_and_decided_once(db):
    lead = _eligible_lead(db)
    request_replacement(db, lead)
    with pytest.raises(ReplacementRefused, match="already"):
        request_replacement(db, lead)
    decide_replacement(db, lead, "credited")
    with pytest.raises(ReplacementRefused, match="no open"):
        decide_replacement(db, lead, "denied")
    with pytest.raises(ReplacementRefused, match="status must be"):
        decide_replacement(db, lead, "requested")


def test_the_cli_refuses_without_a_traceback(monkeypatch, db, caplog):
    from contextlib import contextmanager

    @contextmanager
    def session():
        yield db

    monkeypatch.setattr("src.lending.db.lending_session", session)
    assert main(["request-replacement", "--lead", "-1"]) == 1
    assert "not found" in caplog.text
    assert main(["add-batch", "--name", "CLI", "--purchased", "2031-01-06", "--leads", "2"]) == 0
    assert main([]) == 0
    assert "batch=" in caplog.text and "funded_30d=open" in caplog.text
