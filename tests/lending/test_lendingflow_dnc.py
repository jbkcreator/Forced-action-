"""LendingFlow DNC check right after intake: a lead reaches GHL (and so BatchDialer) only once Tracerfy cleared it."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending import lendingflow
from src.lending.lendingflow import check_dnc_pending, deliver_pending, parse_lendingflow, save_lead
from src.lending.web_leads import PushResult

NOW = datetime(2026, 10, 27, 16, 0, tzinfo=timezone.utc)


class Sink:
    def __init__(self):
        self.pushed = []

    def push(self, lead):
        self.pushed.append(lead["id"])
        return PushResult(contact_id="ghl-1", pipeline_card=False)


class Scrubber:
    def __init__(self, *, dnc=(), litigator=(), fail=False):
        self.calls: list[list[str]] = []
        self.dnc, self.litigator, self.fail = set(dnc), set(litigator), fail

    def __call__(self, phones):
        self.calls.append(list(phones))
        if self.fail:
            raise RuntimeError("Tracerfy down")
        return [{"phone": p, "national_dnc": "Y" if p in self.dnc else "N",
                 "litigator": "Y" if p in self.litigator else "N", "state_dnc": "N"} for p in phones]


@pytest.fixture
def db(lending_db, monkeypatch):
    from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow
    from migrations.apply_lending_lendingflow_dialer import apply_to as apply_dialer

    conn = lending_db.connection()
    apply_lendingflow(conn)
    apply_dialer(conn)
    monkeypatch.setattr(lendingflow.get_settings(), "lending_lendingflow_dnc_check_enabled", True)
    monkeypatch.setattr(lendingflow, "_may_text_now", lambda db, lead, now: False)
    return lending_db


def _save(db):
    phone = f"(813) 555-{uuid.uuid4().int % 10_000:04d}"
    parsed = parse_lendingflow({"lead_id": f"lf-{uuid.uuid4().hex}", "phone": phone, "first_name": "Ada",
                                "consent": {"certificate_id": "c-1", "timestamp": "2026-10-27T15:00:00Z"}})
    result = save_lead(db, parsed, {}, now=NOW - timedelta(minutes=1))
    db.commit()
    return result.lead_id, parsed.phone


def _row(db, lead_id):
    return db.execute(text("SELECT dnc_status, dnc_result, suppressed, ghl_status FROM lending.lendingflow_leads "
                           "WHERE id = :id"), {"id": lead_id}).mappings().one()


def test_a_new_lead_waits_for_the_check_before_it_goes_to_ghl(db):
    lead_id, _ = _save(db)
    assert _row(db, lead_id)["dnc_status"] == "pending"
    sink = Sink()
    deliver_pending(db, sink, lead_id=lead_id, now=NOW)
    assert sink.pushed == []
    assert check_dnc_pending(db, Scrubber(), lead_id=lead_id, now=NOW) == 1
    assert _row(db, lead_id)["dnc_status"] == "clear"
    deliver_pending(db, sink, lead_id=lead_id, now=NOW)
    assert sink.pushed == [lead_id]


@pytest.mark.parametrize("kind, result", [("dnc", "NATIONAL_DNC"), ("litigator", "LITIGATOR")])
def test_a_dnc_or_litigator_lead_is_kept_but_sent_nowhere(db, kind, result):
    lead_id, phone = _save(db)
    check_dnc_pending(db, Scrubber(**{kind: [phone]}), now=NOW)
    row = _row(db, lead_id)
    assert (row["dnc_status"], row["dnc_result"], row["suppressed"], row["ghl_status"]) == ("dnc", result, True, "skipped")
    sink = Sink()
    deliver_pending(db, sink, now=NOW)
    assert lead_id not in sink.pushed


def test_a_dnc_lead_is_never_loaded_into_the_dialer(db):
    from src.lending import lendingflow_dialer as lfd

    lead_id, phone = _save(db)
    check_dnc_pending(db, Scrubber(dnc=[phone]), now=NOW)
    lfd._housekeeping(db, NOW)
    assert db.execute(text("SELECT dialer_status, dialer_block_reason FROM lending.lendingflow_leads WHERE id = :id"),
                      {"id": lead_id}).one() == ("blocked", "SUPPRESSED")


def test_a_failed_scrub_holds_the_lead_and_the_next_run_retries_it(db):
    lead_id, _ = _save(db)
    check_dnc_pending(db, Scrubber(fail=True), now=NOW)
    assert (_row(db, lead_id)["dnc_status"], _row(db, lead_id)["dnc_result"]) == ("pending", "SCRUB_FAILED")
    sink = Sink()
    deliver_pending(db, sink, lead_id=lead_id, now=NOW)
    assert sink.pushed == []
    check_dnc_pending(db, Scrubber(), now=NOW + timedelta(minutes=5))
    assert _row(db, lead_id)["dnc_status"] == "clear"


def test_a_fresh_stored_scrub_is_reused_so_tracerfy_is_not_paid_twice(db):
    lead_id, phone = _save(db)
    db.execute(text("INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) "
                    "VALUES (:p, false, false, :at, 'test') ON CONFLICT (phone) DO UPDATE SET checked_at = :at"),
               {"p": phone, "at": NOW - timedelta(days=1)})
    scrubber = Scrubber()
    check_dnc_pending(db, scrubber, now=NOW)
    assert scrubber.calls == [] and _row(db, lead_id)["dnc_status"] == "clear"


def test_a_lead_left_checking_by_a_crash_is_checked_again(db):
    lead_id, _ = _save(db)
    db.execute(text("UPDATE lending.lendingflow_leads SET dnc_status = 'checking', dnc_claimed_at = :t WHERE id = :id"),
               {"t": NOW - timedelta(minutes=30), "id": lead_id})
    check_dnc_pending(db, Scrubber(), now=NOW)
    assert _row(db, lead_id)["dnc_status"] == "clear"


def test_with_the_check_off_new_leads_skip_it_and_waiting_ones_are_released(db, monkeypatch):
    waiting, _ = _save(db)
    monkeypatch.setattr(lendingflow.get_settings(), "lending_lendingflow_dnc_check_enabled", False)
    fresh, _ = _save(db)
    assert _row(db, fresh)["dnc_status"] == "skipped"
    scrubber = Scrubber()
    check_dnc_pending(db, scrubber, now=NOW)
    assert scrubber.calls == [] and _row(db, waiting)["dnc_status"] == "skipped"
