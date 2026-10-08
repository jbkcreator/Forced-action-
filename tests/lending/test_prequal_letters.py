from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending import prequal_letters
from src.lending.prequal import FitLimits, PrequalLead
from src.lending.prequal_letters import enqueue, send_pending
from src.lending.web_leads import DeliveryError

FULL = PrequalLead(credit_band="700+", loan_amount=400_000, property_state="FL", loan_type="FIX_AND_FLIP")
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)


def fits(_lead):
    return [FitLimits(100_000, 2_000_000)]


def no_fits(_lead):
    return []


class FakeSink:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def deliver(self, letter_id, contact_id, pdf):
        if self.error:
            raise self.error
        self.calls.append((letter_id, contact_id, pdf))


@pytest.fixture(autouse=True)
def _fake_render(monkeypatch):
    monkeypatch.setattr(prequal_letters, "render_pdf", lambda t, c, watermark: b"%PDF-fake")


def _row(db, letter_id):
    return db.execute(text("SELECT status, attempts, skip_reason, last_error, sent_at "
                           "FROM lending.prequal_letters WHERE id = :id"), {"id": letter_id}).mappings().one()


def _q(db, ref="lf-1", lead=FULL, contact="cid1"):
    return enqueue(db, lead_source="lendingflow", lead_ref=ref, contact_id=contact, lead=lead)


def test_enqueue_once_per_lead(prequal_db):
    first = _q(prequal_db)
    assert first is not None
    assert _q(prequal_db) is None
    assert prequal_db.execute(text("SELECT count(*) FROM lending.prequal_letters")).scalar() == 1


@pytest.mark.parametrize("missing", ["credit_band", "loan_amount", "property_state", "loan_type"])
def test_incomplete_lead_not_queued(prequal_db, missing):
    lead = PrequalLead(**{**FULL.__dict__, missing: None})
    assert _q(prequal_db, lead=lead) is None


def test_no_contact_not_queued(prequal_db):
    assert _q(prequal_db, contact="") is None


def test_send_marks_sent(prequal_db):
    lid = _q(prequal_db)
    sink = FakeSink()
    assert send_pending(prequal_db, sink, fits, enabled=True, pct=10, letter_id=lid, now=NOW) == 1
    assert sink.calls == [(lid, "cid1", b"%PDF-fake")]
    row = _row(prequal_db, lid)
    assert row["status"] == "sent" and row["attempts"] == 1 and row["sent_at"] is not None


def test_sent_letter_not_resent(prequal_db):
    lid = _q(prequal_db)
    sink = FakeSink()
    send_pending(prequal_db, sink, fits, enabled=True, pct=10, letter_id=lid, now=NOW)
    assert send_pending(prequal_db, sink, fits, enabled=True, pct=10, now=NOW + timedelta(hours=1)) == 0
    assert len(sink.calls) == 1


def test_no_fitting_lender_skipped(prequal_db):
    lid = _q(prequal_db)
    sink = FakeSink()
    assert send_pending(prequal_db, sink, no_fits, enabled=True, pct=10, letter_id=lid, now=NOW) == 0
    assert sink.calls == []
    row = _row(prequal_db, lid)
    assert row["status"] == "skipped" and row["skip_reason"] == "no fitting lender"


def test_flag_off_or_unconfigured_leaves_pending(prequal_db):
    lid = _q(prequal_db)
    assert send_pending(prequal_db, FakeSink(), fits, enabled=False, pct=10, letter_id=lid, now=NOW) == 0
    assert send_pending(prequal_db, None, fits, enabled=True, pct=10, letter_id=lid, now=NOW) == 0
    assert send_pending(prequal_db, FakeSink(), None, enabled=True, pct=10, letter_id=lid, now=NOW) == 0
    row = _row(prequal_db, lid)
    assert row["status"] == "pending" and row["attempts"] == 0


def test_failure_counts_attempt_and_backs_off(prequal_db):
    lid = _q(prequal_db)
    send_pending(prequal_db, FakeSink(DeliveryError("prequal email send: HTTP 500")), fits,
                 enabled=True, pct=10, letter_id=lid, now=NOW)
    row = _row(prequal_db, lid)
    assert row["status"] == "failed" and row["attempts"] == 1 and "HTTP 500" in row["last_error"]
    sink = FakeSink()
    assert send_pending(prequal_db, sink, fits, enabled=True, pct=10, now=NOW + timedelta(minutes=1)) == 0
    assert send_pending(prequal_db, sink, fits, enabled=True, pct=10, now=NOW + timedelta(minutes=3)) == 1
    assert _row(prequal_db, lid)["status"] == "sent"


def test_config_error_does_not_use_attempt(prequal_db):
    lid = _q(prequal_db)
    send_pending(prequal_db, FakeSink(DeliveryError("HTTP 401", config_error=True)), fits,
                 enabled=True, pct=10, letter_id=lid, now=NOW)
    row = _row(prequal_db, lid)
    assert row["status"] == "failed" and row["attempts"] == 0


def test_unexpected_error_recorded_without_detail(prequal_db):
    lid = _q(prequal_db)
    send_pending(prequal_db, FakeSink(RuntimeError("secret detail")), fits,
                 enabled=True, pct=10, letter_id=lid, now=NOW)
    assert _row(prequal_db, lid)["last_error"] == "unexpected RuntimeError"


def test_gives_up_after_window_and_flags_once(prequal_db, caplog):
    lid = _q(prequal_db)
    prequal_db.execute(text("UPDATE lending.prequal_letters SET created_at = :t WHERE id = :id"),
                       {"t": NOW - timedelta(hours=25), "id": lid})
    with caplog.at_level("WARNING"):
        assert send_pending(prequal_db, FakeSink(), None, enabled=True, pct=10, now=NOW) == 0
    row = _row(prequal_db, lid)
    assert row["status"] == "failed" and row["last_error"].startswith("gave up")
    assert any("giving up" in r.message for r in caplog.records)
    caplog.clear()
    with caplog.at_level("WARNING"):
        send_pending(prequal_db, FakeSink(), fits, enabled=True, pct=10, now=NOW)
    assert not any("giving up" in r.message for r in caplog.records)
