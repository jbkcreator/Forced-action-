"""WP-GL-9 processor: the spec's 'done when' scenarios and the failure modes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_gl9_text_back import apply_to
from src.lending.consent import record_consent
from src.lending.dispositions import queue_missed_call
from src.lending.ghl_sms import GhlSmsError
from src.lending.text_back import process_pending

NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)  # 11:00 ET
PHONE = "+18135558601"
NUMBER = "+18135550100"


class Sender:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def __call__(self, phone, body, first_name=None):
        if self.fail:
            raise GhlSmsError("GHL message send failed: HTTP 500")
        self.sent.append((phone, body, first_name))
        return f"msg-{len(self.sent)}"


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def seed(db, call_id="c1", *, phone=PHONE, age=10, queue="verified_maturity", caller="Alex", address="123 Main St, Tampa FL"):
    ended = NOW - timedelta(seconds=age)
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, queue, caller_name, raw_event) "
                    "VALUES (:c, :p, 'outbound', :e, :q, :n, '{}'::jsonb)"), {"c": call_id, "p": phone, "e": ended, "q": queue, "n": caller})
    record = {"property_address": address} if address else None
    return queue_missed_call(db, call_id, phone, None, record, ended)


def status(db, call_id="c1"):
    return db.execute(text("SELECT status FROM lending.missed_call_events WHERE dialer_call_id = :c"), {"c": call_id}).scalar()


def run(db, sender, *, enabled=True, number=NUMBER, now=NOW):
    return process_pending(db, sender=sender, enabled=enabled, number=number, now=now)


def test_an_unanswered_call_to_a_consented_contact_gets_exactly_one_text(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    sender = Sender()
    assert run(db, sender) == {"sent": 1}
    assert [(p, "123 Main St" in b, b.endswith("Reply STOP to opt out.")) for p, b, _ in sender.sent] == [(PHONE, True, True)]
    row = db.execute(text("SELECT status, template_key, provider_message_id, decided_at FROM lending.missed_call_events")).one()
    assert row[:3] == ("sent", "maturity", "msg-1") and row[3] is not None
    assert run(db, sender) == {} and len(sender.sent) == 1  # nothing pending: never a second text


def test_a_contact_without_consent_gets_no_text(db):
    seed(db)
    sender = Sender()
    assert run(db, sender) == {"skipped_no_consent": 1} and sender.sent == []


def test_consent_plus_suppression_still_gets_no_text(db):
    record_consent(db, PHONE, "web_form")
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    seed(db)  # queue_missed_call itself marks a suppressed number 'blocked'
    sender = Sender()
    assert run(db, sender) == {} and sender.sent == [] and status(db) == "blocked"


def test_an_event_older_than_sixty_seconds_is_never_texted(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db, age=61)
    sender = Sender()
    assert run(db, sender) == {"skipped_late": 1} and sender.sent == []


def test_a_backlog_after_an_outage_sends_nothing_stale(db):
    record_consent(db, PHONE, "on_call_yes")
    for i in range(3):
        seed(db, f"c{i}", phone=f"+1813555870{i}", age=600)
        record_consent(db, f"+1813555870{i}", "on_call_yes")
    sender = Sender()
    assert run(db, sender) == {"skipped_late": 3} and sender.sent == []


def test_flag_off_records_a_dry_run_and_sends_nothing(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    sender = Sender()
    assert run(db, sender, enabled=False) == {"dry_run": 1} and sender.sent == []


def test_no_sender_configured_fails_closed(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    assert run(db, None) == {"skipped_not_configured": 1}


def test_outside_eight_to_eight_et_is_not_texted(db):
    record_consent(db, PHONE, "on_call_yes")
    late_night = datetime(2026, 10, 6, 1, 0, tzinfo=timezone.utc)  # 9:00 pm ET
    ended = late_night - timedelta(seconds=10)
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, raw_event) "
                    "VALUES ('c1', :p, 'outbound', :e, '{}'::jsonb)"), {"p": PHONE, "e": ended})
    queue_missed_call(db, "c1", PHONE, None, None, ended)
    sender = Sender()
    assert run(db, sender, now=late_night) == {"skipped_quiet_hours": 1} and sender.sent == []


def test_a_failed_send_is_recorded_and_never_retried(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    assert run(db, Sender(fail=True)) == {"failed": 1}
    assert status(db) == "failed" and run(db, Sender()) == {}


def test_a_crash_after_the_claim_becomes_send_unknown_and_keeps_the_slot(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    db.execute(text("UPDATE lending.missed_call_events SET status = 'sending', decided_at = :t"), {"t": NOW - timedelta(minutes=6)})
    assert run(db, Sender()) == {} and status(db) == "send_unknown"
    assert queue_missed_call(db, "c2", PHONE, None, None, NOW) == "duplicate_day"


def test_a_skipped_text_does_not_burn_the_days_slot(db):
    seed(db, "c1")                       # 9 am, no consent -> skipped
    assert run(db, Sender()) == {"skipped_no_consent": 1}
    record_consent(db, PHONE, "on_call_yes")
    seed(db, "c2")                       # later call, consent now exists
    sender = Sender()
    assert run(db, sender) == {"sent": 1} and len(sender.sent) == 1


def test_a_second_missed_call_after_a_sent_text_is_a_duplicate_day(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db, "c1")
    sender = Sender()
    run(db, sender)
    assert seed(db, "c2") == "duplicate_day" and run(db, sender) == {} and len(sender.sent) == 1


def test_nurture_queue_and_no_address_use_the_general_text(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db, queue="nurture")
    sender = Sender()
    run(db, sender)
    assert "the loan on" not in sender.sent[0][1] and "Sorry we missed each other" in sender.sent[0][1]


def test_the_borrowers_first_name_comes_from_the_load_table(db):
    record_consent(db, PHONE, "on_call_yes")
    db.execute(text("INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash, campaign_tag, borrower_name) "
                    "VALUES ('r', 'transaction_ready', 'ref', :p, 'h', 'Transaction ready', 'Dana Cruz')"), {"p": PHONE})
    seed(db, queue="transaction_ready")
    sender = Sender()
    run(db, sender)
    assert sender.sent[0][2] == "Dana" and sender.sent[0][1].startswith("Hi Dana,")


def test_the_county_comes_from_the_calling_pool_staging_table(db):
    record_consent(db, PHONE, "on_call_yes")
    try:
        with db.begin_nested():
            db.execute(text("INSERT INTO lending.calling_pool_staging (run_id, pool_name, county_name, normalized_phone, "
                            "aircall_campaign_tag, source_table) VALUES (gen_random_uuid(), 'active_builder', 'Hillsborough', :p, 'x', 't')"),
                       {"p": PHONE})
    except Exception:
        pytest.skip("lending.calling_pool_staging shape differs on this DB (PR #318 migration)")
    seed(db, queue="builders")
    sender = Sender()
    run(db, sender)
    assert "investors in Hillsborough fund their next deal" in sender.sent[0][1]


def test_a_render_failure_is_recorded_failed_and_never_left_in_sending(db):
    record_consent(db, PHONE, "on_call_yes")
    seed(db)
    sender = Sender()
    assert run(db, sender, number="   ") == {"failed": 1} and sender.sent == []
    assert status(db) == "failed"


def test_a_text_back_error_never_stops_the_poller_cycle(monkeypatch, caplog):
    from src.lending import cdr_poller
    monkeypatch.setattr("src.lending.text_back.run_text_back_cycle", lambda **k: (_ for _ in ()).throw(RuntimeError(PHONE)))
    cdr_poller.text_back_step()  # must not raise
    assert PHONE not in caplog.text and "text-back cycle failed" in caplog.text


def test_the_old_second_poller_refuses_to_start(caplog):
    from src.lending import missed_call_poller
    missed_call_poller.main(["--once"])
    assert "superseded" in caplog.text
