"""WP-GL-10 worker: consent / suppression / window gates, no blind re-sends, crash safety."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from config.lending_reminders import MAX_SEND_ATTEMPTS, RETRY_DELAY_SECONDS, STALE_SEND_SECONDS
from migrations.apply_lending_booking_messages import apply_to
from src.lending import reminder_worker
from src.lending.booking_messages import handle_booking_confirmed
from src.lending.consent import record_consent, revoke_consent
from src.lending.ghl_sms import GhlSmsError
from src.lending.reminder_worker import process_due

ET = ZoneInfo("America/New_York")
PHONE = "+18135550111"
TEXT_NUMBER = "+18135550100"  # the text-back number (LENDING_GHL_SMS_FROM_NUMBER)
SLOT = datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc)  # Wed 10:00 ET
NOON = datetime(2026, 10, 5, 16, 0, tzinfo=timezone.utc)  # Mon 12:00 ET: window open


class TextSender:
    def __init__(self, error: Exception | None = None):
        self.calls, self.error = [], error

    def __call__(self, phone, body, first_name=None, *, deadline=None):
        self.calls.append({"phone": phone, "body": body, "first_name": first_name, "deadline": deadline})
        if self.error:
            raise self.error
        return f"msg-{len(self.calls)}"


class EmailSender:
    def __init__(self):
        self.calls = []

    def __call__(self, to, subject, body, *, phone=None, first_name=None, deadline=None):
        self.calls.append((to, subject, body))
        return f"mail-{len(self.calls)}"


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def book(db, *, phone=PHONE, email="jane@example.com", ref="ref-1", slot=SLOT, now=NOON - timedelta(hours=1)):
    person = db.execute(
        text("INSERT INTO fa_max_persons (source, full_name, phone, email) VALUES ('test', 'Jane Doe', :p, :e) "
             "RETURNING person_id::text"), {"p": phone, "e": email}).scalar()
    handle_booking_confirmed(db, {"booking_ref": ref, "provider_event_id": f"appt-{ref}", "person_id": person,
                                  "slot_start_utc": slot, "property_address": "412 Oak Ave", "booked_by": "dana@heu.ai"},
                             now=now)


def cycle(db, *, text_sender=None, email_sender=None, text_enabled=True, email_enabled=False, now=NOON,
          number=TEXT_NUMBER):
    return process_due(db, text_sender=text_sender, email_sender=email_sender, text_enabled=text_enabled,
                       email_enabled=email_enabled, number=number, now=now, clock=lambda: now)


def row(db, kind="confirmation"):
    return db.execute(text("SELECT status, skip_reason, channel, attempts, send_at, provider_message_id, sent_at "
                           "FROM lending.booking_messages WHERE kind = :k"), {"k": kind}).mappings().one()


def test_a_consented_contact_gets_the_confirmation_text_once(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes", captured_by="dana@heu.ai")
    sender = TextSender()
    assert cycle(db, text_sender=sender) == {"sent": 1}
    assert len(sender.calls) == 1
    call = sender.calls[0]
    assert call["phone"] == PHONE and call["first_name"] == "Jane" and call["deadline"] == SLOT
    assert "confirming your call with Josh" in call["body"] and call["body"].endswith("Reply STOP to opt out.")
    done = row(db)
    assert (done["status"], done["channel"], done["provider_message_id"]) == ("sent", "text", "msg-1")
    assert done["sent_at"] is not None
    assert cycle(db, text_sender=sender) == {}  # nothing left that is due
    assert len(sender.calls) == 1


def test_reminders_are_scheduled_not_sent_with_the_confirmation(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    cycle(db, text_sender=TextSender())
    assert row(db, "night_before")["status"] == "pending" and row(db, "ninety_min")["status"] == "pending"


def test_each_reminder_goes_out_when_it_comes_due(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender()
    cycle(db, text_sender=sender)
    assert cycle(db, text_sender=sender, now=datetime(2026, 10, 6, 22, 5, tzinfo=timezone.utc)) == {"sent": 1}  # 6:05 pm ET
    assert "tomorrow at 10:00 am ET" in sender.calls[-1]["body"]
    assert cycle(db, text_sender=sender, now=SLOT - timedelta(minutes=89)) == {"sent": 1}
    assert "in about 90 minutes" in sender.calls[-1]["body"] and "(813) 555-0100" in sender.calls[-1]["body"]
    assert [row(db, k)["status"] for k in ("confirmation", "night_before", "ninety_min")] == ["sent"] * 3


def test_no_consent_and_no_email_is_skipped_visibly(db):
    book(db, email=None)
    sender = TextSender()
    assert cycle(db, text_sender=sender) == {"skipped_no_consent": 1}
    assert sender.calls == [] and row(db)["skip_reason"] == "no_consent"


def test_no_text_consent_falls_back_to_email_once_email_is_live(db):
    book(db)
    texts, mails = TextSender(), EmailSender()
    assert cycle(db, text_sender=texts, email_sender=mails, email_enabled=True) == {"sent": 1}
    assert texts.calls == [] and mails.calls[0][0] == "jane@example.com"
    assert row(db)["channel"] == "email"


def test_email_that_is_not_live_is_skipped_not_reported_sent(db):
    book(db)
    assert cycle(db, text_sender=TextSender(), email_enabled=False) == {"skipped_email_not_enabled": 1}
    assert row(db)["status"] == "skipped"


def test_texting_switched_off_sends_nothing_and_says_why(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender()
    assert cycle(db, text_sender=sender, text_enabled=False) == {"skipped_text_not_enabled": 1}
    assert sender.calls == []


def test_no_text_back_number_means_the_text_is_not_sent(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender()
    assert cycle(db, text_sender=sender, number=None) == {"skipped_not_configured": 1}
    assert sender.calls == []


def test_texting_not_configured_sends_nothing(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    assert cycle(db, text_sender=None) == {"skipped_not_configured": 1}


def test_a_stop_after_scheduling_blocks_the_text(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"),
               {"p": PHONE})
    sender = TextSender()
    assert cycle(db, text_sender=sender) == {"skipped_suppressed": 1}
    assert sender.calls == []


def test_suppression_blocks_the_email_fallback_too(db):
    book(db)
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"),
               {"p": PHONE})
    mails = EmailSender()
    assert cycle(db, email_sender=mails, email_enabled=True) == {"skipped_suppressed": 1}
    assert mails.calls == []


def test_revoked_consent_blocks_the_text(db):
    book(db, email=None)
    record_consent(db, PHONE, "on_call_yes")
    revoke_consent(db, PHONE)
    sender = TextSender()
    assert cycle(db, text_sender=sender) == {"skipped_no_consent": 1}


def test_a_cancelled_booking_is_never_texted(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    db.execute(text("UPDATE lending.booking_messages SET status = 'cancelled'"))
    sender = TextSender()
    assert cycle(db, text_sender=sender, now=SLOT - timedelta(minutes=89)) == {}
    assert sender.calls == []


def test_after_hours_confirmation_waits_for_the_window(db):
    book(db, now=datetime(2026, 10, 6, 1, 0, tzinfo=timezone.utc))  # Mon 9 pm ET
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender()
    night = datetime(2026, 10, 6, 1, 5, tzinfo=timezone.utc)
    assert cycle(db, text_sender=sender, now=night) == {"deferred_quiet_hours": 1}
    deferred = row(db)
    assert sender.calls == [] and deferred["status"] == "pending" and deferred["attempts"] == 0
    assert deferred["send_at"] == datetime(2026, 10, 6, 8, 0, tzinfo=ET)
    assert cycle(db, text_sender=sender, now=datetime(2026, 10, 6, 12, 1, tzinfo=timezone.utc)) == {"sent": 1}


def test_the_ninety_minute_text_is_skipped_not_delayed_when_the_window_is_closed(db):
    early = datetime(2026, 10, 7, 13, 0, tzinfo=timezone.utc)  # Wed 9:00 ET call -> 7:30 am text
    book(db, slot=early)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender()
    cycle(db, text_sender=sender)  # confirmation
    cycle(db, text_sender=sender, now=datetime(2026, 10, 6, 22, 5, tzinfo=timezone.utc))  # night before, on time
    at = early - timedelta(minutes=90)
    assert cycle(db, text_sender=sender, now=at) == {"skipped_quiet_hours": 1}
    assert (row(db, "ninety_min")["status"], row(db, "ninety_min")["skip_reason"]) == ("skipped", "quiet_hours")
    assert len(sender.calls) == 2  # confirmation + night-before only


def test_a_deferral_that_would_pass_the_call_is_skipped(db):
    soon = datetime(2026, 10, 6, 11, 0, tzinfo=timezone.utc)  # Tue 7:00 ET call
    book(db, slot=soon, now=datetime(2026, 10, 6, 1, 0, tzinfo=timezone.utc))
    record_consent(db, PHONE, "on_call_yes")
    assert cycle(db, text_sender=TextSender(), now=datetime(2026, 10, 6, 1, 5, tzinfo=timezone.utc)) == {"skipped_quiet_hours": 1}


def test_a_call_that_already_started_is_not_texted_about(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    assert cycle(db, text_sender=TextSender(), now=SLOT + timedelta(minutes=1)) == {"skipped_call_started": 3}


def test_an_ambiguous_ghl_error_is_never_resent(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender(GhlSmsError("timeout", ambiguous=True))
    assert cycle(db, text_sender=sender) == {"send_unknown": 1}
    assert row(db)["status"] == "send_unknown"
    assert cycle(db, text_sender=sender, now=NOON + timedelta(hours=1)) == {}
    assert len(sender.calls) == 1


def test_a_definite_failure_is_retried_then_given_up(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender(GhlSmsError("HTTP 400", ambiguous=False))
    now = NOON
    for attempt in range(1, MAX_SEND_ATTEMPTS):
        assert cycle(db, text_sender=sender, now=now) == {"retry": 1}
        assert row(db)["status"] == "pending"
        now += timedelta(seconds=RETRY_DELAY_SECONDS + 1)
    assert cycle(db, text_sender=sender, now=now) == {"failed": 1}
    assert row(db)["status"] == "failed" and len(sender.calls) == MAX_SEND_ATTEMPTS


def test_a_crash_mid_send_is_closed_as_unknown_never_resent(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender(RuntimeError("process died"))
    assert cycle(db, text_sender=sender) == {}
    assert row(db)["status"] == "sending"
    later = NOON + timedelta(seconds=STALE_SEND_SECONDS + 5)
    assert cycle(db, text_sender=TextSender(), now=later) == {}
    assert (row(db)["status"], row(db)["skip_reason"]) == ("send_unknown", "stale_claim")


def test_an_error_before_any_send_is_recorded_failed(db, monkeypatch):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    monkeypatch.setattr(reminder_worker, "_is_suppressed", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db hiccup")))
    sender = TextSender()
    assert cycle(db, text_sender=sender) == {"failed": 1}
    assert sender.calls == [] and row(db)["skip_reason"] == "internal_error"


def test_two_workers_never_claim_the_same_row(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    first = db.execute(reminder_worker._CLAIM, {"now": NOON, "limit": 50}).mappings().all()
    second = db.execute(reminder_worker._CLAIM, {"now": NOON, "limit": 50}).mappings().all()
    assert len(first) == 1 and second == []


def test_a_night_before_text_that_could_not_go_out_the_evening_before_is_skipped(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender()
    cycle(db, text_sender=sender)  # confirmation
    morning_of = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)  # Wed 8:00 ET: "tomorrow" would be wrong
    cycle(db, text_sender=sender, now=morning_of)
    assert (row(db, "night_before")["status"], row(db, "night_before")["skip_reason"]) == ("skipped", "too_late")
    assert len(sender.calls) == 1


def test_a_night_before_text_is_not_deferred_into_the_day_of_the_call(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    cycle(db, text_sender=TextSender())
    late_evening = datetime(2026, 10, 7, 1, 0, tzinfo=timezone.utc)  # Tue 9 pm ET, window closed
    assert cycle(db, text_sender=TextSender(), now=late_evening) == {"skipped_quiet_hours": 1}


def test_a_ninety_minute_text_that_is_very_late_is_skipped(db):
    book(db)
    record_consent(db, PHONE, "on_call_yes")
    sender = TextSender()
    cycle(db, text_sender=sender)
    cycle(db, text_sender=sender, now=datetime(2026, 10, 6, 22, 5, tzinfo=timezone.utc))
    worker_was_down = SLOT - timedelta(minutes=30)  # 60 minutes after it came due
    assert cycle(db, text_sender=sender, now=worker_was_down) == {"skipped_too_late": 1}
    assert len(sender.calls) == 2
