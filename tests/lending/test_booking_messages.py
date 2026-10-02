"""WP-GL-10 scheduling: the three messages per booking, rendering, the text window, cancellation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from config.lending_reminders import MAX_TEXT_CHARS
from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import (
    cancel_by_booking_ref,
    cancel_by_provider_event,
    handle_booking_confirmed,
    next_text_window,
    night_before_send_at,
    render_email,
    render_text,
    text_window_open,
)

ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)  # Mon 11:00 ET
SLOT = datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc)  # Wed 10:00 ET


# ── pure: rendering ──────────────────────────────────────────────────────────

def test_confirmation_is_the_approved_wording_with_stop_language():
    body = render_text("confirmation", first_name="Jane", slot_start_utc=SLOT, property_address="412 Oak Ave, Tampa")
    assert body == ("Hi Jane, this is Next Deal Lending confirming your call with Josh on Wednesday, October 7 "
                    "at 10:00 am ET about 412 Oak Ave. Reply here if you need to reschedule. Reply STOP to opt out.")


def test_no_address_drops_the_phrase():
    body = render_text("confirmation", first_name="Jane", slot_start_utc=SLOT, property_address=None)
    assert "about" not in body and body.endswith("Reply STOP to opt out.")
    assert "about" not in render_text("night_before", first_name="Jane", slot_start_utc=SLOT)


def test_night_before_and_ninety_minute_wording():
    nb = render_text("night_before", first_name="Jane", slot_start_utc=SLOT, property_address="412 Oak Ave")
    assert nb == ("Hi Jane, reminder from Next Deal Lending. Your call with Josh is tomorrow at 10:00 am ET "
                  "about 412 Oak Ave. Talk soon.")
    ninety = render_text("ninety_min", first_name="Jane", slot_start_utc=SLOT, property_address="412 Oak Ave",
                         number="(813) 555-0100")
    assert ninety == ("Hi Jane, your Next Deal Lending call with Josh is in about 90 minutes, at 10:00 am ET. "
                      "Call us at (813) 555-0100 if anything's come up.")


def test_only_the_confirmation_carries_stop_language():
    for kind in ("night_before", "ninety_min"):
        assert "STOP" not in render_text(kind, first_name="J", slot_start_utc=SLOT)


def test_blank_first_name_falls_back_and_length_is_capped():
    assert render_text("confirmation", first_name=None, slot_start_utc=SLOT).startswith("Hi there,")
    assert len(render_text("confirmation", first_name="J", slot_start_utc=SLOT, property_address="A" * 500)) <= MAX_TEXT_CHARS


def test_email_carries_no_stop_line():
    subject, body = render_email("confirmation", first_name="Jane", slot_start_utc=SLOT, property_address="412 Oak Ave")
    assert "confirmed" in subject.lower() and "STOP" not in body


# ── pure: times and the text window ──────────────────────────────────────────

def test_night_before_is_the_evening_before_in_eastern_time_across_dst():
    assert night_before_send_at(SLOT) == datetime(2026, 10, 6, 18, 0, tzinfo=ET)
    nov = datetime(2026, 11, 2, 15, 0, tzinfo=timezone.utc)  # Mon 10:00 ET, EST; the day before is still DST
    assert night_before_send_at(nov) == datetime(2026, 11, 1, 18, 0, tzinfo=ET)
    assert night_before_send_at(nov).utcoffset() == timedelta(hours=-5)


@pytest.mark.parametrize("hour,open_", [(7, False), (8, True), (19, True), (20, False), (23, False), (0, False)])
def test_text_window_is_8am_to_8pm_et(hour, open_):
    assert text_window_open(datetime(2026, 10, 5, hour, 30, tzinfo=ET)) is open_


def test_next_window_opens_at_8am_same_or_next_day():
    assert next_text_window(datetime(2026, 10, 5, 6, 0, tzinfo=ET)) == datetime(2026, 10, 5, 8, 0, tzinfo=ET)
    assert next_text_window(datetime(2026, 10, 5, 21, 0, tzinfo=ET)) == datetime(2026, 10, 6, 8, 0, tzinfo=ET)
    inside = datetime(2026, 10, 5, 12, 0, tzinfo=ET)
    assert next_text_window(inside) == inside


# ── database ─────────────────────────────────────────────────────────────────

@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def add_person(db, *, phone="+18135550111", email="jane@example.com", name="Jane Doe", merged_into=None) -> str:
    return db.execute(
        text("INSERT INTO fa_max_persons (source, full_name, phone, email, merged_into_id) "
             "VALUES ('test', :n, :p, :e, CAST(:m AS uuid)) RETURNING person_id::text"),
        {"n": name, "p": phone, "e": email, "m": merged_into},
    ).scalar()


def payload(person_id, **extra):
    return {"booking_ref": "ref-1", "provider_event_id": "appt-1", "person_id": person_id,
            "slot_start_utc": SLOT, "property_address": "412 Oak Ave, Tampa", "booked_by": "dana@heu.ai", **extra}


def rows(db):
    return db.execute(text("SELECT kind, status, skip_reason, send_at, first_name, contact_phone FROM lending.booking_messages "
                           "ORDER BY send_at")).mappings().all()


def test_schedules_confirmation_and_both_reminders(db):
    result = handle_booking_confirmed(db, payload(add_person(db)), now=NOW)
    assert result.inserted == 3 and result.skip_reason is None
    by_kind = {r["kind"]: r for r in rows(db)}
    assert by_kind["confirmation"]["send_at"] == NOW and by_kind["confirmation"]["status"] == "pending"
    assert by_kind["night_before"]["send_at"] == datetime(2026, 10, 6, 18, 0, tzinfo=ET)
    assert by_kind["ninety_min"]["send_at"] == SLOT - timedelta(minutes=90)
    assert {r["status"] for r in by_kind.values()} == {"pending"}
    assert by_kind["confirmation"]["first_name"] == "Jane" and by_kind["confirmation"]["contact_phone"] == "+18135550111"


def test_a_redelivered_booking_event_changes_nothing(db):
    person = add_person(db)
    handle_booking_confirmed(db, payload(person), now=NOW)
    again = handle_booking_confirmed(db, payload(person), now=NOW + timedelta(minutes=5))
    assert again.inserted == 0
    assert len(rows(db)) == 3
    assert db.execute(text("SELECT count(*) FROM lending.confirmation_tasks")).scalar() == 1


def test_a_booking_made_late_records_the_missed_reminders_as_skipped(db):
    late = payload(add_person(db), slot_start_utc=NOW + timedelta(minutes=60))  # 60 min away
    handle_booking_confirmed(db, late, now=NOW)
    by_kind = {r["kind"]: r for r in rows(db)}
    assert by_kind["confirmation"]["status"] == "pending"
    assert (by_kind["night_before"]["status"], by_kind["night_before"]["skip_reason"]) == ("skipped", "too_late")
    assert (by_kind["ninety_min"]["status"], by_kind["ninety_min"]["skip_reason"]) == ("skipped", "too_late")


def test_no_person_is_recorded_visibly_not_dropped(db):
    result = handle_booking_confirmed(db, payload(None), now=NOW)
    assert result.skip_reason == "no_person"
    assert {(r["status"], r["skip_reason"]) for r in rows(db)} == {("skipped", "no_person")}


def test_unknown_person_and_contactless_person_are_skipped_with_a_reason(db):
    assert handle_booking_confirmed(db, payload("00000000-0000-0000-0000-000000000000"), now=NOW).skip_reason == "person_not_found"
    empty = add_person(db, phone=None, email=None)
    assert handle_booking_confirmed(db, payload(empty, booking_ref="ref-2"), now=NOW).skip_reason == "no_contact_method"


def test_a_merged_person_resolves_to_the_surviving_record(db):
    survivor = add_person(db, phone="+18135550222", email="new@example.com", name="Jane Doe")
    old = add_person(db, phone="+18135550333", email=None, name="J Doe", merged_into=survivor)
    handle_booking_confirmed(db, payload(old), now=NOW)
    assert {r["contact_phone"] for r in rows(db)} == {"+18135550222"}


def test_confirmation_call_goes_to_the_booking_caller_or_josh_for_ai_bookings(db):
    person = add_person(db)
    caller = handle_booking_confirmed(db, payload(person), now=NOW)
    ai = handle_booking_confirmed(db, payload(person, booking_ref="ref-ai", booked_by="ai"), now=NOW)
    assert caller.assignee == "dana@heu.ai"
    assert ai.assignee == "jbkantor@gmail.com"
    due = db.execute(text("SELECT due_date FROM lending.confirmation_tasks WHERE booking_ref = 'ref-1'")).scalar()
    assert str(due) == "2026-10-06"  # the day before the Wednesday call


def test_a_naive_slot_time_is_rejected(db):
    with pytest.raises(ValueError):
        handle_booking_confirmed(db, payload(add_person(db), slot_start_utc=datetime(2026, 10, 7, 14, 0)), now=NOW)


def test_cancel_by_appointment_id_cancels_only_pending_rows(db):
    handle_booking_confirmed(db, payload(add_person(db)), now=NOW)
    db.execute(text("UPDATE lending.booking_messages SET status = 'sent' WHERE kind = 'confirmation'"))
    assert cancel_by_provider_event(db, "appt-1", "booking_cancelled") == 2
    statuses = {r["kind"]: r["status"] for r in rows(db)}
    assert statuses == {"confirmation": "sent", "night_before": "cancelled", "ninety_min": "cancelled"}
    assert cancel_by_provider_event(db, "appt-1", "booking_cancelled") == 0
    assert cancel_by_provider_event(db, "unknown", "booking_cancelled") == 0


def test_cancel_by_booking_ref(db):
    handle_booking_confirmed(db, payload(add_person(db)), now=NOW)
    assert cancel_by_booking_ref(db, "ref-1", "booking_rescheduled") == 3
