"""WP-GL-10: the morning Slack list of confirmation calls due."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import handle_booking_confirmed
from src.tasks import lending_confirmation_tasks as digest

NOW = datetime(2026, 10, 6, 13, 0, tzinfo=timezone.utc)   # Tue 9:00 ET
TODAY = date(2026, 10, 6)


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def book(db, ref, slot, booked_by="dana@heu.ai", name="Marcus", phone="+18135550147", booked_at=None):
    handle_booking_confirmed(db, {"booking_ref": ref, "phone": phone, "first_name": name, "slot_start_utc": slot,
                                  "booked_by": booked_by}, now=booked_at or datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc))


def test_a_task_due_today_is_listed_with_the_last_four_digits_only(db):
    book(db, "r1", datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc))     # Wed call -> due Tue
    message = digest.format_slack(digest.open_tasks(db, TODAY, NOW), TODAY)
    assert "dana@heu.ai" in message and "Marcus (…0147)" in message and "Wed Oct 7 at 10:00 am ET" in message
    assert "+1813" not in message and "OVERDUE" not in message


def test_an_overdue_task_is_marked_and_a_future_one_is_not_listed(db):
    book(db, "r1", datetime(2026, 10, 6, 21, 0, tzinfo=timezone.utc))     # Tue 5pm ET call, due Mon (booked Oct 1)
    book(db, "r2", datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc))     # Fri call -> due Thu, not yet
    message = digest.format_slack(digest.open_tasks(db, TODAY, NOW), TODAY)
    assert "OVERDUE since Mon Oct 5" in message and message.count("•") == 1


def test_a_task_whose_call_has_started_drops_off(db):
    book(db, "r1", datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc))     # Tue 10:00 ET
    assert digest.open_tasks(db, TODAY, datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)) == []


def test_a_completed_task_and_an_ancient_one_are_not_listed(db):
    book(db, "r1", datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc))
    db.execute(text("UPDATE lending.confirmation_tasks SET completed_at = now() WHERE booking_ref = 'r1'"))
    book(db, "r2", datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc), booked_at=datetime(2026, 9, 1, 15, 0, tzinfo=timezone.utc))
    db.execute(text("UPDATE lending.confirmation_tasks SET due_date = '2026-09-20' WHERE booking_ref = 'r2'"))
    assert digest.open_tasks(db, TODAY, NOW) == []


def test_tasks_are_grouped_by_assignee_and_ai_bookings_go_to_josh(db):
    book(db, "r1", datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc))
    book(db, "r2", datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc), booked_by="ai", name="Dana", phone="+18135550111",
         booked_at=datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc))
    message = digest.format_slack(digest.open_tasks(db, TODAY, NOW), TODAY)
    assert "(2)" in message and "*dana@heu.ai*" in message and "*jbkantor@gmail.com*" in message


def test_nothing_due_posts_nothing(db):
    assert digest.format_slack([], TODAY) is None


def test_main_only_acts_at_9am_et_and_fails_loudly_when_unconfigured(monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_dial_tasks_channel", "", raising=False)
    assert digest.main([], now=datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)) == 0      # 11:00 ET: not the hour
    assert digest.main([], now=NOW) == 1                                                    # 9:00 ET, no channel
