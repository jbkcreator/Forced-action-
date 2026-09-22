"""Fixtures for the calendar suite."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from src.services.calendar import reset_calendar_client


@pytest.fixture
def bookings_db(fresh_db):
    """A session for tests that touch fa_max_bookings.

    Skips when the table is absent rather than creating it. The only database
    this suite can reach is the shared production one, where a CREATE TABLE
    carrying a foreign key takes a lock on the live fa_max_persons — a cost a
    test must never impose, and one a transaction rollback does not undo.
    The table arrives via migrations/apply_fa_max_bookings.py, run
    deliberately.
    """
    if fresh_db.execute(text("SELECT to_regclass('public.fa_max_bookings')")).scalar() is None:
        pytest.skip("fa_max_bookings absent — run migrations/apply_fa_max_bookings.py")

    # The integrity migration adds the columns and partial indexes that
    # idempotency and slot-claiming depend on; without them these tests would
    # pass against a schema that cannot make those guarantees.
    has_key = fresh_db.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'fa_max_bookings' AND column_name = 'idempotency_key'"
        )
    ).first()
    if has_key is None:
        pytest.skip(
            "fa_max_bookings lacks idempotency_key — "
            "run migrations/apply_fa_max_bookings_integrity.py"
        )
    return fresh_db


@pytest.fixture(autouse=True)
def _isolate_fake_calendar():
    """The fake client is a module singleton; one test's bookings must not leak."""
    reset_calendar_client()
    yield
    reset_calendar_client()
