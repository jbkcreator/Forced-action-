"""Unit tests for activation_tracking.py — the 5-minute activation funnel
stamps (T-B12-05), extended in Section 4.10 with onboarding_completed_time.

Mock-backed: db.begin_nested() is used as a context manager only, no real
transaction semantics are exercised here — that's covered by whatever
integration test hits the real onboarding/dashboard/unlock endpoints.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.services.activation_tracking import (
    get_activation_status,
    stamp_first_leads_shown,
    stamp_first_unlock,
    stamp_onboarding_completed,
)


def _db():
    db = MagicMock()
    db.begin_nested.return_value.__enter__ = MagicMock(return_value=None)
    db.begin_nested.return_value.__exit__ = MagicMock(return_value=False)
    return db


class TestStampOnboardingCompleted:

    def test_ensures_row_then_updates(self):
        db = _db()
        stamp_onboarding_completed(7, db)

        statements = [str(c.args[0]) for c in db.execute.call_args_list]
        assert any("INSERT INTO activation_events" in s for s in statements)
        assert any("onboarding_completed_time = now()" in s for s in statements)

    def test_set_once_guard_in_sql(self):
        """The UPDATE must only fire when the column is still NULL — the
        set-once contract lives in the WHERE clause, not in Python."""
        db = _db()
        stamp_onboarding_completed(7, db)

        update_stmt = next(
            str(c.args[0]) for c in db.execute.call_args_list
            if "onboarding_completed_time = now()" in str(c.args[0])
        )
        assert "onboarding_completed_time IS NULL" in update_stmt

    def test_exception_is_swallowed_not_raised(self):
        """Instrumentation must never break the onboarding submit it's attached to."""
        db = MagicMock()
        db.begin_nested.side_effect = RuntimeError("db gone")

        stamp_onboarding_completed(7, db)  # must not raise


class TestGetActivationStatus:

    def test_all_none_when_no_row(self):
        db = MagicMock()
        db.execute.return_value.mappings.return_value.first.return_value = None

        status = get_activation_status(7, db)

        assert status == {
            "signup_time": None,
            "onboarding_completed_time": None,
            "first_leads_shown_time": None,
            "first_unlock_time": None,
        }

    def test_includes_onboarding_completed_time(self):
        import datetime as dt

        db = MagicMock()
        ts = dt.datetime(2026, 7, 27, tzinfo=dt.timezone.utc)
        db.execute.return_value.mappings.return_value.first.return_value = {
            "signup_time": ts,
            "onboarding_completed_time": ts,
            "first_leads_shown_time": None,
            "first_unlock_time": None,
        }

        status = get_activation_status(7, db)

        assert status["onboarding_completed_time"] == ts.isoformat()
        assert status["first_leads_shown_time"] is None


class TestExistingStampsUnaffected:
    """Regression: extending the table/service must not touch the existing
    two stamps' behavior."""

    def test_first_leads_shown_still_set_once(self):
        db = _db()
        stamp_first_leads_shown(7, db)
        update_stmt = next(
            str(c.args[0]) for c in db.execute.call_args_list
            if "first_leads_shown_time = now()" in str(c.args[0])
        )
        assert "first_leads_shown_time IS NULL" in update_stmt

    def test_first_unlock_still_set_once(self):
        db = _db()
        stamp_first_unlock(7, db)
        update_stmt = next(
            str(c.args[0]) for c in db.execute.call_args_list
            if "first_unlock_time = now()" in str(c.args[0])
        )
        assert "first_unlock_time IS NULL" in update_stmt
