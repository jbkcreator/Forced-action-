"""Unit tests for the new-lead contractor notification.

Mock-backed on purpose: the notifier's contract is "resolve a recipient, send,
never raise", which is fully exercisable without touching Postgres.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.services.delivery_notifier import _format_location, notify_delivery


def _db_returning(row):
    """A session whose delivery lookup yields `row` (or None)."""
    db = MagicMock()
    db.execute.return_value.mappings.return_value.first.return_value = row
    return db


def _row(**overrides):
    base = {
        "delivery_id": 1,
        "grade": "Gold",
        "vertical": "roofing",
        "address": "123 Main St",
        "city": "Tampa",
        "zip": "33601",
        "email": "contractor@test.com",
        "name": "Dana",
    }
    base.update(overrides)
    return base


class TestFormatLocation:

    def test_joins_all_parts(self):
        assert _format_location("123 Main St", "Tampa", "33601") == "123 Main St, Tampa, 33601"

    def test_skips_missing_parts(self):
        assert _format_location(None, "Tampa", "33601") == "Tampa, 33601"

    def test_falls_back_when_everything_missing(self):
        assert _format_location(None, None, None) == "your territory"


class TestNotifyDelivery:

    def test_sends_email_to_contractor(self):
        db = _db_returning(_row())

        with patch("src.services.delivery_notifier.send_email", return_value=True) as mock_send:
            result = notify_delivery(db, 1)

        assert result is True
        assert mock_send.call_count == 1
        kwargs = mock_send.call_args.kwargs
        assert kwargs["to"] == "contractor@test.com"
        assert "Gold" in kwargs["subject"] and "roofing" in kwargs["subject"]
        assert "123 Main St, Tampa, 33601" in kwargs["body_text"]

    def test_skips_when_no_email_on_file(self):
        db = _db_returning(_row(email=None))

        with patch("src.services.delivery_notifier.send_email") as mock_send:
            result = notify_delivery(db, 1)

        assert result is False
        assert mock_send.call_count == 0

    def test_returns_false_when_delivery_missing(self):
        db = _db_returning(None)

        with patch("src.services.delivery_notifier.send_email") as mock_send:
            result = notify_delivery(db, 999)

        assert result is False
        assert mock_send.call_count == 0

    def test_send_failure_is_swallowed(self):
        """A delivery is already committed by the time we notify — a send blowing
        up must not propagate and fail the sweep."""
        db = _db_returning(_row())

        with patch("src.services.delivery_notifier.send_email", side_effect=RuntimeError("smtp down")):
            result = notify_delivery(db, 1)

        assert result is False

    def test_lookup_failure_is_swallowed(self):
        db = MagicMock()
        db.execute.side_effect = RuntimeError("db gone")

        assert notify_delivery(db, 1) is False

    def test_suppressed_recipient_reports_not_sent(self):
        """send_email returns False for a suppressed/opted-out address."""
        db = _db_returning(_row())

        with patch("src.services.delivery_notifier.send_email", return_value=False):
            assert notify_delivery(db, 1) is False


class TestSweepIntegration:

    def test_notify_helper_never_raises(self):
        from src.tasks.lead_delivery_sweep import _notify

        with patch("src.services.delivery_notifier.notify_delivery",
                   side_effect=RuntimeError("boom")):
            assert _notify(MagicMock(), 1) is False

    def test_notify_helper_passes_through_result(self):
        from src.tasks.lead_delivery_sweep import _notify

        with patch("src.services.delivery_notifier.notify_delivery", return_value=True):
            assert _notify(MagicMock(), 1) is True
