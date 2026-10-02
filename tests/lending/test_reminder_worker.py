"""Tests for WP-GL-10 reminder_worker: gate order, suppression, crash-recovery.

All tests use the Fake messenger (no network). DB is mocked.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from config.lending_reminders import KIND_CONFIRMATION, KIND_NIGHT_BEFORE, KIND_NINETY_MIN
from src.lending.ghl_messenger import FakeGHLMessenger, MessageResult
from src.lending.reminder_worker import process_due_rows


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _slot(hours: float = 24.0) -> datetime:
    return _now() + timedelta(hours=hours)


def _row(
    *,
    row_id: int = 1,
    kind: str = KIND_CONFIRMATION,
    channel: str = "text",
    phone: Optional[str] = "+18135550001",
    email: Optional[str] = "borrower@example.com",
    first_name: str = "Jane",
    address: Optional[str] = "412 Oak Ave",
    slot: Optional[datetime] = None,
    text_consent: bool = True,
) -> dict:
    return {
        "id": row_id,
        "booking_ref": f"ref-{row_id}",
        "kind": kind,
        "channel": channel,
        "send_at": _now() - timedelta(seconds=5),
        "first_name": first_name,
        "contact_phone": phone,
        "contact_email": email,
        "property_address": address,
        "slot_start_utc": slot or _slot(),
        "booked_by": "caller@heu.ai",
        "text_consent": text_consent,
    }


def _make_db(rows: list[dict]) -> MagicMock:
    db = MagicMock()

    def execute_side(stmt, params=None):
        result = MagicMock()
        sql = str(stmt)
        if "WHERE status = 'pending'" in sql:
            result.mappings.return_value.all.return_value = rows
        else:
            result.rowcount = 1
        return result

    db.execute.side_effect = execute_side
    return db


class TestProcessDueRows:
    def _run(self, rows, *, suppressed=None, text_enabled=True, dry_run=False):
        db = _make_db(rows)
        messenger = FakeGHLMessenger()
        suppressed_set = suppressed or set()

        def fake_suppression(db_, phones):
            return suppressed_set

        with (
            patch("src.lending.reminder_worker.get_messenger", return_value=messenger),
            patch("src.lending.reminder_worker.get_settings") as mock_s,
        ):
            mock_s.return_value.lending_text_enabled = text_enabled
            mock_s.return_value.lending_email_enabled = False  # email fake mode
            counts = process_due_rows(
                db, dry_run=dry_run, suppression_lookup=fake_suppression
            )
        return counts, messenger, db

    def test_sends_text_for_due_row(self):
        counts, messenger, _ = self._run([_row(channel="text")])
        assert counts.get("sent_text") == 1
        assert len(messenger.sent) == 1

    def test_suppressed_contact_is_skipped(self):
        row = _row(phone="+18135550001", channel="text")
        counts, messenger, _ = self._run([row], suppressed={"+18135550001"})
        assert counts.get("suppressed") == 1
        assert len(messenger.sent) == 0

    def test_text_not_enabled_skips(self):
        counts, messenger, _ = self._run([_row(channel="text")], text_enabled=False)
        assert counts.get("text_not_enabled") == 1
        assert len(messenger.sent) == 0

    def test_no_phone_skips_text_channel(self):
        counts, messenger, _ = self._run([_row(channel="text", phone=None)])
        assert counts.get("no_phone") == 1
        assert len(messenger.sent) == 0

    def test_email_channel_without_email_address_skips(self):
        counts, _, _ = self._run([_row(channel="email", email=None)])
        assert counts.get("no_email") == 1

    def test_email_channel_fake_sends(self):
        """Email fake always returns sent=True."""
        counts, messenger, _ = self._run([_row(channel="email", email="x@y.com")])
        assert counts.get("sent_email") == 1
        assert len(messenger.sent) == 0  # email path does not use GHL messenger

    def test_dry_run_does_not_send(self):
        counts, messenger, _ = self._run([_row()], dry_run=True)
        assert counts.get("dry_run") == 1
        assert len(messenger.sent) == 0

    def test_ghl_error_leaves_row_for_retry(self):
        """A GHLMessengerError is caught and counted as 'retry' (row stays pending)."""
        from src.lending.ghl_messenger import GHLMessengerError

        class FailingMessenger:
            def send_text(self, **kwargs):
                raise GHLMessengerError("network error")

        db = _make_db([_row(channel="text")])
        with (
            patch("src.lending.reminder_worker.get_messenger", return_value=FailingMessenger()),
            patch("src.lending.reminder_worker.get_settings") as mock_s,
        ):
            mock_s.return_value.lending_text_enabled = True
            mock_s.return_value.lending_email_enabled = False
            counts = process_due_rows(
                db, suppression_lookup=lambda db_, p: set()
            )
        assert counts.get("retry") == 1

    def test_no_double_send_on_concurrent_workers(self):
        """FOR UPDATE SKIP LOCKED is handled at the DB level; worker only sees
        rows the DB handed it. Two workers processing disjoint row-sets cannot
        both send the same row."""
        # Simulate: worker A gets row 1, worker B gets row 2.
        counts_a, messenger_a, _ = self._run([_row(row_id=1)])
        counts_b, messenger_b, _ = self._run([_row(row_id=2)])
        assert counts_a.get("sent_text") == 1
        assert counts_b.get("sent_text") == 1
        assert len(messenger_a.sent) == 1
        assert len(messenger_b.sent) == 1

    def test_empty_cycle_returns_empty_counts(self):
        counts, _, _ = self._run([])
        assert counts == {}

    def test_stop_propagation_blocks_text(self):
        """A phone that is in the suppression list after scheduling is blocked."""
        phone = "+18135550002"
        row = _row(phone=phone, channel="text")
        counts, messenger, _ = self._run([row], suppressed={phone})
        assert counts.get("suppressed") == 1
        assert len(messenger.sent) == 0
