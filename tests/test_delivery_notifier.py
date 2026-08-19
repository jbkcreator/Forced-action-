"""Unit tests for the batched new-lead contractor notification.

Mock-backed on purpose: the notifier's contract is "group un-notified deliveries
by recipient, send one email each, stamp on success, never raise", which is fully
exercisable without touching Postgres.
"""
from __future__ import annotations

from unittest.mock import patch

from src.services.delivery_notifier import (
    _PENDING_EMAILS_SQL,
    _PENDING_FOR_EMAIL_SQL,
    _build_batch_bodies,
    _format_location,
    notify_pending_deliveries,
)


class _FakeResult:
    def __init__(self, data):
        self._data = data

    def scalars(self):
        return self

    def mappings(self):
        return self

    def all(self):
        return self._data


class _FakeDB:
    """Session double: serves pending emails, per-recipient rows, records UPDATEs."""

    def __init__(self, emails, rows_by_email):
        self._emails = emails
        self._rows_by_email = rows_by_email
        self.stamped_ids: list[list[int]] = []
        self.committed = 0
        self.rolledback = 0

    def execute(self, stmt, params=None):
        if stmt is _PENDING_EMAILS_SQL:
            return _FakeResult(self._emails)
        if stmt is _PENDING_FOR_EMAIL_SQL:
            return _FakeResult(self._rows_by_email.get(params["email"], []))
        # the UPDATE ... SET notified_at
        self.stamped_ids.append(params["ids"])
        return _FakeResult([])

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolledback += 1


def _row(**overrides):
    base = {
        "delivery_id": 1,
        "grade": "Gold",
        "vertical": "roofing",
        "address": "123 Main St",
        "city": "Tampa",
        "zip": "33601",
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


class TestBuildBatchBodies:

    def test_lists_every_lead_and_is_branded(self):
        leads = [_row(delivery_id=1, address="1 A St"), _row(delivery_id=2, address="2 B St")]
        text_body, html = _build_batch_bodies("Dana", leads)

        assert "1 A St" in html and "2 B St" in html
        assert "Forced" in html and "#d4a040" in html  # shared shell + gold accent
        assert "2 new leads" in html
        assert "1 A St" in text_body and "2 B St" in text_body

    def test_caps_inline_rows_and_summarises_remainder(self):
        leads = [_row(delivery_id=i, address=f"{i} St") for i in range(30)]
        _text, html = _build_batch_bodies("Dana", leads)

        assert "0 St" in html          # first shown
        assert "29 St" not in html     # beyond the 25 cap
        assert "and 5 more" in html    # 30 - 25


class TestNotifyPendingDeliveries:

    def test_one_email_per_recipient_and_stamps_all_their_rows(self):
        db = _FakeDB(
            emails=["a@x.com"],
            rows_by_email={"a@x.com": [_row(delivery_id=1), _row(delivery_id=2)]},
        )
        with patch("src.services.delivery_notifier.send_email", return_value=True) as send:
            stats = notify_pending_deliveries(db)

        assert send.call_count == 1
        assert stats == {"recipients": 1, "emailed": 1, "deliveries": 2}
        assert db.stamped_ids == [[1, 2]]   # both rows stamped in one UPDATE
        assert db.committed == 1

    def test_suppressed_or_failed_send_leaves_rows_unstamped(self):
        db = _FakeDB(emails=["a@x.com"], rows_by_email={"a@x.com": [_row(delivery_id=7)]})
        with patch("src.services.delivery_notifier.send_email", return_value=False):
            stats = notify_pending_deliveries(db)

        assert stats["emailed"] == 0
        assert db.stamped_ids == []      # never stamped → retried next run
        assert db.rolledback >= 1

    def test_send_exception_is_swallowed_and_not_stamped(self):
        db = _FakeDB(emails=["a@x.com"], rows_by_email={"a@x.com": [_row(delivery_id=9)]})
        with patch("src.services.delivery_notifier.send_email", side_effect=RuntimeError("smtp")):
            stats = notify_pending_deliveries(db)

        assert stats["emailed"] == 0
        assert db.stamped_ids == []

    def test_multiple_recipients_each_get_one_email(self):
        db = _FakeDB(
            emails=["a@x.com", "b@x.com"],
            rows_by_email={
                "a@x.com": [_row(delivery_id=1)],
                "b@x.com": [_row(delivery_id=2), _row(delivery_id=3)],
            },
        )
        with patch("src.services.delivery_notifier.send_email", return_value=True) as send:
            stats = notify_pending_deliveries(db)

        assert send.call_count == 2
        assert stats == {"recipients": 2, "emailed": 2, "deliveries": 3}
