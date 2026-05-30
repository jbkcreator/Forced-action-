"""
Unit tests for county_waitlist_notifier.

DB is fully mocked via MagicMock + monkeypatch. No real Postgres, SMTP, or
Telnyx calls are made. Pattern follows test_county_launch_evaluator.py.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.tasks.county_waitlist_notifier import run_waitlist_notifier


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_db_context(monkeypatch):
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)

    @contextmanager
    def _mock_ctx():
        yield session

    monkeypatch.setattr("src.tasks.county_waitlist_notifier.get_db_context", _mock_ctx)
    return session


def _make_candidate(
    county_id="pinellas",
    status="launched",
    waitlist_notified_at=None,
):
    c = MagicMock()
    c.county_id = county_id
    c.status = status
    c.waitlist_notified_at = waitlist_notified_at
    return c


def _make_entry(
    id=1,
    email="user@test.com",
    phone_e164=None,
    sms_opt_in=False,
    name="Test User",
    vertical="roofing",
    status="waiting",
    notified_email_at=None,
):
    e = MagicMock()
    e.id = id
    e.email = email
    e.phone_e164 = phone_e164
    e.sms_opt_in = sms_opt_in
    e.name = name
    e.vertical = vertical
    e.status = status
    e.notified_email_at = notified_email_at
    return e


def _setup_db(session, candidates, entry_batches):
    """
    Configure session.execute side effects.
    First call → candidates query (scalars().all()).
    Second call → County display name (scalar_one_or_none).
    Subsequent calls → entry batches (scalars().all()), one per batch + empty terminator.
    """
    call_count = [0]
    county_result = MagicMock()
    county_result.scalar_one_or_none.return_value = "Pinellas"

    def side_effect(stmt):
        idx = call_count[0]
        call_count[0] += 1
        result = MagicMock()
        if idx == 0:
            result.scalars.return_value.all.return_value = candidates
        elif idx == 1:
            return county_result
        else:
            # entry batches: idx-2 is the batch index
            batch_idx = idx - 2
            batch = entry_batches[batch_idx] if batch_idx < len(entry_batches) else []
            result.scalars.return_value.all.return_value = batch
        return result

    session.execute.side_effect = side_effect


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestNormalSuccessfulLaunch:
    def test_sends_email_and_sms(self, mock_db_context, monkeypatch):
        candidate = _make_candidate()
        entry_email = _make_entry(id=1, email="a@test.com")
        entry_sms = _make_entry(id=2, email="b@test.com", phone_e164="+13125550001", sms_opt_in=True)

        _setup_db(mock_db_context, [candidate], [[entry_email, entry_sms], []])

        mock_email = MagicMock(return_value=True)
        mock_sms = MagicMock(return_value=True)
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_email", mock_email)
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_sms", mock_sms)

        result = run_waitlist_notifier(dry_run=False)

        assert result["processed"][0]["sent_email"] == 2
        assert result["processed"][0]["sent_sms"] == 1
        assert result["processed"][0]["failed"] == 0
        assert mock_email.call_count == 2
        assert mock_sms.call_count == 1
        assert candidate.waitlist_notified_at is not None
        assert entry_email.status == "notified"
        assert entry_sms.status == "notified"


class TestNoWaitlistEntries:
    def test_stamps_county_guard_with_zero_entries(self, mock_db_context, monkeypatch):
        candidate = _make_candidate()
        _setup_db(mock_db_context, [candidate], [[]])  # empty first batch

        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_email", MagicMock())
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_sms", MagicMock())

        result = run_waitlist_notifier(dry_run=False)

        assert result["processed"][0]["sent_email"] == 0
        assert result["processed"][0]["failed"] == 0
        # zero failures → guard stamped
        assert candidate.waitlist_notified_at is not None


class TestPartialFailureRetry:
    def test_guard_not_stamped_on_failure(self, mock_db_context, monkeypatch):
        candidate = _make_candidate()
        entry1 = _make_entry(id=1, email="good@test.com")
        entry2 = _make_entry(id=2, email="bad@test.com")

        _setup_db(mock_db_context, [candidate], [[entry1, entry2], []])

        call_count = [0]
        def flaky_email(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("SMTP timeout")
            return True

        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_email", flaky_email)
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_sms", MagicMock())

        result = run_waitlist_notifier(dry_run=False)

        assert result["processed"][0]["sent_email"] == 1
        assert result["processed"][0]["failed"] == 1
        # guard NOT stamped — county retried next tick
        assert candidate.waitlist_notified_at is None
        # first entry still notified
        assert entry1.status == "notified"


class TestAlreadyNotifiedNoop:
    def test_returns_no_pending_when_guard_set(self, mock_db_context, monkeypatch):
        # DB returns empty candidates list (waitlist_notified_at IS NOT NULL filter)
        call_count = [0]
        def side_effect(stmt):
            call_count[0] += 1
            result = MagicMock()
            result.scalars.return_value.all.return_value = []
            return result
        mock_db_context.execute.side_effect = side_effect

        mock_email = MagicMock()
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_email", mock_email)

        result = run_waitlist_notifier(dry_run=False)

        assert result == {"no_pending_counties": True}
        mock_email.assert_not_called()


class TestDryRunNoDbWrites:
    def test_dry_run_leaves_db_untouched(self, mock_db_context, monkeypatch):
        candidate = _make_candidate()
        entry = _make_entry(id=1, email="user@test.com")

        _setup_db(mock_db_context, [candidate], [[entry], []])

        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_email", MagicMock())
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_sms", MagicMock())

        result = run_waitlist_notifier(dry_run=True)

        assert result["processed"][0]["sent_email"] == 1
        # entry untouched — dry-run must not mutate entry fields
        assert entry.status == "waiting"
        assert entry.notified_email_at is None
        # candidate guard NOT stamped
        assert candidate.waitlist_notified_at is None
        # audit commit is allowed (records the dry-run); data commit is not
        # The only commit should be the _write_audit call, not a data commit
        assert mock_db_context.commit.call_count <= 1
