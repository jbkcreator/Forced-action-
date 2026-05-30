"""
Unit tests for county_launch_pulse.

DB is fully mocked via MagicMock + monkeypatch. run_daily_pulse is patched
at the import level. Pattern follows test_county_waitlist_notifier.py.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.tasks.county_launch_pulse import run_county_launch_pulse

NOW = datetime(2026, 5, 29, 12, 0, 0, tzinfo=timezone.utc)
T25H_AGO = NOW - timedelta(hours=25)
T23H_AGO = NOW - timedelta(hours=23)
T48H_AGO = NOW - timedelta(hours=48)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_db_context(monkeypatch):
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)

    @contextmanager
    def _mock_ctx():
        yield session

    monkeypatch.setattr("src.tasks.county_launch_pulse.get_db_context", _mock_ctx)
    return session


def _make_candidate(
    county_id="pinellas",
    launched_at=T25H_AGO,
    revenue_pulse_sent_at=None,
):
    c = MagicMock()
    c.county_id = county_id
    c.launched_at = launched_at
    c.revenue_pulse_sent_at = revenue_pulse_sent_at
    return c


def _setup_candidates(session, candidates):
    result = MagicMock()
    result.scalars.return_value.all.return_value = candidates
    session.execute.return_value = result


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestPulseNotDueYet:
    def test_returns_no_pending_when_under_24h(self, mock_db_context, monkeypatch):
        # Simulate DB returning empty because launched_at > cutoff
        _setup_candidates(mock_db_context, [])

        mock_pulse = MagicMock()
        monkeypatch.setattr("src.tasks.county_launch_pulse.run_daily_pulse", mock_pulse)

        result = run_county_launch_pulse(dry_run=False)

        assert result == {"no_pending_counties": True}
        mock_pulse.assert_not_called()


class TestPulseDueAndSent:
    def test_calls_pulse_and_stamps_guard(self, mock_db_context, monkeypatch):
        candidate = _make_candidate(launched_at=T25H_AGO)
        _setup_candidates(mock_db_context, [candidate])

        mock_pulse = MagicMock(return_value={"sent": True, "message": "FA..."})
        monkeypatch.setattr("src.tasks.county_launch_pulse.run_daily_pulse", mock_pulse)

        result = run_county_launch_pulse(dry_run=False)

        mock_pulse.assert_called_once_with(county_id="pinellas", dry_run=False)
        assert result["processed"][0]["sent"] is True
        assert result["processed"][0]["county_id"] == "pinellas"
        # guard stamped
        assert candidate.revenue_pulse_sent_at is not None
        mock_db_context.commit.assert_called()


class TestPulseAlreadySentNoop:
    def test_already_stamped_returns_no_pending(self, mock_db_context, monkeypatch):
        # DB returns empty because revenue_pulse_sent_at IS NOT NULL filter
        _setup_candidates(mock_db_context, [])

        mock_pulse = MagicMock()
        monkeypatch.setattr("src.tasks.county_launch_pulse.run_daily_pulse", mock_pulse)

        result = run_county_launch_pulse(dry_run=False)

        assert result == {"no_pending_counties": True}
        mock_pulse.assert_not_called()


class TestDryRun:
    def test_dry_run_calls_pulse_but_does_not_stamp(self, mock_db_context, monkeypatch):
        candidate = _make_candidate(launched_at=T25H_AGO)
        _setup_candidates(mock_db_context, [candidate])

        mock_pulse = MagicMock(return_value={"sent": False, "dry_run": True, "message": "FA..."})
        monkeypatch.setattr("src.tasks.county_launch_pulse.run_daily_pulse", mock_pulse)

        result = run_county_launch_pulse(dry_run=True)

        mock_pulse.assert_called_once_with(county_id="pinellas", dry_run=True)
        # guard NOT stamped in dry-run — this is the key invariant
        assert candidate.revenue_pulse_sent_at is None
        # audit commit is allowed (records the dry-run); data commit is not
        assert mock_db_context.commit.call_count <= 1


class TestPulseSendFailureNoStamp:
    def test_exception_leaves_guard_null(self, mock_db_context, monkeypatch):
        candidate = _make_candidate(launched_at=T25H_AGO)
        _setup_candidates(mock_db_context, [candidate])

        mock_pulse = MagicMock(side_effect=RuntimeError("SMS provider down"))
        monkeypatch.setattr("src.tasks.county_launch_pulse.run_daily_pulse", mock_pulse)

        result = run_county_launch_pulse(dry_run=False)

        assert result["processed"][0]["sent"] is False
        # guard NOT stamped — next tick will retry
        assert candidate.revenue_pulse_sent_at is None
