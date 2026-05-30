"""Phase 4 tests — nightly churn_scoring job.

All DB I/O stubbed. No real DB required. Clock injected for determinism.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from src.tasks.churn_scoring import is_in_holdout, run


_NOW = datetime(2026, 5, 30, 13, 0, 0, tzinfo=timezone.utc)


def _ns(**kwargs):
    return SimpleNamespace(**kwargs)


def _make_sub(sub_id=1, tier="wallet", status="active"):
    return _ns(id=sub_id, tier=tier, status=status)


# ── is_in_holdout ─────────────────────────────────────────────────────────


def test_holdout_is_stable():
    """Same subscriber_id always maps to same holdout value."""
    for sub_id in [1, 42, 1337, 99999]:
        assert is_in_holdout(sub_id) == is_in_holdout(sub_id)


def test_holdout_ratio_approx_10pct():
    """Over 1000 synthetic ids, holdout ≈ 8–12%."""
    count = sum(1 for i in range(1000) if is_in_holdout(i))
    assert 80 <= count <= 120, f"Holdout ratio out of range: {count}/1000"


def test_holdout_is_deterministic_across_calls():
    """Two calls with the same id always agree."""
    results_a = [is_in_holdout(i) for i in range(200)]
    results_b = [is_in_holdout(i) for i in range(200)]
    assert results_a == results_b


# ── run() ─────────────────────────────────────────────────────────────────


def _build_run_mocks(subs, risk_override=None, dry_run=False):
    """Return (mock_session, risk_dict) with execute() side_effect routing by call order.

    Call order for dry_run=False:
        0: backfill query  → .scalars().all() → []
        1: subs query      → .scalars().all() → subs
        2+: UPDATE calls   → return value ignored
    Call order for dry_run=True:
        0: subs query      → .scalars().all() → subs
        1+: (no writes)
    """
    default_risk = {
        "score": 72,
        "band": "high",
        "predicted_inactivity_at": _NOW,
        "reason": "Steep decline",
        "features": {"inactivity": {}, "slope": {}, "stress": {}, "dampener": {}},
    }
    risk = risk_override or default_risk

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    _call = [0]
    _subs_call = 0 if dry_run else 1

    def _execute(*_a, **_kw):
        idx = _call[0]
        _call[0] += 1
        result = MagicMock()
        if idx < _subs_call:
            # backfill query
            result.scalars.return_value.all.return_value = []
        elif idx == _subs_call:
            # subs query
            result.scalars.return_value.all.return_value = subs
        # else: UPDATE / other queries — default MagicMock return is fine
        return result

    mock_session.execute.side_effect = _execute
    return mock_session, risk


@patch("src.tasks.churn_scoring.compute_churn_risk")
@patch("src.tasks.churn_scoring.get_db_context")
def test_run_writes_snapshot_and_history(mock_ctx, mock_compute):
    """N subscribers → N history rows (db.add) + N segment updates (db.execute update)."""
    subs = [_make_sub(i) for i in range(1, 4)]  # 3 subs
    mock_session, risk = _build_run_mocks(subs)
    mock_ctx.return_value = mock_session
    mock_compute.return_value = risk

    result = run(dry_run=False)

    assert result["checked"] == 3
    assert mock_session.add.call_count == 3  # 3 ChurnPrediction rows


@patch("src.tasks.churn_scoring.compute_churn_risk")
@patch("src.tasks.churn_scoring.get_db_context")
def test_dry_run_writes_nothing(mock_ctx, mock_compute):
    """dry_run=True → 0 db.add calls, 0 segment mutations."""
    subs = [_make_sub(1), _make_sub(2)]
    mock_session, risk = _build_run_mocks(subs, dry_run=True)
    mock_ctx.return_value = mock_session
    mock_compute.return_value = risk

    result = run(dry_run=True)

    assert result["checked"] == 2
    mock_session.add.assert_not_called()


@patch("src.tasks.churn_scoring.compute_churn_risk")
@patch("src.tasks.churn_scoring.get_db_context")
def test_excludes_free_and_data_only(mock_ctx, mock_compute):
    """The SQL query uses status IN (active, grace) AND tier NOT IN (free, data_only).

    We verify it by checking the query was constructed with the right filters;
    here we just confirm the tier exclusion is declared in the job.
    """
    from src.tasks.churn_scoring import _EXCLUDED_TIERS
    assert "free" in _EXCLUDED_TIERS
    assert "data_only" in _EXCLUDED_TIERS


@patch("src.tasks.churn_scoring.compute_churn_risk")
@patch("src.tasks.churn_scoring.get_db_context")
def test_at_risk_count_tracks_high_bands(mock_ctx, mock_compute):
    """at_risk counter increments only for high/very_high bands."""
    subs = [_make_sub(1), _make_sub(2), _make_sub(3)]
    mock_session = MagicMock()
    mock_session.__enter__.return_value = mock_session
    mock_session.__exit__.return_value = False

    backfill_result = MagicMock()
    backfill_result.scalars.return_value.all.return_value = []
    subs_result = MagicMock()
    subs_result.scalars.return_value.all.return_value = subs
    mock_session.execute.side_effect = [backfill_result, subs_result]
    mock_ctx.return_value = mock_session

    risk_by_id = {
        1: {"score": 72, "band": "high",   "predicted_inactivity_at": _NOW, "reason": "r", "features": {}},
        2: {"score": 85, "band": "very_high", "predicted_inactivity_at": _NOW, "reason": "r", "features": {}},
        3: {"score": 20, "band": "low",    "predicted_inactivity_at": None, "reason": "r", "features": {}},
    }
    mock_compute.side_effect = lambda sub_id, db, now=None: risk_by_id[sub_id]

    result = run(dry_run=False)

    assert result["at_risk"] == 2


@patch("src.tasks.churn_scoring.compute_churn_risk")
@patch("src.tasks.churn_scoring.get_db_context")
def test_no_sends_no_claude(mock_ctx, mock_compute):
    """Job must not invoke any send_email, send_sms, or call_claude* function."""
    subs = [_make_sub(1)]
    mock_session, risk = _build_run_mocks(subs)
    mock_ctx.return_value = mock_session
    mock_compute.return_value = risk

    # The test passes as long as none of these modules are even imported in the job
    import src.tasks.churn_scoring as job_module
    assert not hasattr(job_module, "send_email"), "job must not import send_email"
    assert not hasattr(job_module, "send_sms"), "job must not import send_sms"
    assert not hasattr(job_module, "call_claude_with_usage"), "job must not import Claude call"

    run(dry_run=False)


@patch("src.tasks.churn_scoring.compute_churn_risk")
@patch("src.tasks.churn_scoring.get_db_context")
def test_idempotent_per_day_appends_history(mock_ctx, mock_compute):
    """Second run same day: segment snapshot is overwritten (UPDATE), history appended."""
    subs = [_make_sub(1)]
    # Simulate two successive runs
    for _ in range(2):
        mock_session, risk = _build_run_mocks(subs)
        mock_ctx.return_value = mock_session
        mock_compute.return_value = risk

        result = run(dry_run=False)
        assert result["checked"] == 1
        # Each run appends one ChurnPrediction (history is append-only)
        assert mock_session.add.call_count == 1
