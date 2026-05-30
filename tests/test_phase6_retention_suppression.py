"""Phase 6 tests — retention event suppression when churn risk is high.

Verifies _is_high_churn_risk and its integration into the run() loop.
All DB I/O stubbed. No real DB required.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.tasks.retention_event_producer import _is_high_churn_risk


_NOW = datetime(2026, 5, 30, 16, 0, 0, tzinfo=timezone.utc)


def _ns(**kwargs):
    return SimpleNamespace(**kwargs)


def _make_prediction(band="high", predicted_inactivity_at=None):
    """Latest churn_predictions row (the source _is_high_churn_risk now reads)."""
    return _ns(churn_risk_band=band, predicted_inactivity_at=predicted_inactivity_at)


def _build_db(prediction=None):
    db = MagicMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = prediction
    db.execute.return_value = result
    return db


# ── _is_high_churn_risk ───────────────────────────────────────────────────


@patch("src.tasks.retention_event_producer.datetime")
def test_high_churn_risk_suppresses(mock_dt):
    """band=high AND predicted_inactivity_at within horizon → True."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(_make_prediction("high", _NOW + timedelta(days=2)))
    assert _is_high_churn_risk(db, 1) is True


@patch("src.tasks.retention_event_producer.datetime")
def test_low_risk_does_not_suppress(mock_dt):
    """band=low → False."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(_make_prediction("low", _NOW + timedelta(days=1)))
    assert _is_high_churn_risk(db, 1) is False


@patch("src.tasks.retention_event_producer.datetime")
def test_high_risk_but_horizon_far_does_not_suppress(mock_dt):
    """band=high but predicted_inactivity_at > HORIZON_DAYS → False."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(_make_prediction("high", _NOW + timedelta(days=10)))  # 10 > HORIZON_DAYS=3
    assert _is_high_churn_risk(db, 1) is False


def test_no_prediction_row_does_not_suppress():
    """No churn_predictions row → False (safe default)."""
    db = _build_db(prediction=None)
    assert _is_high_churn_risk(db, 1) is False


@patch("src.tasks.retention_event_producer.datetime")
def test_suppresses_via_prediction_without_user_segments_row(mock_dt):
    """REGRESSION (snapshot coverage): suppression reads churn_predictions, not the
    UPDATE-only user_segments mirror that ~90% of subscribers lack. A high-risk
    prediction within horizon suppresses retention with no segment row involved.
    """
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(_make_prediction("very_high", _NOW + timedelta(days=1)))
    assert _is_high_churn_risk(db, 1) is True


# ── run() integration ─────────────────────────────────────────────────────


@patch("src.tasks.retention_event_producer._is_high_churn_risk", return_value=True)
@patch("src.tasks.retention_event_producer._is_deduplicated", return_value=False)
@patch("src.tasks.retention_event_producer.get_active_pause", return_value=None)
@patch("src.tasks.retention_event_producer.get_db_context")
def test_suppression_counted_in_results(mock_ctx, mock_pause, mock_dedup, mock_risk):
    """When churn risk suppresses, deferred_to_save counter increments."""
    from src.tasks.retention_event_producer import run
    from config.retention import RETENTION_CADENCE_DAYS

    sub = _ns(id=1, tier="wallet", status="active", created_at=_NOW - timedelta(days=60))

    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = False

    # execute().scalars().all() → [sub] for wallet tier
    result = MagicMock()
    result.scalars.return_value.all.return_value = [sub]
    session.execute.return_value = result
    mock_ctx.return_value = session

    results = run(dry_run=False)

    assert results["deferred_to_save"] >= 1
    assert results["events_emitted"] == 0


@patch("src.tasks.retention_event_producer._is_high_churn_risk", return_value=False)
@patch("src.tasks.retention_event_producer._is_deduplicated", return_value=False)
@patch("src.tasks.retention_event_producer._last_engagement", return_value=None)
@patch("src.tasks.retention_event_producer.get_active_pause", return_value=None)
@patch("src.tasks.retention_event_producer._emit_event")
@patch("src.tasks.retention_event_producer._mark_deduplicated")
@patch("src.tasks.retention_event_producer.get_db_context")
def test_low_risk_retention_still_fires(mock_ctx, mock_mark, mock_emit, mock_pause,
                                         mock_engage, mock_dedup, mock_risk):
    """Non-flagged subscriber (low churn risk) still gets retention event."""
    from src.tasks.retention_event_producer import run

    sub = _ns(
        id=2, tier="annual_lock", status="active",
        created_at=_NOW - timedelta(days=30),
    )

    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = False

    result = MagicMock()
    result.scalars.return_value.all.return_value = [sub]
    session.execute.return_value = result
    mock_ctx.return_value = session

    results = run(dry_run=False)

    assert results["deferred_to_save"] == 0
    assert mock_emit.called, "retention event should have been emitted"
