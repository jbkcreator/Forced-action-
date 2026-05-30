"""Phase 3 tests — Churn Risk scorer (churn_risk.py + config/churn.py).

All DB I/O is stubbed. No real DB required. Clock injected for determinism.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services.churn_risk import band_for, compute_churn_risk


_NOW = datetime(2026, 5, 30, 13, 0, 0, tzinfo=timezone.utc)


def _ns(**kwargs):
    return SimpleNamespace(**kwargs)


def _make_sub(**kwargs):
    defaults = dict(
        id=1,
        status="active",
        tier="wallet",
        recovery_day3_sent=False,
        recovery_day5_sent=False,
        missed_lead_count=0,
        disputed_count=0,
    )
    defaults.update(kwargs)
    return _ns(**defaults)


# ── band_for ──────────────────────────────────────────────────────────────


def test_band_mapping():
    """Score→band boundaries are inclusive and cover the full 0–100 range."""
    assert band_for(0) == "low"
    assert band_for(29) == "low"
    assert band_for(30) == "medium"
    assert band_for(59) == "medium"
    assert band_for(60) == "high"
    assert band_for(79) == "high"
    assert band_for(80) == "very_high"
    assert band_for(100) == "very_high"


def test_band_mapping_clips_out_of_range():
    """Values outside 0–100 are clipped before mapping."""
    assert band_for(-10) == "low"
    assert band_for(150) == "very_high"


# ── compute_churn_risk ────────────────────────────────────────────────────


def _build_db_mock(sub=None):
    """Return a mock DB whose .execute().scalar_one_or_none() returns `sub`."""
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = sub
    mock_db = MagicMock()
    mock_db.execute.return_value = mock_result
    return mock_db


@patch("src.services.churn_risk.inactivity_trajectory")
@patch("src.services.churn_risk.usage_slope")
@patch("src.services.churn_risk.payment_stress")
@patch("src.services.churn_risk.engagement_dampener")
def test_predicted_inactivity_at_none_for_healthy(m_damp, m_stress, m_slope, m_inact):
    """Active daily buyer with no risk signals → predicted_inactivity_at is None."""
    m_inact.return_value = {
        "score": 0.0,
        "raw": {"days_since_last_debit": 0.5, "median_interval_days": 1.0,
                "debit_count": 30, "personalized": True},
    }
    m_slope.return_value = {"score": 0.0, "raw": {"recent_7d_count": 5, "prior_7d_count": 5,
                                                    "recent_7d_volume": 50, "prior_7d_volume": 50,
                                                    "count_slope": 0.0}}
    m_stress.return_value = {"score": 0.0, "raw": {"in_grace": False, "recovery_day3_sent": False,
                                                    "recovery_day5_sent": False, "missed_lead_count": 0,
                                                    "disputed_count": 0}}
    m_damp.return_value = {"score": 0.0, "raw": {"recent_replies": 0, "recent_clicks": 0, "window_days": 7}}

    sub = _make_sub()
    db = _build_db_mock(sub)

    result = compute_churn_risk(1, db, now=_NOW)

    assert result["predicted_inactivity_at"] is None
    assert result["score"] == 0
    assert result["band"] == "low"


@patch("src.services.churn_risk.inactivity_trajectory")
@patch("src.services.churn_risk.usage_slope")
@patch("src.services.churn_risk.payment_stress")
@patch("src.services.churn_risk.engagement_dampener")
def test_predicted_inactivity_at_within_horizon_for_declining(m_damp, m_stress, m_slope, m_inact):
    """Declining subscriber → predicted_inactivity_at ≤ HORIZON_DAYS out."""
    from config.churn import HORIZON_DAYS

    m_inact.return_value = {
        "score": 0.9,
        "raw": {"days_since_last_debit": 3.5, "median_interval_days": 1.0,
                "debit_count": 20, "personalized": True},
    }
    m_slope.return_value = {"score": 0.8, "raw": {"recent_7d_count": 1, "prior_7d_count": 8,
                                                    "recent_7d_volume": 10, "prior_7d_volume": 80,
                                                    "count_slope": -0.875}}
    m_stress.return_value = {"score": 0.0, "raw": {"in_grace": False, "recovery_day3_sent": False,
                                                    "recovery_day5_sent": False, "missed_lead_count": 0,
                                                    "disputed_count": 0}}
    m_damp.return_value = {"score": 0.0, "raw": {"recent_replies": 0, "recent_clicks": 0, "window_days": 7}}

    sub = _make_sub()
    db = _build_db_mock(sub)

    result = compute_churn_risk(1, db, now=_NOW)

    assert result["predicted_inactivity_at"] is not None
    days_out = (result["predicted_inactivity_at"] - _NOW).total_seconds() / 86400
    assert days_out <= HORIZON_DAYS, f"Predicted {days_out:.1f}d out, expected ≤ {HORIZON_DAYS}"


@patch("src.services.churn_risk.inactivity_trajectory")
@patch("src.services.churn_risk.usage_slope")
@patch("src.services.churn_risk.payment_stress")
@patch("src.services.churn_risk.engagement_dampener")
def test_reason_string_nonempty_and_bounded(m_damp, m_stress, m_slope, m_inact):
    """Reason string is non-empty and ≤ 255 chars."""
    m_inact.return_value = {
        "score": 0.8,
        "raw": {"days_since_last_debit": 4.0, "median_interval_days": 1.0,
                "debit_count": 10, "personalized": True},
    }
    m_slope.return_value = {"score": 0.6, "raw": {"recent_7d_count": 2, "prior_7d_count": 8,
                                                    "recent_7d_volume": 20, "prior_7d_volume": 80,
                                                    "count_slope": -0.75}}
    m_stress.return_value = {"score": 0.55, "raw": {"in_grace": True, "recovery_day3_sent": True,
                                                     "recovery_day5_sent": False, "missed_lead_count": 0,
                                                     "disputed_count": 0}}
    m_damp.return_value = {"score": 0.0, "raw": {"recent_replies": 0, "recent_clicks": 0, "window_days": 7}}

    sub = _make_sub(status="grace")
    db = _build_db_mock(sub)

    result = compute_churn_risk(1, db, now=_NOW)

    assert result["reason"], "reason string is empty"
    assert len(result["reason"]) <= 255
    assert isinstance(result["reason"], str)


@patch("src.services.churn_risk.inactivity_trajectory")
@patch("src.services.churn_risk.usage_slope")
@patch("src.services.churn_risk.payment_stress")
@patch("src.services.churn_risk.engagement_dampener")
def test_weights_sum_and_breakdown_consistent(m_damp, m_stress, m_slope, m_inact):
    """Breakdown component contributions reconcile to the pre-dampener score."""
    m_inact.return_value = {
        "score": 0.6,
        "raw": {"days_since_last_debit": 3.0, "median_interval_days": 1.0,
                "debit_count": 10, "personalized": True},
    }
    m_slope.return_value = {"score": 0.4, "raw": {"recent_7d_count": 3, "prior_7d_count": 5,
                                                    "recent_7d_volume": 30, "prior_7d_volume": 50,
                                                    "count_slope": -0.4}}
    m_stress.return_value = {"score": 0.0, "raw": {"in_grace": False, "recovery_day3_sent": False,
                                                    "recovery_day5_sent": False, "missed_lead_count": 0,
                                                    "disputed_count": 0}}
    m_damp.return_value = {"score": 0.0, "raw": {"recent_replies": 0, "recent_clicks": 0, "window_days": 7}}

    sub = _make_sub()
    db = _build_db_mock(sub)

    result = compute_churn_risk(1, db, now=_NOW)

    # With dampener=0: final == raw; breakdown parts should sum to final
    breakdown = result["breakdown"]
    component_sum = (
        breakdown["inactivity_trajectory"]
        + breakdown["usage_slope"]
        + breakdown["payment_stress"]
    )
    assert component_sum == pytest.approx(result["score"] + breakdown["dampener_reduction"], abs=1.5)


@patch("src.services.churn_risk.inactivity_trajectory")
@patch("src.services.churn_risk.usage_slope")
@patch("src.services.churn_risk.payment_stress")
@patch("src.services.churn_risk.engagement_dampener")
def test_deterministic(m_damp, m_stress, m_slope, m_inact):
    """Same inputs + same injected clock → identical output."""
    def _setup():
        m_inact.return_value = {
            "score": 0.7,
            "raw": {"days_since_last_debit": 3.5, "median_interval_days": 1.0,
                    "debit_count": 15, "personalized": True},
        }
        m_slope.return_value = {"score": 0.5, "raw": {"recent_7d_count": 3, "prior_7d_count": 6,
                                                       "recent_7d_volume": 30, "prior_7d_volume": 60,
                                                       "count_slope": -0.5}}
        m_stress.return_value = {"score": 0.2, "raw": {"in_grace": False, "recovery_day3_sent": False,
                                                        "recovery_day5_sent": False, "missed_lead_count": 0,
                                                        "disputed_count": 0}}
        m_damp.return_value = {"score": 0.3, "raw": {"recent_replies": 1, "recent_clicks": 0, "window_days": 7}}

    sub = _make_sub()

    _setup()
    db1 = _build_db_mock(sub)
    result1 = compute_churn_risk(1, db1, now=_NOW)

    _setup()
    db2 = _build_db_mock(sub)
    result2 = compute_churn_risk(1, db2, now=_NOW)

    assert result1["score"] == result2["score"]
    assert result1["band"] == result2["band"]
    assert result1["reason"] == result2["reason"]


def test_subscriber_not_found_returns_safe_default():
    """Missing subscriber returns score=0 and band=low without raising."""
    db = _build_db_mock(sub=None)
    result = compute_churn_risk(999, db, now=_NOW)
    assert result["score"] == 0
    assert result["band"] == "low"
    assert result["predicted_inactivity_at"] is None
