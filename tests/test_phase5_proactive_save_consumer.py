"""Phase 5 tests — proactive_save as churn_scoring consumer.

Tests _identify_risk with the churn-risk Trigger 1 + holdout/cooldown gates.
Firing reads the latest churn_predictions row (NOT user_segments); cooldown
reads MAX(save_offer_sent_at) across all of the subscriber's predictions.
All DB I/O stubbed. No real DB required.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.tasks.proactive_save import _identify_risk, _stamp_save_offer_sent_at


_NOW = datetime(2026, 5, 30, 15, 0, 0, tzinfo=timezone.utc)


def _ns(**kwargs):
    return SimpleNamespace(**kwargs)


def _make_sub(sub_id=1, tier="wallet", status="active", email="sub@test.com",
              grace_expires_at=None):
    return _ns(
        id=sub_id, tier=tier, status=status, email=email,
        grace_expires_at=grace_expires_at, name="Test Sub",
        event_feed_uuid="uuid-123", stripe_subscription_id="sub_abc",
        created_at=_NOW - timedelta(days=90), founding_member=False,
    )


def _make_prediction(band="high", predicted_in_days=2, in_holdout=False,
                     save_offer_sent_at=None):
    """Latest churn_predictions row. predicted_in_days=None → onset None."""
    predicted = (_NOW + timedelta(days=predicted_in_days)) if predicted_in_days is not None else None
    return _ns(
        churn_risk_band=band,
        predicted_inactivity_at=predicted,
        in_holdout=in_holdout,
        save_offer_sent_at=save_offer_sent_at,
    )


def _build_db(latest_prediction=None, last_offer_sent_at=None):
    """Mock DB sequencing the two scalar_one_or_none() reads _identify_risk makes:
    (1) latest churn_predictions row, (2) MAX(save_offer_sent_at).
    """
    call_count = [0]
    returns = [latest_prediction, last_offer_sent_at]

    def _execute(*_a, **_kw):
        result = MagicMock()
        idx = call_count[0]
        call_count[0] += 1
        result.scalar_one_or_none.return_value = returns[idx] if idx < len(returns) else None
        return result

    db = MagicMock()
    db.execute.side_effect = _execute
    return db


# ── Trigger 1: churn_risk ─────────────────────────────────────────────────


@patch("src.tasks.proactive_save.datetime")
def test_fires_when_band_high_and_within_horizon(mock_dt):
    """band=high AND predicted_inactivity_at 2 days out → trigger = 'churn_risk'."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(latest_prediction=_make_prediction("high", 2))
    assert _identify_risk(_make_sub(), db) == "churn_risk"


@patch("src.tasks.proactive_save.datetime")
def test_does_not_fire_when_horizon_far(mock_dt):
    """predicted_inactivity_at 6 days out (> HORIZON_DAYS=3) → no trigger."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(latest_prediction=_make_prediction("high", 6))
    assert _identify_risk(_make_sub(), db) is None


@patch("src.tasks.proactive_save.datetime")
def test_low_band_does_not_fire(mock_dt):
    """band not in FIRE_BANDS → no trigger even within horizon."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(latest_prediction=_make_prediction("medium", 1))
    assert _identify_risk(_make_sub(), db) is None


@patch("src.tasks.proactive_save.datetime")
def test_holdout_subscriber_skipped(mock_dt):
    """in_holdout=True → returns None even when band+horizon qualify."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(latest_prediction=_make_prediction("very_high", 1, in_holdout=True))
    assert _identify_risk(_make_sub(), db) is None


@patch("src.tasks.proactive_save.datetime")
def test_cooldown_blocks_resend(mock_dt):
    """Last offer 5 days ago (< COOLDOWN_DAYS=21) → no resend."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(
        latest_prediction=_make_prediction("high", 2),
        last_offer_sent_at=_NOW - timedelta(days=5),
    )
    assert _identify_risk(_make_sub(), db) is None


@patch("src.tasks.proactive_save.datetime")
def test_cooldown_expired_allows_resend(mock_dt):
    """Last offer 22 days ago (> COOLDOWN_DAYS=21) → allowed."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(
        latest_prediction=_make_prediction("high", 2),
        last_offer_sent_at=_NOW - timedelta(days=22),
    )
    assert _identify_risk(_make_sub(), db) == "churn_risk"


# ── Regression: cooldown survives the nightly fresh-row insert ────────────


@patch("src.tasks.proactive_save.datetime")
def test_cooldown_survives_fresh_nightly_prediction_row(mock_dt):
    """REGRESSION (cooldown defeat): churn_scoring appends a fresh prediction row
    every night with save_offer_sent_at=NULL, which becomes the *latest* row.
    The old code read cooldown from that latest row only → NULL → re-fired daily.

    Here the latest row has save_offer_sent_at=None, but an offer WAS sent 5 days
    ago (captured by MAX across all rows). Cooldown must still block.
    """
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    fresh_row = _make_prediction("very_high", 1, in_holdout=False, save_offer_sent_at=None)
    db = _build_db(
        latest_prediction=fresh_row,                     # today's fresh row, unstamped
        last_offer_sent_at=_NOW - timedelta(days=5),     # MAX over history = 5d ago
    )
    assert _identify_risk(_make_sub(), db) is None


# ── Regression: firing does not depend on a user_segments row ─────────────


@patch("src.tasks.proactive_save.datetime")
def test_fires_without_user_segments_row(mock_dt):
    """REGRESSION (snapshot coverage): ~90% of subscribers have no user_segments
    row, so the old segment-based read silently returned None and never fired.
    Firing now reads churn_predictions (written for every scored subscriber), so
    a high-risk prediction fires even with no segment row at all.

    The mock makes only the prediction read available; there is no segment read.
    """
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

    db = _build_db(latest_prediction=_make_prediction("high", 2))
    assert _identify_risk(_make_sub(), db) == "churn_risk"


# ── Trigger 2: payment_failure_day5 ──────────────────────────────────────


@patch("src.tasks.proactive_save.datetime")
@patch("src.tasks.proactive_save.settings")
def test_grace_trigger_still_works(mock_settings, mock_dt):
    """payment_failure_day5 fires when 5+ days in grace, even with no prediction row."""
    mock_dt.now.return_value = _NOW
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
    mock_settings.grace_period_hours = 72

    grace_entered_6d = _NOW - timedelta(days=6)
    grace_expires_6d = grace_entered_6d + timedelta(hours=72)
    sub = _make_sub(status="grace", grace_expires_at=grace_expires_6d)

    db = _build_db(latest_prediction=None)  # no churn prediction
    assert _identify_risk(sub, db) == "payment_failure_day5"


# ── save_offer_sent_at stamping ───────────────────────────────────────────


def test_save_offer_sent_at_recorded():
    """On successful churn_risk send, save_offer_sent_at is stamped on latest prediction."""
    pred = _make_prediction(save_offer_sent_at=None)
    db = MagicMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = pred
    db.execute.return_value = result

    _stamp_save_offer_sent_at(1, db)
    assert pred.save_offer_sent_at is not None


def test_stamp_handles_no_prediction_row():
    """No churn_predictions row → _stamp_save_offer_sent_at does not raise."""
    db = MagicMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    db.execute.return_value = result
    _stamp_save_offer_sent_at(999, db)  # must not raise


# ── Tier exclusion ─────────────────────────────────────────────────────────


def test_data_only_excluded():
    """data_only tier always returns None."""
    assert _identify_risk(_make_sub(tier="data_only"), MagicMock()) is None


def test_free_excluded():
    """free tier always returns None."""
    assert _identify_risk(_make_sub(tier="free"), MagicMock()) is None
