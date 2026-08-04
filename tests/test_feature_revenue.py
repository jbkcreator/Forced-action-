"""LEARN-v2.2 T-LEARN-06 — feature-to-revenue aggregation unit tests.

Tests the pure _aggregate helper (no DB): n counting, reply-rate math, and the
MIN_N floor (below / at / above 30).
"""
from src.services.feature_revenue import MIN_N, _aggregate


def _obs(key, value, replied, count):
    return [(key, value, replied)] * count


def test_min_n_is_30():
    assert MIN_N == 30


def test_below_floor_is_insufficient_and_no_rate():
    # 29 observations — below MIN_N.
    obs = _obs("buyer_type", "flipper", True, 15) + _obs("buyer_type", "flipper", False, 14)
    (corr,) = _aggregate(obs)
    assert corr.n == 29
    assert corr.replies == 15
    assert corr.sufficient is False
    assert corr.reply_rate_pct is None


def test_at_floor_is_sufficient_with_rate():
    # Exactly 30 observations — at MIN_N, sufficient.
    obs = _obs("buyer_type", "landlord", True, 15) + _obs("buyer_type", "landlord", False, 15)
    (corr,) = _aggregate(obs)
    assert corr.n == 30
    assert corr.replies == 15
    assert corr.sufficient is True
    assert corr.reply_rate_pct == 50.0


def test_above_floor_rate_rounded_2dp():
    # 40 obs, 13 replies -> 32.5%.
    obs = _obs("equity_band", "high", True, 13) + _obs("equity_band", "high", False, 27)
    (corr,) = _aggregate(obs)
    assert corr.n == 40
    assert corr.replies == 13
    assert corr.sufficient is True
    assert corr.reply_rate_pct == 32.5


def test_multiple_feature_values_bucketed_independently():
    obs = (
        _obs("buyer_type", "flipper", True, 30)
        + _obs("buyer_type", "landlord", False, 30)
    )
    result = {(c.feature_key, c.feature_value): c for c in _aggregate(obs)}
    assert result[("buyer_type", "flipper")].reply_rate_pct == 100.0
    assert result[("buyer_type", "landlord")].reply_rate_pct == 0.0


def test_empty_observations_yields_no_correlations():
    assert _aggregate([]) == []
