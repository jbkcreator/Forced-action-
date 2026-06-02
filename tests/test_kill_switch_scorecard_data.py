"""Phase 1 tests — consecutive_red_days, latest_color, open_incident_for."""
from unittest.mock import MagicMock, patch

import pytest

from src.services.kill_switch_scorecard_data import (
    consecutive_red_days,
    latest_color,
    open_incident_for,
    SNAPSHOTTED_METRICS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_with_rows(rows):
    """Mock Session.execute().fetchall() returning rows list."""
    fetchall_result = MagicMock()
    fetchall_result.fetchall.return_value = rows
    db = MagicMock()
    db.execute.return_value = fetchall_result
    return db


def _make_row(val):
    r = MagicMock()
    r.val = val
    return r


def _db_first(row):
    result = MagicMock()
    result.first.return_value = row
    db = MagicMock()
    db.execute.return_value = result
    return db


# ---------------------------------------------------------------------------
# consecutive_red_days
# ---------------------------------------------------------------------------

class TestConsecutiveRedDays:
    def test_streak_counts_only_trailing_red(self):
        # first_payment_rate: red < 20, green >= 30, yellow in between
        # Rows (newest first): 15(R), 14(R), 14(R), 25(Y), 14(R)  → streak=3
        rows = [_make_row(v) for v in [15, 14, 14, 25, 14]]
        db = _db_with_rows(rows)
        result = consecutive_red_days(db, "first_payment_rate", "hillsborough")
        assert result == 3

    def test_streak_resets_on_green(self):
        # Rows: 15(R), 50(G), 15(R), 15(R) → streak=1
        rows = [_make_row(v) for v in [15, 50, 15, 15]]
        db = _db_with_rows(rows)
        result = consecutive_red_days(db, "first_payment_rate", "hillsborough")
        assert result == 1

    def test_streak_zero_when_latest_not_red(self):
        rows = [_make_row(50)]  # green
        db = _db_with_rows(rows)
        result = consecutive_red_days(db, "first_payment_rate", "hillsborough")
        assert result == 0

    def test_streak_capped_at_window(self):
        # 10 red days, window=7 → 7
        rows = [_make_row(10) for _ in range(10)]
        db = _db_with_rows(rows)
        result = consecutive_red_days(db, "first_payment_rate", "hillsborough", max_window=7)
        assert result == 7

    def test_streak_none_for_unsnapshotted_metric(self):
        # free_tier_cost_ratio is NOT in _BASELINE_COLUMNS → None
        db = _db_with_rows([])
        result = consecutive_red_days(db, "free_tier_cost_ratio", "hillsborough")
        assert result is None

    def test_streak_none_for_county_profitability(self):
        db = _db_with_rows([])
        result = consecutive_red_days(db, "county_profitability", "hillsborough")
        assert result is None

    def test_empty_rows_returns_zero(self):
        db = _db_with_rows([])
        result = consecutive_red_days(db, "first_payment_rate", "hillsborough")
        assert result == 0


# ---------------------------------------------------------------------------
# latest_color
# ---------------------------------------------------------------------------

class TestLatestColor:
    def test_prefers_snapshot_over_redis(self):
        # Snapshot row has value 15 (red for first_payment_rate)
        row = _make_row(15.0)
        db = _db_first(row)
        with patch("src.services.kill_switch_scorecard_data.get_cached_metric", return_value=50.0):
            color, observed = latest_color(db, "first_payment_rate", "hillsborough")
        assert color == "red"
        assert observed == 15.0

    def test_falls_back_to_redis_when_no_snapshot_row(self):
        db = _db_first(None)
        with patch("src.services.kill_switch_scorecard_data.get_cached_metric", return_value=50.0):
            color, observed = latest_color(db, "first_payment_rate", "hillsborough")
        assert color == "green"
        assert observed == 50.0

    def test_unknown_when_no_data_anywhere(self):
        db = _db_first(None)
        with patch("src.services.kill_switch_scorecard_data.get_cached_metric", return_value=None):
            color, observed = latest_color(db, "first_payment_rate", "hillsborough")
        assert color == "unknown"
        assert observed is None

    def test_unsnapshotted_metric_uses_redis_only(self):
        # free_tier_cost_ratio has no _BASELINE_COLUMNS entry → Redis path.
        db = MagicMock()
        with patch("src.services.kill_switch_scorecard_data.get_cached_metric", return_value=30.0):
            color, observed = latest_color(db, "free_tier_cost_ratio", "hillsborough")
        # free_tier_cost_ratio lower_is_better green=40 → 30 <= 40 → green
        assert color == "green"
        assert observed == 30.0


# ---------------------------------------------------------------------------
# open_incident_for
# ---------------------------------------------------------------------------

class TestOpenIncidentFor:
    def test_returns_row_when_incident_open(self):
        incident = MagicMock()
        incident.metric_name = "first_payment_rate"
        result = MagicMock()
        result.first.return_value = incident
        db = MagicMock()
        db.execute.return_value = result

        row = open_incident_for(db, "first_payment_rate", "hillsborough")
        assert row is incident

    def test_returns_none_when_no_open_incident(self):
        result = MagicMock()
        result.first.return_value = None
        db = MagicMock()
        db.execute.return_value = result

        row = open_incident_for(db, "first_payment_rate", "hillsborough")
        assert row is None


# ---------------------------------------------------------------------------
# SNAPSHOTTED_METRICS set
# ---------------------------------------------------------------------------

class TestSnapshottedMetrics:
    def test_known_snapshotted_metrics_present(self):
        for m in ["first_payment_rate", "retention_30d", "sms_reply_rate",
                  "offer_acceptance_rate", "wallet_adoption", "lock_conversion"]:
            assert m in SNAPSHOTTED_METRICS

    def test_unsnapshotted_metrics_absent(self):
        for m in ["free_tier_cost_ratio", "county_profitability", "sms_cost_per_signup"]:
            assert m not in SNAPSHOTTED_METRICS
