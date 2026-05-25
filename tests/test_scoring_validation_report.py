"""
Stage E — scoring validation report.

The report runs three SQL-driven checks (tier distribution per county,
event-rate monotonicity per county, cross-county Ultra-Platinum parity)
and rolls them up into an overall PASS/WARN/FAIL. These tests stub the
session so we can pin the status logic and threshold edges without
spinning up Postgres.
"""

from unittest.mock import MagicMock

import pytest

from src.tasks.scoring_validation_report import (
    CROSS_COUNTY_FAIL_REL,
    CROSS_COUNTY_WARN_REL,
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_WARN,
    TIER_DISTRIBUTION_FAIL_UP_PCT,
    TIER_DISTRIBUTION_WARN_UP_PCT,
    TIER_ORDER,
    check_cross_county_up_parity,
    check_event_rate_monotonicity,
    check_tier_distribution,
)


# ---------------------------------------------------------------------------
# Session stubs
# ---------------------------------------------------------------------------

def _fake_session(rowsets):
    """A fake session whose execute().fetchall() returns the next rowset in order."""
    session = MagicMock()
    rows_iter = iter(rowsets)

    def _execute(_stmt, _params=None):
        result = MagicMock()
        result.fetchall.return_value = next(rows_iter)
        return result

    session.execute.side_effect = _execute
    return session


def _row(**kwargs):
    """Build a row-like object with attribute access (mirrors SQLAlchemy Row)."""
    return MagicMock(**kwargs)


# ---------------------------------------------------------------------------
# check_tier_distribution
# ---------------------------------------------------------------------------

class TestTierDistribution:

    def test_empty_shadow_table_warns(self):
        session = _fake_session([[]])
        result = check_tier_distribution(session)
        assert result.status == STATUS_WARN
        assert "empty" in result.detail.lower()

    def test_healthy_distribution_passes(self):
        # Hillsborough-shaped distribution: UP < 10% of total.
        rows = [
            _row(county_id="hillsborough", lead_tier="Ultra Platinum", cnt=200),
            _row(county_id="hillsborough", lead_tier="Platinum",       cnt=600),
            _row(county_id="hillsborough", lead_tier="Gold",           cnt=1500),
            _row(county_id="hillsborough", lead_tier="Silver",         cnt=4000),
            _row(county_id="hillsborough", lead_tier="Bronze",         cnt=20000),
        ]
        result = check_tier_distribution(_fake_session([rows]))
        assert result.status == STATUS_PASS

    def test_pinellas_94pct_up_fails(self):
        """The actual production state we're trying to fix."""
        rows = [
            _row(county_id="pinellas", lead_tier="Ultra Platinum", cnt=940),
            _row(county_id="pinellas", lead_tier="Platinum",       cnt=30),
            _row(county_id="pinellas", lead_tier="Gold",           cnt=20),
            _row(county_id="pinellas", lead_tier="Silver",         cnt=10),
            _row(county_id="pinellas", lead_tier="Bronze",         cnt=0),
        ]
        result = check_tier_distribution(_fake_session([rows]))
        assert result.status == STATUS_FAIL
        assert "pinellas" in result.detail.lower()
        assert "94" in result.detail  # 94.0%

    def test_warn_band_between_thresholds(self):
        """UP between WARN and FAIL thresholds → WARN, not FAIL."""
        # Build a distribution where UP is ~20% (in WARN band, below FAIL).
        assert TIER_DISTRIBUTION_WARN_UP_PCT < 20 < TIER_DISTRIBUTION_FAIL_UP_PCT
        rows = [
            _row(county_id="orange", lead_tier="Ultra Platinum", cnt=20),
            _row(county_id="orange", lead_tier="Bronze",         cnt=80),
        ]
        result = check_tier_distribution(_fake_session([rows]))
        assert result.status == STATUS_WARN

    def test_multi_county_one_failing_overall_fails(self):
        rows = [
            _row(county_id="hillsborough", lead_tier="Ultra Platinum", cnt=20),
            _row(county_id="hillsborough", lead_tier="Bronze",         cnt=2000),
            _row(county_id="pinellas",     lead_tier="Ultra Platinum", cnt=900),
            _row(county_id="pinellas",     lead_tier="Bronze",         cnt=50),
        ]
        result = check_tier_distribution(_fake_session([rows]))
        assert result.status == STATUS_FAIL
        assert "pinellas" in result.detail.lower()


# ---------------------------------------------------------------------------
# check_event_rate_monotonicity
# ---------------------------------------------------------------------------

class TestEventRateMonotonicity:

    def test_no_outcomes_warns(self):
        session = _fake_session([[]])
        result = check_event_rate_monotonicity(session, window_days=90)
        assert result.status == STATUS_WARN

    def test_monotonic_rates_pass(self):
        rows = [
            _row(county_id="hillsborough", lead_tier="Ultra Platinum",
                 total=100, transacted=20, event_rate_pct=20.0),
            _row(county_id="hillsborough", lead_tier="Platinum",
                 total=200, transacted=30, event_rate_pct=15.0),
            _row(county_id="hillsborough", lead_tier="Gold",
                 total=500, transacted=50, event_rate_pct=10.0),
            _row(county_id="hillsborough", lead_tier="Silver",
                 total=1000, transacted=50, event_rate_pct=5.0),
            _row(county_id="hillsborough", lead_tier="Bronze",
                 total=5000, transacted=100, event_rate_pct=2.0),
        ]
        result = check_event_rate_monotonicity(_fake_session([rows]), window_days=90)
        assert result.status == STATUS_PASS

    def test_inversion_fails(self):
        """The actual production state — Bronze > UP. Must fail loudly."""
        rows = [
            _row(county_id="hillsborough", lead_tier="Ultra Platinum",
                 total=100, transacted=1, event_rate_pct=1.0),
            _row(county_id="hillsborough", lead_tier="Bronze",
                 total=1000, transacted=23, event_rate_pct=2.34),
        ]
        result = check_event_rate_monotonicity(_fake_session([rows]), window_days=90)
        assert result.status == STATUS_FAIL
        assert "inversion" in result.detail.lower()

    def test_missing_tier_in_middle_is_skipped(self):
        """If a county has no Gold leads, monotonicity over the remaining tiers stays valid."""
        rows = [
            _row(county_id="hillsborough", lead_tier="Ultra Platinum",
                 total=50, transacted=10, event_rate_pct=20.0),
            _row(county_id="hillsborough", lead_tier="Platinum",
                 total=100, transacted=15, event_rate_pct=15.0),
            # No Gold row — should be skipped without breaking the walk.
            _row(county_id="hillsborough", lead_tier="Silver",
                 total=200, transacted=10, event_rate_pct=5.0),
            _row(county_id="hillsborough", lead_tier="Bronze",
                 total=1000, transacted=20, event_rate_pct=2.0),
        ]
        result = check_event_rate_monotonicity(_fake_session([rows]), window_days=90)
        assert result.status == STATUS_PASS


# ---------------------------------------------------------------------------
# check_cross_county_up_parity
# ---------------------------------------------------------------------------

class TestCrossCountyUpParity:

    def _up_only_rows(self, **county_up):
        """Build rows where every county has an Ultra-Platinum bucket with total≥30."""
        rows = []
        for cid, (total, rate) in county_up.items():
            rows.append(_row(
                county_id=cid, lead_tier="Ultra Platinum",
                total=total, transacted=int(total * rate / 100),
                event_rate_pct=rate,
            ))
        return rows

    def test_single_county_warns(self):
        # Need ≥2 counties to compare.
        rows = self._up_only_rows(hillsborough=(100, 10.0))
        result = check_cross_county_up_parity(_fake_session([rows]), window_days=90)
        assert result.status == STATUS_WARN

    def test_low_volume_county_excluded(self):
        # Pinellas has only 10 UP leads (< 30) so it should be excluded from
        # the parity comparison — leaving us with only 1 eligible county → WARN.
        rows = [
            _row(county_id="hillsborough", lead_tier="Ultra Platinum",
                 total=100, transacted=10, event_rate_pct=10.0),
            _row(county_id="pinellas",     lead_tier="Ultra Platinum",
                 total=10, transacted=0, event_rate_pct=0.0),
        ]
        result = check_cross_county_up_parity(_fake_session([rows]), window_days=90)
        assert result.status == STATUS_WARN

    def test_close_rates_pass(self):
        # 10% vs 11% → 10% relative spread → below WARN threshold → PASS.
        rows = self._up_only_rows(
            hillsborough=(100, 10.0),
            pinellas=(100, 11.0),
        )
        result = check_cross_county_up_parity(_fake_session([rows]), window_days=90)
        assert result.status == STATUS_PASS
        assert result.data["relative_spread"] == pytest.approx(0.1, rel=0.01)

    def test_wide_rates_fail(self):
        # 1% vs 10% → 900% relative spread → fail.
        rows = self._up_only_rows(
            hillsborough=(100, 10.0),
            pinellas=(100, 1.0),
        )
        result = check_cross_county_up_parity(_fake_session([rows]), window_days=90)
        assert result.status == STATUS_FAIL

    def test_zero_rate_county_treats_as_infinite_spread(self):
        rows = self._up_only_rows(
            hillsborough=(100, 5.0),
            pinellas=(100, 0.0),
        )
        result = check_cross_county_up_parity(_fake_session([rows]), window_days=90)
        # Can't pass with one county at 0% and another at 5% — must FAIL.
        assert result.status == STATUS_FAIL


# ---------------------------------------------------------------------------
# Threshold sanity — make sure WARN < FAIL, so the bands don't overlap
# ---------------------------------------------------------------------------

class TestThresholdsAreOrdered:

    def test_tier_distribution_thresholds(self):
        assert TIER_DISTRIBUTION_WARN_UP_PCT < TIER_DISTRIBUTION_FAIL_UP_PCT

    def test_cross_county_thresholds(self):
        assert CROSS_COUNTY_WARN_REL < CROSS_COUNTY_FAIL_REL

    def test_tier_order_canonical(self):
        # Order is load-bearing for the monotonicity walk.
        assert TIER_ORDER == ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]
