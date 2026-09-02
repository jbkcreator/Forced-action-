"""
Aggregate freshness-regression alert — distinct from heartbeat_monitor.py's
existing per-source alerts. Diffs the CURRENT fleet-wide fresh/stale split
against the split 4 days ago (default FRESHNESS_REGRESSION_BASELINE_DAYS),
recomputed via compute_heartbeats()'s own `now=` parameter rather than a new
time-series table — scraper_run_stats' history is permanent, so calling the
same, already-tested function with an older `now` genuinely recomputes
historical freshness.

Pure-function coverage only (newly_stale_labels / FreshnessRegression /
check_freshness_regression), all via monkeypatched compute_heartbeats() —
no DB access, no live scraper_run_stats reads.
"""
from datetime import datetime, timezone

from src.tasks.heartbeat_monitor import (
    FRESHNESS_REGRESSION_MIN_NEWLY_STALE,
    FreshnessRegression,
    Heartbeat,
    check_freshness_regression,
    effective_sla_minutes,
    newly_stale_labels,
)


def _beat(source_type, is_stale, county_id=None):
    return Heartbeat(
        source_type=source_type, sla_minutes=1500, last_success_at=None,
        age_minutes=0, is_stale=is_stale, county_id=county_id,
    )


class TestEffectiveSlaMinutes:
    """Regression for PR review finding: 'Thursday baseline excludes most of
    the monitored fleet'. A 4-day lookback from Thursday lands on Sunday —
    the off-day for nearly the whole Mon-Sat fleet. effective_sla_minutes()
    must extend the SLA the same way whether the evaluation point is a
    working day right after an off-day (existing case) or the off-day
    itself (new case, needed so a baseline landing there doesn't read a
    healthy source as already-stale)."""

    def test_working_day_right_after_an_off_day_gets_extended(self):
        # Monday (0), Sunday (6) is the off-day -> 1 day of extension.
        assert effective_sla_minutes(0, {6}, 1500) == 1500 + 1440

    def test_working_day_not_after_an_off_day_is_unchanged(self):
        # Tuesday (1), Sunday (6) is the off-day, not immediately before Tuesday.
        assert effective_sla_minutes(1, {6}, 1500) == 1500

    def test_evaluating_on_the_off_day_itself_gets_extended(self):
        # Sunday (6) itself is the off-day being evaluated (a baseline
        # snapshot landing on it) -> must extend, not use the raw SLA.
        assert effective_sla_minutes(6, {6}, 1500) == 1500 + 1440

    def test_multi_day_off_streak_extends_by_each_day(self):
        # A source off both Saturday and Sunday, evaluated on Monday ->
        # 2 days of extension.
        assert effective_sla_minutes(0, {5, 6}, 1500) == 1500 + 2 * 1440


class TestNewlyStaleLabels:
    def test_fresh_to_stale_is_newly_stale(self):
        now = [_beat("permits", is_stale=True)]
        baseline = [_beat("permits", is_stale=False)]
        assert newly_stale_labels(now, baseline) == ["permits"]

    def test_stale_to_stale_is_not_newly_stale(self):
        """Already stale at baseline too -> not a NEW regression, an
        ongoing one; per-source heartbeat alerts already cover this."""
        now = [_beat("permits", is_stale=True)]
        baseline = [_beat("permits", is_stale=True)]
        assert newly_stale_labels(now, baseline) == []

    def test_fresh_to_fresh_is_not_newly_stale(self):
        now = [_beat("permits", is_stale=False)]
        baseline = [_beat("permits", is_stale=False)]
        assert newly_stale_labels(now, baseline) == []

    def test_absent_from_baseline_is_skipped_not_counted(self):
        """A label only present now (e.g. an off-day skip at baseline time)
        has nothing to compare against — must not be treated as a
        regression by default-counting it in."""
        now = [_beat("permits", is_stale=True)]
        baseline = []
        assert newly_stale_labels(now, baseline) == []

    def test_multi_county_labels_compared_independently(self):
        now = [
            _beat("violations", is_stale=True, county_id="pinellas"),
            _beat("violations", is_stale=False, county_id="hillsborough"),
        ]
        baseline = [
            _beat("violations", is_stale=False, county_id="pinellas"),
            _beat("violations", is_stale=False, county_id="hillsborough"),
        ]
        assert newly_stale_labels(now, baseline) == ["violations/pinellas"]


class TestFreshnessRegressionDataclass:
    def test_below_threshold_is_not_a_regression(self):
        r = FreshnessRegression(
            now_fresh_count=39, now_total_count=41, baseline_fresh_count=41,
            baseline_total_count=41, baseline_days=4,
            newly_stale=["permits", "foreclosures"],
        )
        assert len(r.newly_stale) == 2 < FRESHNESS_REGRESSION_MIN_NEWLY_STALE
        assert r.is_regression is False

    def test_at_threshold_is_a_regression(self):
        r = FreshnessRegression(
            now_fresh_count=38, now_total_count=41, baseline_fresh_count=41,
            baseline_total_count=41, baseline_days=4,
            newly_stale=["permits", "foreclosures", "sunbiz"],
        )
        assert len(r.newly_stale) == FRESHNESS_REGRESSION_MIN_NEWLY_STALE
        assert r.is_regression is True

    def test_alert_subject_and_body_name_every_newly_stale_source(self):
        r = FreshnessRegression(
            now_fresh_count=38, now_total_count=41, baseline_fresh_count=41,
            baseline_total_count=41, baseline_days=4,
            newly_stale=["permits", "foreclosures", "sunbiz"],
        )
        subject = r.alert_subject()
        body = r.alert_body()
        assert "3" in subject
        assert "38/41" in subject
        assert "41/41" in subject
        for label in r.newly_stale:
            assert label in body
        assert "AGGREGATE" in body


class TestCheckFreshnessRegression:
    def test_diffs_against_a_four_day_baseline_by_default(self, monkeypatch):
        calls = []

        def fake_compute_heartbeats(now=None, include_off_days=False):
            calls.append((now, include_off_days))
            return [_beat("permits", is_stale=False)]

        monkeypatch.setattr(
            "src.tasks.heartbeat_monitor.compute_heartbeats", fake_compute_heartbeats
        )
        now = datetime(2026, 9, 2, tzinfo=timezone.utc)
        beats_now = [_beat("permits", is_stale=True)]

        result = check_freshness_regression(beats_now, now=now)

        assert result.baseline_days == 4
        assert calls == [(datetime(2026, 8, 29, tzinfo=timezone.utc), True)]
        assert result.newly_stale == ["permits"]
        assert result.now_fresh_count == 0
        assert result.now_total_count == 1
        assert result.baseline_fresh_count == 1
        assert result.baseline_total_count == 1

    def test_custom_baseline_days_is_honored(self, monkeypatch):
        calls = []

        def fake_compute_heartbeats(now=None, include_off_days=False):
            calls.append(now)
            return []

        monkeypatch.setattr(
            "src.tasks.heartbeat_monitor.compute_heartbeats", fake_compute_heartbeats
        )
        now = datetime(2026, 9, 2, tzinfo=timezone.utc)
        check_freshness_regression([], now=now, baseline_days=7)
        assert calls == [datetime(2026, 8, 26, tzinfo=timezone.utc)]

    def test_baseline_call_includes_off_days_so_a_thursday_lookback_sees_the_fleet(self, monkeypatch):
        """Regression for PR review finding: a fixed N-day lookback can land
        on a source's own off-day (e.g. this 4-day baseline from a Thursday
        falls on Sunday, the off-day for nearly the whole Mon-Sat fleet).
        Without include_off_days=True on the baseline call, those sources
        are silently absent from the baseline and can never be flagged as
        newly stale — exactly hiding the shared-dependency outage this
        check exists to catch."""
        seen_include_off_days = []

        def fake_compute_heartbeats(now=None, include_off_days=False):
            seen_include_off_days.append(include_off_days)
            return []

        monkeypatch.setattr(
            "src.tasks.heartbeat_monitor.compute_heartbeats", fake_compute_heartbeats
        )
        thursday = datetime(2026, 9, 3, tzinfo=timezone.utc)  # a Thursday
        assert thursday.weekday() == 3
        check_freshness_regression([], now=thursday)
        assert seen_include_off_days == [True]
