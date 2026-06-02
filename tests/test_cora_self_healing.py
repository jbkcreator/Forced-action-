"""
fa034 — Cora self-healing tests.

All DB I/O in the production code uses raw SQL via sa_text(). These tests
stub `session.execute(...).first()` / `.scalar()` results — no live DB, no
ORM construction. Pattern mirrors tests/test_scoring_validation_report.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Tiny pure functions — _grade
# ---------------------------------------------------------------------------

class TestGrade:

    def test_higher_is_better_grades(self):
        from src.tasks.cora_self_healing import _grade
        # first_payment_rate thresholds: green ≥30, yellow [20-30), red <20
        assert _grade("first_payment_rate", 35) == "green"
        assert _grade("first_payment_rate", 25) == "yellow"
        assert _grade("first_payment_rate", 18) == "red"
        assert _grade("first_payment_rate", 30) == "green"        # boundary

    def test_lower_is_better_grades(self):
        from src.tasks.cora_self_healing import _grade
        # cac_paid_channels thresholds: green ≤25, yellow (25-40], red >40
        assert _grade("cac_paid_channels", 20) == "green"
        assert _grade("cac_paid_channels", 30) == "yellow"
        assert _grade("cac_paid_channels", 50) == "red"
        assert _grade("cac_paid_channels", 25) == "green"         # boundary

    def test_unknown_metric_returns_unknown(self):
        from src.tasks.cora_self_healing import _grade
        assert _grade("not_a_real_metric", 50) == "unknown"

    def test_none_observed_returns_unknown(self):
        from src.tasks.cora_self_healing import _grade
        assert _grade("first_payment_rate", None) == "unknown"


# ---------------------------------------------------------------------------
# Fake-session pattern — matches the raw-SQL repo convention
# ---------------------------------------------------------------------------

class _FakeResult:
    """Stub for .first() / .scalar() returned by session.execute."""
    def __init__(self, *, first=None, scalar=None):
        self._first = first
        self._scalar = scalar

    def first(self):
        return self._first

    def scalar(self):
        return self._scalar


def _ns(**kwargs):
    """Row-like object with attribute access."""
    return SimpleNamespace(**kwargs)


class _FakeSession:
    """Returns canned results for each execute() call in order.

    Pass either a single _FakeResult (returned for every call) or a list
    of _FakeResult (popped FIFO).
    """
    def __init__(self, results):
        if not isinstance(results, list):
            results = [results]
        self._results = list(results)
        self.calls = []

    def execute(self, statement, params=None):
        sql_str = str(statement)
        self.calls.append({"sql": sql_str, "params": params})
        if not self._results:
            return _FakeResult()
        return self._results.pop(0)


# ---------------------------------------------------------------------------
# Incident lifecycle
# ---------------------------------------------------------------------------

class TestIncidentLifecycle:

    def test_new_incident_opens_when_metric_breaches(self):
        """1. metric below red threshold + no open incident → INSERT new row."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        # Session responses in order:
        # 1. _find_open_incident  → None (no open)
        # 2. _count_new_incidents_last_hour → 0
        # 3. _open_incident INSERT RETURNING id → row with id=42
        # 4. _find_open_incident (re-fetch for slack post) → open row
        session = _FakeSession([
            _FakeResult(first=None),
            _FakeResult(first=_ns(c=0)),
            _FakeResult(first=_ns(id=42)),
            _FakeResult(first=_ns(
                id=42, metric_name="first_payment_rate", severity="red",
                observed_value=18, threshold_value=20, baseline_value=None,
                breach_started=datetime.now(timezone.utc), action_taken="no_op",
                county_id="hillsborough", feature_name=None,
            )),
        ])
        counters = _Counters(kill_recs_today=0)
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=18), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session,
                metric_name="first_payment_rate",
                county_id="hillsborough",
                feature_name=None,
                counters=counters,
                dry_run=False,
            )
        assert result["result"] == "opened"
        assert result["severity"] == "red"
        assert counters.incidents_opened == 1
        mock_post.assert_called_once()
        # First call site arg of post_incident_alert is the incident row.
        assert mock_post.call_args.kwargs["kind"] == "new"

    def test_incident_stays_open_while_metric_still_bad(self):
        """2. open incident + still red + duration < threshold → observing."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=1, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=10),
            action_taken="no_op", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([_FakeResult(first=open_row)])
        counters = _Counters(kill_recs_today=0)
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing.post_incident_alert"):
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=counters, dry_run=False,
            )
        assert result["result"] == "observing"
        assert counters.actions_taken == 0

    def test_incident_closes_when_metric_recovers(self):
        """3. open incident + metric back in green → close (action='resolved')."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=7, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=20),
            action_taken="fallback_enabled", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([
            _FakeResult(first=open_row),
            _FakeResult(first=None),                  # close UPDATE returns nothing
        ])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=6), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=_Counters(0), dry_run=False,
            )
        assert result["result"] == "closed_resolved"
        # post_incident_alert called with kind='resolved'
        mock_post.assert_called_once()
        assert mock_post.call_args.kwargs["kind"] == "resolved"
        # An UPDATE SQL ran with breach_resolved=NOW().
        assert any("breach_resolved = NOW()" in c["sql"] for c in session.calls)


# ---------------------------------------------------------------------------
# Action triggers
# ---------------------------------------------------------------------------

class TestActionTriggers:

    def test_48hr_breach_triggers_fallback_for_lock_conversion(self):
        """4. open lock_conversion incident at >48h → Redis fallback flag + UPDATE."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=11, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=50),
            action_taken="no_op", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([
            _FakeResult(first=open_row),
            _FakeResult(first=None),         # _record_action UPDATE returns nothing
        ])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing._apply_fallback") as mock_apply, \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=_Counters(0), dry_run=False,
            )
        assert result["result"] == "fallback_enabled"
        assert result["flag"] == "lock_close_use_fallback"
        mock_apply.assert_called_once_with("lock_close_use_fallback")
        assert mock_post.call_args.kwargs["kind"] == "action_taken"

    def test_48hr_breach_escalates_for_first_payment(self):
        """5. open first_payment_rate incident at >48h → human_escalated, NO fallback."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=12, metric_name="first_payment_rate", severity="red",
            observed_value=18, threshold_value=20, baseline_value=30,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=49),
            action_taken="no_op", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([
            _FakeResult(first=open_row),
            _FakeResult(first=None),
        ])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=18), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=30), \
             patch("src.tasks.cora_self_healing._apply_fallback") as mock_apply, \
             patch("src.tasks.cora_self_healing._apply_ab_pause") as mock_ab, \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session, metric_name="first_payment_rate", county_id="hillsborough",
                feature_name=None, counters=_Counters(0), dry_run=False,
            )
        assert result["result"] == "human_escalated"
        mock_apply.assert_not_called()
        mock_ab.assert_not_called()
        assert mock_post.call_args.kwargs["kind"] == "human_required"

    def test_yellow_band_covers_sub_25pct_48h_escalation(self):
        """FA-2B-v9 ambiguity: first_payment_rate at 24% (within Yellow 20-30% band)
        for >48h → opens incident, then human_escalated.
        This pins that the Yellow band ALREADY covers the spec's "below 25% for 48h"
        trigger without any code change to the banding.

        Observed=24 → Yellow (≥20? yes, ≥30? no, <20? no)
        After 49h → action fires as human_escalated (first_payment_rate default policy).
        """
        from src.tasks.cora_self_healing import _process_metric, _Counters

        # 24% → yellow in the 20-30% band. Open for 49h (past 48h threshold).
        open_row = _ns(
            id=13, metric_name="first_payment_rate", severity="yellow",
            observed_value=24, threshold_value=30, baseline_value=30,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=49),
            action_taken="no_op", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([
            _FakeResult(first=open_row),
            _FakeResult(first=None),
        ])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=24), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=30), \
             patch("src.tasks.cora_self_healing._apply_fallback") as mock_apply, \
             patch("src.tasks.cora_self_healing._apply_ab_pause") as mock_ab, \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session, metric_name="first_payment_rate", county_id="hillsborough",
                feature_name=None, counters=_Counters(0), dry_run=False,
            )
        # First assertion: _grade says "yellow" for 24%
        # (confirmed by test_higher_is_better_grades in TestGrade)
        # Second assertion: after 49h it's escalated, not auto-corrected.
        assert result["result"] == "human_escalated"
        mock_apply.assert_not_called()
        mock_ab.assert_not_called()
        assert mock_post.call_args.kwargs["kind"] == "human_required"

    def test_yellow_24pct_opens_incident_then_observes_before_48h(self):
        """FA-2B-v9: first_payment_rate at 24% (Yellow) for <48h → opens
        incident when no incident exists, then observes until 48h threshold.

        This proves the self-healing pipeline starts tracking a <25% breach
        immediately and doesn't wait for the value to cross into Red.
        """
        from src.tasks.cora_self_healing import _process_metric, _Counters

        # No open incident yet, observed=24 (yellow), age irrelevant.
        session = _FakeSession([
            _FakeResult(first=None),              # no open incident
            _FakeResult(first=_ns(c=0)),          # no new incidents last hour
            _FakeResult(first=_ns(id=55)),         # INSERT returns id=55
            _FakeResult(first=_ns(
                id=55, metric_name="first_payment_rate", severity="yellow",
                observed_value=24, threshold_value=30, baseline_value=None,
                breach_started=datetime.now(timezone.utc), action_taken="no_op",
                county_id="hillsborough", feature_name=None,
            )),
        ])
        counters = _Counters(kill_recs_today=0)
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=24), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session, metric_name="first_payment_rate", county_id="hillsborough",
                feature_name=None, counters=counters, dry_run=False,
            )
        assert result["result"] == "opened"
        assert result["severity"] == "yellow"
        assert counters.incidents_opened == 1

        # Now simulate the next cron run: incident open, 24% still yellow,
        # age <48h → "observing"
        open_row = _ns(
            id=55, metric_name="first_payment_rate", severity="yellow",
            observed_value=24, threshold_value=30, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=10),
            action_taken="no_op", county_id="hillsborough", feature_name=None,
        )
        session2 = _FakeSession([_FakeResult(first=open_row)])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=24), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing.post_incident_alert"):
            result2 = _process_metric(
                session2, metric_name="first_payment_rate", county_id="hillsborough",
                feature_name=None, counters=_Counters(0), dry_run=False,
            )
        assert result2["result"] == "observing"

    def test_7d_red_triggers_kill_recommendation_only(self):
        """6. open red incident for >7d → action_taken='feature_killed' (recommendation)."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=15, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(days=8),
            action_taken="fallback_enabled", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([
            _FakeResult(first=open_row),
            _FakeResult(first=None),
        ])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing._apply_fallback") as mock_apply, \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            counters = _Counters(kill_recs_today=0)
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=counters, dry_run=False,
            )
        assert result["result"] == "kill_recommended"
        # Kill rec is RECOMMENDATION only — does NOT actually disable the feature.
        mock_apply.assert_not_called()
        assert mock_post.call_args.kwargs["kind"] == "kill_recommended"
        assert counters.kill_recs_today == 1


# ---------------------------------------------------------------------------
# Rate limits
# ---------------------------------------------------------------------------

class TestRateLimits:

    def test_max_actions_per_run_enforced(self):
        """8. counter at max_actions_per_run → skip new actions, log only."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=21, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=50),
            action_taken="no_op", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([_FakeResult(first=open_row)])
        counters = _Counters(0)
        counters.actions_taken = 3  # at the cap
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing._apply_fallback") as mock_apply, \
             patch("src.tasks.cora_self_healing.post_incident_alert"):
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=counters, dry_run=False,
            )
        assert result["result"] == "skipped_rate_limit_actions"
        mock_apply.assert_not_called()

    def test_max_feature_kill_per_day_enforced(self):
        """9. counter at max_feature_kill_recommendations_per_day → skip."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=22, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(days=8),
            action_taken="fallback_enabled", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([_FakeResult(first=open_row)])
        counters = _Counters(kill_recs_today=1)  # at the daily cap
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=counters, dry_run=False,
            )
        assert result["result"] == "skipped_rate_limit_kill"
        mock_post.assert_not_called()

    def test_max_new_incidents_per_hour_enforced(self):
        """10. >5 new incidents in last hour → don't open another."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        session = _FakeSession([
            _FakeResult(first=None),              # no open incident
            _FakeResult(first=_ns(c=5)),          # 5 already opened in last hour
        ])
        counters = _Counters(0)
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=18), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session, metric_name="first_payment_rate", county_id="hillsborough",
                feature_name=None, counters=counters, dry_run=False,
            )
        assert result["result"] == "skipped_rate_limit_new_incidents"
        mock_post.assert_not_called()
        assert counters.incidents_opened == 0


# ---------------------------------------------------------------------------
# Master switch + Slack/email fallback
# ---------------------------------------------------------------------------

class TestMasterSwitch:

    def test_master_kill_switch_disabled_is_noop(self):
        """15. CORA_SELF_HEALING_ENABLED=false → return 0 without DB/Redis."""
        from src.tasks.cora_self_healing import main

        with patch("src.tasks.cora_self_healing.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(cora_self_healing_enabled=False)
            with patch("src.tasks.cora_self_healing.run_self_healing") as mock_run:
                exit_code = main(argv=[])
        assert exit_code == 0
        mock_run.assert_not_called()


class TestSlackEmailFallback:

    def test_slack_disabled_falls_back_to_email(self):
        """13. cora_incident_slack_channel unset → send_alert is called."""
        from src.services.cora_slack import post_incident_alert

        incident = _ns(
            metric_name="first_payment_rate", severity="red",
            observed_value=18, threshold_value=20, baseline_value=30,
            county_id="hillsborough", feature_name=None, action_taken="no_op",
            duration_hours=None, breach_started=datetime.now(timezone.utc),
        )
        with patch("src.services.cora_slack.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(
                slack_bot_token=None,
                cora_incident_slack_channel=None,
            )
            with patch("src.services.email.send_alert", return_value=True) as mock_send_alert:
                ts = post_incident_alert(incident, kind="new", action_summary="open")
        assert ts == "email"
        mock_send_alert.assert_called_once()


# ---------------------------------------------------------------------------
# Revenue Pulse incident injection
# ---------------------------------------------------------------------------

class TestRevenuePulseIntegration:

    def test_revenue_pulse_includes_unresolved_incident(self):
        """7. Fake open incident → daily SMS uses it instead of learning card."""
        from src.tasks import revenue_pulse as rp

        # _format_cora_incident_alert is the helper; mock the session it sees.
        fake_session = MagicMock()
        fake_session.execute.return_value.first.return_value = _ns(
            metric_name="first_payment_rate",
            severity="red",
            observed_value=18,
            threshold_value=20,
            action_taken="human_escalated",
            breach_started=datetime.now(timezone.utc),
            county_id="hillsborough",
        )
        result = rp._format_cora_incident_alert(fake_session)
        assert result is not None
        assert "[RED]" in result
        assert "first_payment_rate" in result
        # Within the SMS alert slot budget (~140 chars).
        assert len(result) <= 140

    def test_revenue_pulse_alert_is_none_when_no_incident(self):
        from src.tasks import revenue_pulse as rp
        fake_session = MagicMock()
        fake_session.execute.return_value.first.return_value = None
        result = rp._format_cora_incident_alert(fake_session)
        assert result is None

    def test_weekly_summary_omits_when_no_activity(self):
        from src.tasks import revenue_pulse as rp
        fake_session = MagicMock()
        fake_session.execute.return_value.first.return_value = _ns(
            red_open=0, yellow_open=0, resolved_7d=0, kill_pending_7d=0,
        )
        result = rp._format_cora_incidents_weekly_summary(fake_session)
        assert result is None

    def test_weekly_summary_formats_counts_when_active(self):
        from src.tasks import revenue_pulse as rp
        fake_session = MagicMock()
        fake_session.execute.return_value.first.return_value = _ns(
            red_open=2, yellow_open=4, resolved_7d=3, kill_pending_7d=1,
        )
        result = rp._format_cora_incidents_weekly_summary(fake_session)
        assert "2 red" in result
        assert "4 yellow" in result
        assert "3 resolved" in result
        assert "1 kill-pending" in result


# ---------------------------------------------------------------------------
# Graph kill_switch_feature wiring (regression pins)
# ---------------------------------------------------------------------------

class TestGraphKillSwitchWiring:
    """14. Verify each Cora graph passes the correct kill_switch_feature value
    to run_decision_hierarchy. One assertion per graph touched in fa034."""

    def test_fomo_passes_lock_conversion(self):
        from src.agents.graphs import fomo
        assert fomo.KILL_SWITCH_FEATURE == "lock_conversion"
        # The hierarchy node sources from this constant — read the function
        # to confirm it's referenced (not a leftover None).
        import inspect
        src = inspect.getsource(fomo._node_hierarchy_check)
        assert "kill_switch_feature" in src
        assert "KILL_SWITCH_FEATURE" in src
        assert '"kill_switch_feature": None' not in src

    def test_abandonment_passes_first_payment_rate(self):
        from src.agents.graphs import abandonment
        assert abandonment.KILL_SWITCH_FEATURE == "first_payment_rate"
        import inspect
        # The hierarchy check is wired in _node_hierarchy_check (wave1+wave2
        # share it). Inspect the module source to confirm KILL_SWITCH_FEATURE
        # is referenced and the old fail-open None has been removed.
        src = inspect.getsource(abandonment)
        assert "KILL_SWITCH_FEATURE" in src
        # The previous comment-stuck "kill_switch_feature": None has been
        # replaced with the constant + live metric read.
        assert "kill_switch_observed_value" in src

    def test_wallet_to_lock_close_passes_observed_value(self):
        from src.agents.graphs import wallet_to_lock_close
        assert wallet_to_lock_close.KILL_SWITCH_FEATURE == "lock_conversion"
        import inspect
        # Source must include kill_switch_observed_value (the audit caught
        # this one missing — fa034 added the live-metric read).
        src = inspect.getsource(wallet_to_lock_close)
        assert "kill_switch_observed_value" in src

    def test_retention_already_wired(self):
        # retention.py was already wired pre-fa034 — pin it stays wired.
        from src.agents.graphs import retention
        assert retention.KILL_SWITCH_FEATURE == "retention_30d"
        import inspect
        src = inspect.getsource(retention)
        assert "kill_switch_observed_value" in src


# ---------------------------------------------------------------------------
# No-duplicate-incident behavior
# ---------------------------------------------------------------------------

class TestNoDuplicateIncidents:

    def test_no_duplicate_incidents_when_already_open(self):
        """11. Re-running the task with the same breach doesn't open a second
        row for the same (metric, county, feature)."""
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=99, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=10),
            action_taken="no_op", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([_FakeResult(first=open_row)])
        counters = _Counters(0)
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing.post_incident_alert"):
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=counters, dry_run=False,
            )
        # Result is "observing" — no INSERT happened.
        assert result["result"] == "observing"
        # Only one execute() call was made (the _find_open_incident SELECT).
        # No INSERT INTO cora_incident appears in the SQL traces.
        for call in session.calls:
            assert "INSERT INTO cora_incident" not in call["sql"]


# ---------------------------------------------------------------------------
# A/B engine reuse path
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase C — direct hierarchy-call tests for the fa034 wiring.
# These pin the exact kwargs each graph passes to run_decision_hierarchy.
# Bypasses the pre-existing happy-path scenario tests that fail at
# get_subscriber_profile (DB-fixture issue, unrelated to fa034).
# ---------------------------------------------------------------------------

class TestGraphDecisionHierarchyCall:
    """Pin that each graph passes the correct kill_switch_feature +
    kill_switch_observed_value when calling run_decision_hierarchy. These
    tests invoke the hierarchy node directly and inspect the call args —
    no DB fixtures, no full scenario traversal.
    """

    def test_fomo_calls_hierarchy_with_lock_conversion(self):
        from src.agents.graphs import fomo

        with patch("src.agents.graphs.fomo.run_decision_hierarchy") as mock_hier, \
             patch("src.agents.graphs.fomo.get_cached_metric", return_value=4.2):
            mock_hier.return_value = {
                "action_allowed": True,
                "kill_switch_color": "yellow",
                "use_fallback": False,
                "revenue_signal_score": 50,
            }
            state = {
                "subscriber_id": 1,
                "decision_id": "d1",
                "event_payload": {"lead_id": 100, "zip_code": "33647"},
                "subscriber_profile": {"id": 1, "name": "X", "vertical": "wholesalers"},
                "zip_activity": {},
                "competition_status": {},
            }
            fomo._node_hierarchy_check(state)

        mock_hier.assert_called_once()
        call_kwargs = mock_hier.call_args.args[0]
        assert call_kwargs["kill_switch_feature"] == "lock_conversion"
        assert call_kwargs["kill_switch_observed_value"] == 4.2
        # Sanity — the OLD fail-open None is gone.
        assert call_kwargs["kill_switch_feature"] is not None

    def test_abandonment_wave1_calls_hierarchy_with_first_payment_rate(self):
        from src.agents.graphs import abandonment

        with patch("src.agents.graphs.abandonment.run_decision_hierarchy") as mock_hier, \
             patch("src.agents.graphs.abandonment.get_cached_metric", return_value=32.0):
            mock_hier.return_value = {
                "action_allowed": True,
                "kill_switch_color": "green",
                "use_fallback": False,
                "revenue_signal_score": 70,
            }
            state = {
                "subscriber_id": 1,
                "decision_id": "d1",
                "event_payload": {},
                "subscriber_profile": {"id": 1, "name": "X"},
                "wave": abandonment.GRAPH_WAVE1,
            }
            abandonment._node_hierarchy_check(state)

        mock_hier.assert_called_once()
        call_kwargs = mock_hier.call_args.args[0]
        assert call_kwargs["kill_switch_feature"] == "first_payment_rate"
        assert call_kwargs["kill_switch_observed_value"] == 32.0
        assert call_kwargs["graph_name"] == abandonment.GRAPH_WAVE1

    def test_abandonment_wave2_also_uses_first_payment_rate(self):
        """Wave 2 shares the hierarchy node — same gate metric."""
        from src.agents.graphs import abandonment

        with patch("src.agents.graphs.abandonment.run_decision_hierarchy") as mock_hier, \
             patch("src.agents.graphs.abandonment.get_cached_metric", return_value=24.5):
            mock_hier.return_value = {
                "action_allowed": True,
                "kill_switch_color": "yellow",
                "use_fallback": True,
                "revenue_signal_score": 55,
            }
            state = {
                "subscriber_id": 1,
                "decision_id": "d1",
                "event_payload": {},
                "subscriber_profile": {"id": 1, "name": "X"},
                "wave": abandonment.GRAPH_WAVE2,
            }
            abandonment._node_hierarchy_check(state)

        mock_hier.assert_called_once()
        call_kwargs = mock_hier.call_args.args[0]
        assert call_kwargs["kill_switch_feature"] == "first_payment_rate"
        assert call_kwargs["graph_name"] == abandonment.GRAPH_WAVE2

    def test_wallet_to_lock_close_calls_hierarchy_with_lock_conversion(self):
        from src.agents.graphs import wallet_to_lock_close

        with patch("src.agents.graphs.wallet_to_lock_close.run_decision_hierarchy") as mock_hier, \
             patch("src.agents.graphs.wallet_to_lock_close.get_cached_metric", return_value=3.5):
            mock_hier.return_value = {
                "action_allowed": True,
                "kill_switch_color": "yellow",
                "use_fallback": False,
            }
            state = {
                "subscriber_id": 1,
                "decision_id": "d1",
                "event_payload": {"zip_code": "33647", "credits_spent": 42},
                "subscriber_profile": {"id": 1, "name": "X"},
            }
            wallet_to_lock_close._node_hierarchy_check(state)

        mock_hier.assert_called_once()
        call_kwargs = mock_hier.call_args.args[0]
        assert call_kwargs["kill_switch_feature"] == "lock_conversion"
        # The audit caught this one missing observed_value pre-fa034 — pin it.
        assert call_kwargs["kill_switch_observed_value"] == 3.5

    def test_no_graph_passes_kill_switch_feature_none(self):
        """Defense-in-depth: re-scan the post-fa034 source code to confirm
        nobody re-introduces fail-open None for our wired graphs."""
        import inspect
        from src.agents.graphs import fomo, abandonment, wallet_to_lock_close

        for mod in (fomo, abandonment, wallet_to_lock_close):
            src = inspect.getsource(mod._node_hierarchy_check)
            assert '"kill_switch_feature": None' not in src, (
                f"{mod.__name__} regressed to fail-open kill_switch=None"
            )

    def test_early_exit_when_terminal_status_set(self):
        """If a previous node set terminal_status, hierarchy_check returns {}
        without calling run_decision_hierarchy. Pin this behavior for all
        three rewired graphs so a metric-cache fetch can't accidentally happen
        on an already-aborted run."""
        from src.agents.graphs import fomo, abandonment, wallet_to_lock_close

        for mod in (fomo, abandonment, wallet_to_lock_close):
            with patch.object(mod, "run_decision_hierarchy") as mock_hier, \
                 patch.object(mod, "get_cached_metric") as mock_metric:
                state = {"subscriber_id": 1, "decision_id": "d1",
                         "terminal_status": "aborted",
                         "event_payload": {}, "subscriber_profile": {}}
                result = mod._node_hierarchy_check(state)
            assert result == {}, f"{mod.__name__} did not early-exit on terminal_status"
            mock_hier.assert_not_called()
            mock_metric.assert_not_called()


class TestAbEngineReuse:

    def test_ab_engine_pause_path_reused_when_auto_paused(self):
        """12. metric configured with auto_action_type='auto_paused' →
        ab_engine.should_rollback + complete_test are called rather than
        duplicating the rollback logic in self_healing.

        Currently no metric is configured with auto_paused in the shipped
        guardrails — the path exists for future use. Verify by patching
        KILL_SWITCH at module load time to flip lock_conversion to auto_paused.
        """
        from src.tasks import cora_self_healing
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=33, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(hours=50),
            action_taken="no_op", county_id="hillsborough", feature_name=None,
        )
        # Override the lock_conversion config to use auto_paused.
        patched_ks = dict(cora_self_healing.KILL_SWITCH)
        patched_ks["lock_conversion"] = dict(patched_ks["lock_conversion"])
        patched_ks["lock_conversion"]["auto_action_type"] = "auto_paused"
        patched_ks["lock_conversion"]["fallback_feature_flag"] = "fake_ab_test"
        patched_ks["lock_conversion"]["requires_approval"] = False

        session = _FakeSession([
            _FakeResult(first=open_row),
            _FakeResult(first=None),  # _record_action UPDATE
        ])

        with patch.object(cora_self_healing, "KILL_SWITCH", patched_ks), \
             patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.services.ab_engine.should_rollback", return_value=True) as mock_should, \
             patch("src.services.ab_engine.complete_test") as mock_complete, \
             patch("src.tasks.cora_self_healing.post_incident_alert") as mock_post:
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=_Counters(0), dry_run=False,
            )
        assert result["result"] == "ab_paused"
        mock_should.assert_called_once_with("fake_ab_test", session)
        mock_complete.assert_called_once_with("fake_ab_test", winner="a", db=session)
        assert mock_post.call_args.kwargs["kind"] == "action_taken"


# ---------------------------------------------------------------------------
# fa036 regression — kill recommendation writes a cora_playbook row
# ---------------------------------------------------------------------------

class TestFa036KillWritesPlaybook:
    """When an open red incident crosses the kill_after_red_days threshold,
    self-healing records action_taken='feature_killed' AND writes a
    cora_playbook row with status='recommended', authored_by='cora', and
    source_type='self_healing_kill'. The feature flag is NOT actually
    disabled — that requires explicit human adoption via the admin endpoint.
    """

    def test_kill_path_calls_upsert_with_correct_attribution(self):
        """13. fa036 — feature_killed action invokes upsert_recommendation
        with source_type='self_healing_kill', authored_by='cora', and the
        metric name as the source_id (canonical source_key dedupe).
        """
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=15, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(days=8),
            action_taken="fallback_enabled", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([
            _FakeResult(first=open_row),
            _FakeResult(first=None),   # _record_action UPDATE
        ])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing._apply_fallback") as mock_apply, \
             patch("src.tasks.cora_self_healing.post_incident_alert"), \
             patch("src.services.playbook_writer.upsert_recommendation") as mock_upsert:
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=_Counters(kill_recs_today=0),
                dry_run=False,
            )

        assert result["result"] == "kill_recommended"
        # Feature flag NOT actually disabled — recommendation only.
        mock_apply.assert_not_called()

        # The playbook write happened with the right attribution.
        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args.kwargs
        assert kwargs["source_type"] == "self_healing_kill"
        assert kwargs["source_id"] == "lock_conversion"
        assert kwargs["authored_by"] == "cora"
        assert kwargs["name"] == "kill_recommendation:lock_conversion"
        # Pattern carries enough context for the human reviewer.
        assert kwargs["pattern"]["metric"] == "lock_conversion"
        assert "age_days" in kwargs["pattern"]

    def test_kill_path_does_not_crash_when_playbook_write_fails(self):
        """14. fa036 — if upsert_recommendation raises, the self-healing loop
        still records the incident action. Playbook write is auxiliary; it
        must NEVER block the incident lifecycle (try/except contract).
        """
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=15, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(days=8),
            action_taken="fallback_enabled", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([
            _FakeResult(first=open_row),
            _FakeResult(first=None),
        ])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.tasks.cora_self_healing._apply_fallback") as mock_apply, \
             patch("src.tasks.cora_self_healing.post_incident_alert"), \
             patch("src.services.playbook_writer.upsert_recommendation",
                   side_effect=RuntimeError("simulated DB hiccup")):
            counters = _Counters(kill_recs_today=0)
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=counters, dry_run=False,
            )

        # Loop survives the playbook write failure.
        assert result["result"] == "kill_recommended"
        mock_apply.assert_not_called()
        # Counters still advanced — incident lifecycle was not blocked.
        assert counters.kill_recs_today == 1

    def test_dry_run_does_not_write_playbook(self):
        """15. fa036 — dry-run mode short-circuits before upsert_recommendation.
        Verifies the kill recommendation row is NOT created during dry-run
        smoke tests.
        """
        from src.tasks.cora_self_healing import _process_metric, _Counters

        open_row = _ns(
            id=15, metric_name="lock_conversion", severity="red",
            observed_value=2, threshold_value=3, baseline_value=None,
            breach_started=datetime.now(timezone.utc) - timedelta(days=8),
            action_taken="fallback_enabled", county_id="hillsborough", feature_name=None,
        )
        session = _FakeSession([_FakeResult(first=open_row)])
        with patch("src.tasks.cora_self_healing.get_cached_metric", return_value=2), \
             patch("src.tasks.cora_self_healing.compute_baseline", return_value=None), \
             patch("src.services.playbook_writer.upsert_recommendation") as mock_upsert:
            result = _process_metric(
                session, metric_name="lock_conversion", county_id="hillsborough",
                feature_name=None, counters=_Counters(0), dry_run=True,
            )
        assert result["result"] == "would_recommend_kill"
        mock_upsert.assert_not_called()
