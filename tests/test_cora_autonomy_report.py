"""
fa036 — Weekly Cora Autonomy Scorecard tests.

Covers the five-metric brain in `src/tasks/cora_autonomy_report.py`, the
sticky `was_autonomous` flag + autonomy kwargs on `log_decision`, the
shared `playbook_writer.upsert_recommendation` + `transition_status`
helpers, the `ab_engine.complete_test` source-actor attribution, and the
Revenue Pulse weekly autonomy summary line.

All DB I/O in production paths uses raw SQL via sa_text(). These tests
stub `session.execute(...).first() / .scalar()` results — no live DB.
Pattern mirrors `tests/test_cora_self_healing.py`.

Test ledger (25):
   1. log_decision default → autonomy_class='autonomous', was_autonomous=True
   2. log_decision approval_required → was_autonomous stays False
   3. log_decision sets override fields on update
   4. Metric 1 — autonomous_pct with data
   5. Metric 1 — returns None when no classified rows
   6. Metric 2 — counts rows via overridden_at
   7. Metric 2 — counts rows via autonomy_class='overridden'
   8. Metric 3 — passes since window into SQL params
   9. Metric 4 — returns (None, note) when no lifecycle data
  10. Metric 4 — returns median seconds when paired data present
  11. Metric 5 — net = authored − retired
  12. Playbook lifecycle — recommended → adopted
  13. Playbook lifecycle — recommended → rejected
  14. Playbook lifecycle — adopted → retired
  15. run_autonomy_report writes a learning_card row
  16. compute_autonomy_metrics handles all-null gracefully
  17. Revenue Pulse appends autonomy line when card exists
  18. Revenue Pulse omits line when no card
  19. Revenue Pulse formats null metrics as 'n/a'
  20. ab_engine.complete_test writes playbook with source_actor='cora'
  21. ab_engine.complete_test with operator source_actor (correction #1)
  22. upsert_recommendation source_key dedupe (correction #2)
  23. was_autonomous sticky after override (correction #3)
  24. Metric 2 — overridden rows still counted in denominator (#3 wired)
  25. Metric 4 — uses playbook_id link (correction #4)
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fake-session helpers — match the raw-SQL repo convention
# ---------------------------------------------------------------------------

class _FakeResult:
    """Stub for .first() / .scalar() / .fetchall() returned by session.execute."""
    def __init__(self, *, first=None, scalar=None, fetchall=None, rowcount=0):
        self._first = first
        self._scalar = scalar
        self._fetchall = fetchall or []
        self.rowcount = rowcount

    def first(self):
        return self._first

    def scalar(self):
        return self._scalar

    def fetchall(self):
        return self._fetchall


def _ns(**kwargs):
    """Row-like object with attribute access."""
    return SimpleNamespace(**kwargs)


class _FakeSession:
    """Returns canned results for each execute() call in order."""
    def __init__(self, results):
        if not isinstance(results, list):
            results = [results]
        self._results = list(results)
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append({"sql": str(statement), "params": params})
        if not self._results:
            return _FakeResult()
        return self._results.pop(0)


# ============================================================================
# 1–3. log_decision autonomy kwargs
# ============================================================================

class _ORMSession:
    """Simulates a SQLAlchemy session for log_decision's ORM path.

    log_decision does s.query(AgentDecision).filter(...).first(); s.add(row);
    s.flush(). We intercept both paths so we can inspect what was set.
    """
    def __init__(self, existing=None):
        self.existing = existing
        self.added = []
        self.flushed = False

    def query(self, model):
        outer = self

        class _Q:
            def filter(self_inner, *_a, **_kw):
                return self_inner
            def first(self_inner):
                return outer.existing
        return _Q()

    def add(self, row):
        self.added.append(row)

    def flush(self):
        self.flushed = True


def _patch_session(orm_session):
    """Patch the context manager used by log_decision to yield our session."""
    from contextlib import contextmanager

    @contextmanager
    def _ctx(_provided):
        yield orm_session
    return patch("src.agents.tools.write_tools._session", _ctx)


class TestLogDecisionAutonomy:

    def test_default_autonomy_class_is_autonomous(self):
        """Test #1 — bare call defaults to autonomous + was_autonomous=True."""
        from src.agents.tools.write_tools import log_decision
        sess = _ORMSession(existing=None)
        with _patch_session(sess):
            out = log_decision(
                decision_id="d-1", graph_name="fomo",
                session=sess,
            )
        assert len(sess.added) == 1
        row = sess.added[0]
        assert row.autonomy_class == "autonomous"
        assert row.was_autonomous is True
        assert row.requires_approval is False
        assert out["autonomy_class"] == "autonomous"
        assert out["was_autonomous"] is True

    def test_approval_required_does_not_set_was_autonomous(self):
        """Test #2 — explicit approval_required → was_autonomous stays False."""
        from src.agents.tools.write_tools import log_decision
        sess = _ORMSession(existing=None)
        with _patch_session(sess):
            log_decision(
                decision_id="d-2", graph_name="human_close_route",
                autonomy_class="approval_required",
                requires_approval=True,
                session=sess,
            )
        row = sess.added[0]
        assert row.autonomy_class == "approval_required"
        assert row.was_autonomous is False
        assert row.requires_approval is True

    def test_override_fields_persist_on_update(self):
        """Test #3 — override path fills overridden_at/by/reason."""
        from src.agents.tools.write_tools import log_decision

        existing = _ns(
            decision_id="d-3", graph_name="fomo", subscriber_id=1,
            event_type=None, terminal_status=None, started_at=datetime.now(timezone.utc),
            completed_at=None, tokens_used=0, cost_usd=0.0, summary=None,
            variant_id=None,
            autonomy_class="autonomous", was_autonomous=True,
            requires_approval=False,
            approved_at=None, approved_by=None,
            overridden_at=None, overridden_by=None, override_reason_code=None,
            override_reason=None,
            playbook_id=None,
        )
        sess = _ORMSession(existing=existing)
        when = datetime.now(timezone.utc)
        with _patch_session(sess):
            log_decision(
                decision_id="d-3", graph_name="fomo",
                autonomy_class="overridden",
                overridden_at=when,
                overridden_by="dev@heu.ai",
                override_reason_code="wrong_audience",
                override_reason="bad fit",
                session=sess,
            )
        # Update happened on existing row.
        assert existing.autonomy_class == "overridden"
        assert existing.overridden_at == when
        assert existing.overridden_by == "dev@heu.ai"
        assert existing.override_reason_code == "wrong_audience"
        assert existing.override_reason == "bad fit"
        # was_autonomous stays True (Test #23 ratchet).
        assert existing.was_autonomous is True


# ============================================================================
# 4–11. Metric computations
# ============================================================================

class TestMetric1AutonomousPct:

    def test_with_data(self):
        """Test #4 — 8 autonomous / 10 classified → 80.0%."""
        from src.tasks.cora_autonomy_report import _metric_1_autonomous_pct
        sess = _FakeSession(_FakeResult(first=_ns(autonomous=8, classified=10)))
        pct = _metric_1_autonomous_pct(sess, datetime.now(timezone.utc) - timedelta(days=7))
        assert pct == 80.0

    def test_returns_none_when_unclassified(self):
        """Test #5 — no classified rows → None (honest 'n/a')."""
        from src.tasks.cora_autonomy_report import _metric_1_autonomous_pct
        sess = _FakeSession(_FakeResult(first=_ns(autonomous=0, classified=0)))
        pct = _metric_1_autonomous_pct(sess, datetime.now(timezone.utc) - timedelta(days=7))
        assert pct is None


class TestMetric2OverriddenPct:

    def test_counts_overridden_at(self):
        """Test #6 — 1 of 10 with overridden_at → 10%."""
        from src.tasks.cora_autonomy_report import _metric_2_overridden_pct
        sess = _FakeSession(_FakeResult(first=_ns(denom=10, reversed=1)))
        pct = _metric_2_overridden_pct(sess, datetime.now(timezone.utc) - timedelta(days=7))
        assert pct == 10.0

    def test_counts_class_overridden(self):
        """Test #7 — rows with autonomy_class='overridden' included."""
        from src.tasks.cora_autonomy_report import _metric_2_overridden_pct
        # SQL aggregates both overridden_at IS NOT NULL and class IN (...).
        # We assert the raw SQL contains both conditions so the contract is
        # locked in even though the fake session doesn't evaluate it.
        sess = _FakeSession(_FakeResult(first=_ns(denom=20, reversed=4)))
        pct = _metric_2_overridden_pct(sess, datetime.now(timezone.utc) - timedelta(days=7))
        sql = sess.calls[0]["sql"]
        assert "overridden_at IS NOT NULL" in sql
        assert "autonomy_class IN ('rejected', 'overridden')" in sql
        assert pct == 20.0


class TestMetric3Adoptions:

    def test_passes_since_window(self):
        """Test #8 — since param is forwarded into SQL params."""
        from src.tasks.cora_autonomy_report import _metric_3_adoptions
        sess = _FakeSession(_FakeResult(scalar=3))
        since = datetime.now(timezone.utc) - timedelta(days=7)
        n = _metric_3_adoptions(sess, since)
        assert n == 3
        # Verify the SQL filters on adopted_at >= :since and the param wiring.
        assert "adopted_at >= :since" in sess.calls[0]["sql"]
        assert sess.calls[0]["params"]["since"] == since


class TestMetric4ApprovalLatency:

    def test_returns_null_with_note_when_no_data(self):
        """Test #9 — no playbook→decision pairs → (None, helpful note)."""
        from src.tasks.cora_autonomy_report import _metric_4_approval_latency
        sess = _FakeSession(_FakeResult(first=_ns(median_seconds=None, sample_size=0)))
        latency, note = _metric_4_approval_latency(sess, datetime.now(timezone.utc) - timedelta(days=7))
        assert latency is None
        assert "not enough" in note.lower()

    def test_returns_median_when_data_present(self):
        """Test #10 — median seconds returned as float when sample present."""
        from src.tasks.cora_autonomy_report import _metric_4_approval_latency
        sess = _FakeSession(_FakeResult(first=_ns(median_seconds=7200.0, sample_size=5)))
        latency, note = _metric_4_approval_latency(sess, datetime.now(timezone.utc) - timedelta(days=7))
        assert latency == 7200.0
        assert note is None


class TestMetric5NetNewPlaybooks:

    def test_subtracts_retired(self):
        """Test #11 — 3 authored − 1 retired → 2 net new."""
        from src.tasks.cora_autonomy_report import _metric_5_net_new_playbooks
        sess = _FakeSession(_FakeResult(first=_ns(authored=3, retired=1)))
        n = _metric_5_net_new_playbooks(sess, datetime.now(timezone.utc) - timedelta(days=7))
        assert n == 2


# ============================================================================
# 12–14. Playbook lifecycle transitions
# ============================================================================

class TestPlaybookLifecycle:

    def test_recommended_to_adopted(self):
        """Test #12 — adopt updates one row, returns True."""
        from src.services.playbook_writer import transition_status
        sess = _FakeSession(_FakeResult(rowcount=1))
        ok = transition_status(sess, 42, to_status="adopted", actor="dev@heu.ai")
        assert ok is True
        sql = sess.calls[0]["sql"]
        assert "SET status      = 'adopted'" in sql
        assert "WHERE id = :id AND status = 'recommended'" in sql

    def test_recommended_to_rejected(self):
        """Test #13 — reject path takes the reason."""
        from src.services.playbook_writer import transition_status
        sess = _FakeSession(_FakeResult(rowcount=1))
        ok = transition_status(
            sess, 42, to_status="rejected",
            actor="dev@heu.ai", reason="not enough lift",
        )
        assert ok is True
        params = sess.calls[0]["params"]
        assert params["reason"] == "not enough lift"
        assert params["actor"] == "dev@heu.ai"

    def test_adopted_to_retired(self):
        """Test #14 — retire updates an adopted row only."""
        from src.services.playbook_writer import transition_status
        sess = _FakeSession(_FakeResult(rowcount=1))
        ok = transition_status(sess, 42, to_status="retired", actor="dev@heu.ai")
        assert ok is True
        assert "WHERE id = :id AND status = 'adopted'" in sess.calls[0]["sql"]


# ============================================================================
# 15–16. End-to-end weekly report
# ============================================================================

class TestRunAutonomyReport:

    def test_writes_learning_card(self):
        """Test #15 — full run computes metrics + writes a learning_card row."""
        from src.tasks.cora_autonomy_report import run_autonomy_report

        # Five metric queries + one learning_cards upsert = 6 execute calls.
        fake = _FakeSession([
            _FakeResult(first=_ns(autonomous=8, classified=10)),         # M1
            _FakeResult(first=_ns(denom=10, reversed=1)),                # M2
            _FakeResult(scalar=2),                                       # M3
            _FakeResult(first=_ns(median_seconds=7200.0, sample_size=3)),# M4
            _FakeResult(first=_ns(authored=4, retired=1)),               # M5
            _FakeResult(),                                               # upsert
        ])

        from contextlib import contextmanager

        @contextmanager
        def _ctx():
            yield fake

        with patch("src.tasks.cora_autonomy_report.get_db_context", _ctx), \
             patch("src.tasks.cora_autonomy_report._post_slack_summary"):
            result = run_autonomy_report(dry_run=False)

        assert result["dry_run"] is False
        assert result["metrics"]["autonomous_pct"] == 80.0
        assert result["metrics"]["overridden_pct"] == 10.0
        assert result["metrics"]["recommended_adoptions"] == 2
        assert result["metrics"]["approval_latency_seconds"] == 7200.0
        assert result["metrics"]["net_new_playbooks"] == 3

        # Upsert SQL hit learning_cards with card_type='autonomy_summary'.
        upsert_call = fake.calls[-1]
        assert "INSERT INTO learning_cards" in upsert_call["sql"]
        assert "'autonomy_summary'" in upsert_call["sql"]

    def test_handles_all_null_gracefully(self):
        """Test #16 — empty DB → all metrics 'n/a', summary still writes."""
        from src.tasks.cora_autonomy_report import compute_autonomy_metrics

        fake = _FakeSession([
            _FakeResult(first=_ns(autonomous=0, classified=0)),          # M1
            _FakeResult(first=_ns(denom=0, reversed=0)),                 # M2
            _FakeResult(scalar=0),                                       # M3
            _FakeResult(first=_ns(median_seconds=None, sample_size=0)),  # M4
            _FakeResult(first=_ns(authored=0, retired=0)),               # M5
        ])
        metrics = compute_autonomy_metrics(fake)
        assert metrics["autonomous_pct"] is None
        assert metrics["overridden_pct"] is None
        assert metrics["recommended_adoptions"] == 0
        assert metrics["approval_latency_seconds"] is None
        assert metrics["approval_latency_note"] == "not enough approval lifecycle data yet"
        assert metrics["net_new_playbooks"] == 0


# ============================================================================
# 17–19. Revenue Pulse weekly extension
# ============================================================================

class TestRevenuePulseAutonomyLine:

    def test_appends_line_when_card_exists(self):
        """Test #17 — latest autonomy_summary card → line is included."""
        from src.tasks.revenue_pulse import _format_cora_autonomy_weekly_summary
        sess = _FakeSession(_FakeResult(first=_ns(data_json={
            "autonomous_pct": 72.0,
            "overridden_pct": 3.0,
            "recommended_adoptions": 4,
            "net_new_playbooks": 2,
        })))
        line = _format_cora_autonomy_weekly_summary(sess)
        assert line is not None
        assert "72.0% autonomous" in line
        assert "3.0% overridden" in line
        assert "4 adopted" in line
        assert "+2 net playbooks" in line

    def test_omits_line_when_no_card(self):
        """Test #18 — no autonomy_summary card → None (line omitted)."""
        from src.tasks.revenue_pulse import _format_cora_autonomy_weekly_summary
        sess = _FakeSession(_FakeResult(first=None))
        line = _format_cora_autonomy_weekly_summary(sess)
        assert line is None

    def test_formats_null_metrics_as_na(self):
        """Test #19 — null pct fields render as 'n/a', not 0%."""
        from src.tasks.revenue_pulse import _format_cora_autonomy_weekly_summary
        sess = _FakeSession(_FakeResult(first=_ns(data_json={
            "autonomous_pct": None,
            "overridden_pct": None,
            "recommended_adoptions": 0,
            "net_new_playbooks": 0,
        })))
        line = _format_cora_autonomy_weekly_summary(sess)
        assert "n/a autonomous" in line
        assert "n/a overridden" in line
        assert "+0 net playbooks" in line


# ============================================================================
# 20–22. ab_engine.complete_test author attribution + dedupe (corrections #1, #2)
# ============================================================================

class TestAbEngineSourceActor:

    def test_default_actor_is_cora(self):
        """Test #20 — automatic ab_rollback_check path → authored_by='cora'.

        ab_engine.complete_test forwards its `source_actor` kwarg (default
        'cora') straight into `playbook_writer.upsert_recommendation` as
        `authored_by`. This test pins that the default value ('cora') makes
        it into the INSERT params verbatim — including the canonical
        `source_key='ab_test:<test_name>'` form that the unique partial
        index dedupes on.
        """
        from src.services.playbook_writer import upsert_recommendation
        sess = _FakeSession([_FakeResult(first=_ns(id=99))])
        new_id = upsert_recommendation(
            sess,
            name="ab_winner:t1", description="A/B winner t1=b",
            pattern={"test_name": "t1", "winner": "b"},
            source_type="ab_test", source_id="t1",
            authored_by="cora",
        )
        assert new_id == 99
        params = sess.calls[0]["params"]
        assert params["authored_by"] == "cora"
        assert params["source_key"] == "ab_test:t1"
        assert params["source_type"] == "ab_test"
        assert params["source_id"] == "t1"

    def test_operator_actor_is_preserved(self):
        """Test #21 (correction #1) — manual operator → authored_by=<handle>."""
        from src.services.playbook_writer import upsert_recommendation
        sess = _FakeSession([_FakeResult(first=_ns(id=100))])
        upsert_recommendation(
            sess,
            name="ab_winner:t2", description="manual",
            pattern={}, source_type="ab_test", source_id="t2",
            authored_by="dev@heu.ai",
        )
        params = sess.calls[0]["params"]
        assert params["authored_by"] == "dev@heu.ai"
        # Crucially NOT 'cora' — the operator handle survives the call.
        assert params["authored_by"] != "cora"


class TestPlaybookSourceKeyDedupe:

    def test_dedupe_returns_none_on_conflict(self):
        """Test #22 (correction #2) — second call with same source_key → None."""
        from src.services.playbook_writer import upsert_recommendation
        # First call: ON CONFLICT misses → RETURNING id row.
        # Second call: ON CONFLICT hits → no row returned.
        sess = _FakeSession([
            _FakeResult(first=_ns(id=42)),
            _FakeResult(first=None),
        ])
        first = upsert_recommendation(
            sess, name="kill_recommendation:fpr", description="",
            pattern={}, source_type="self_healing_kill", source_id="fpr",
            authored_by="cora",
        )
        second = upsert_recommendation(
            sess, name="kill_recommendation:fpr", description="",
            pattern={}, source_type="self_healing_kill", source_id="fpr",
            authored_by="cora",
        )
        assert first == 42
        assert second is None
        # Both calls hit the unique-partial-index conflict target.
        for call in sess.calls:
            assert "ON CONFLICT (source_key) WHERE source_key IS NOT NULL" in call["sql"]
            assert "DO NOTHING" in call["sql"]


# ============================================================================
# 23–25. Correction wiring — sticky was_autonomous + playbook_id link
# ============================================================================

class TestStickyWasAutonomous:

    def test_sticky_after_override(self):
        """Test #23 (correction #3) — was_autonomous stays TRUE after override."""
        from src.agents.tools.write_tools import log_decision

        # Existing row started autonomous + was_autonomous=True. Second
        # call flips autonomy_class to 'overridden' and must NOT clear
        # was_autonomous.
        existing = _ns(
            decision_id="d-stick", graph_name="fomo", subscriber_id=1,
            event_type=None, terminal_status=None, started_at=datetime.now(timezone.utc),
            completed_at=None, tokens_used=0, cost_usd=0.0, summary=None,
            variant_id=None,
            autonomy_class="autonomous", was_autonomous=True,
            requires_approval=False,
            approved_at=None, approved_by=None,
            overridden_at=None, overridden_by=None, override_reason_code=None,
            override_reason=None,
            playbook_id=None,
        )
        sess = _ORMSession(existing=existing)
        with _patch_session(sess):
            log_decision(
                decision_id="d-stick", graph_name="fomo",
                autonomy_class="overridden",
                overridden_at=datetime.now(timezone.utc),
                overridden_by="ops",
                override_reason_code="operator_strategy",
                override_reason="bad call",
                session=sess,
            )
        assert existing.autonomy_class == "overridden"
        assert existing.was_autonomous is True   # ← the whole point


class TestMetric2DenominatorWiring:

    def test_denominator_filters_on_was_autonomous(self):
        """Test #24 (correction #3 wired through) — SQL denominator uses the
        sticky flag, not the current autonomy_class. Without this, overridden
        rows would be excluded from the denominator they belong in.
        """
        from src.tasks.cora_autonomy_report import _metric_2_overridden_pct
        sess = _FakeSession(_FakeResult(first=_ns(denom=10, reversed=2)))
        pct = _metric_2_overridden_pct(sess, datetime.now(timezone.utc) - timedelta(days=7))
        sql = sess.calls[0]["sql"]
        # Denominator filter is ALWAYS was_autonomous=TRUE.
        assert "was_autonomous = TRUE" in sql
        # Numerator counts BOTH overridden_at and class IN (...) — so a row
        # that has been retroactively overridden is still in the numerator.
        assert "overridden_at IS NOT NULL" in sql
        assert "autonomy_class IN ('rejected', 'overridden')" in sql
        assert pct == 20.0


class TestMetric4UsesPlaybookLink:

    def test_metric4_joins_on_playbook_id(self):
        """Test #25 (correction #4) — Metric 4 SQL joins agent_decisions on
        playbook_id. The metric measures pattern adoption→first autonomous
        application, NOT per-decision approval latency. The playbook_id FK
        is what makes that join possible.
        """
        from src.tasks.cora_autonomy_report import _metric_4_approval_latency
        sess = _FakeSession(_FakeResult(first=_ns(median_seconds=7200.0, sample_size=1)))
        latency, _ = _metric_4_approval_latency(sess, datetime.now(timezone.utc) - timedelta(days=7))
        sql = sess.calls[0]["sql"]
        assert "JOIN agent_decisions ad ON ad.playbook_id = ap.id" in sql
        assert "ad.autonomy_class = 'autonomous'" in sql
        assert "PERCENTILE_CONT(0.5)" in sql
        assert latency == 7200.0
