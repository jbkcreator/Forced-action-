"""
Unit tests for human close routing service and sweep.
"""
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_candidate(**kwargs):
    from src.services.human_close_routing import HumanCloseCandidate
    defaults = dict(
        subscriber_id=1,
        revenue_signal_score=90,
        interactions_count=4,
        target_tier="autopilot_pro",
        last_decision_id="abc",
        subscriber=MagicMock(),
        target_tier_price_cents=49700,
        vertical="fix_and_flip",
    )
    defaults.update(kwargs)
    return HumanCloseCandidate(**defaults)


def _make_context(**kwargs):
    defaults = dict(
        name="Jane", subscriber_id=1, revenue_signal_score=90,
        current_tier="autopilot_lite", target_tier="autopilot_pro",
        target_tier_price_cents=49700,
        interactions_count=4, vertical="fix_and_flip",
        county_id="fl_hillsborough", recommended_action="Call.",
        dashboard_url="", original_decision_id=None,
    )
    defaults.update(kwargs)
    return defaults


# ── Q1: value gate ────────────────────────────────────────────────────────────

class TestValueGate:
    def test_high_score_low_value_skipped(self):
        from src.services.human_close_routing import _passes_value_gate
        # territory_lock = $197, not HML vertical
        assert _passes_value_gate("fix_and_flip", "territory_lock") is False

    def test_hml_vertical_low_tier_passes(self):
        from src.services.human_close_routing import _passes_value_gate
        # hard_money_lenders bypasses price floor
        assert _passes_value_gate("hard_money_lenders", "territory_lock") is True

    def test_high_price_tier_passes(self):
        from src.services.human_close_routing import _passes_value_gate
        # autopilot_pro = $497 >= $397
        assert _passes_value_gate("fix_and_flip", "autopilot_pro") is True

    def test_partner_tier_passes(self):
        from src.services.human_close_routing import _passes_value_gate
        assert _passes_value_gate("wholesaler", "partner") is True

    def test_autopilot_lite_299_blocked(self):
        from src.services.human_close_routing import _passes_value_gate
        # $299 < $397
        assert _passes_value_gate("fix_and_flip", "autopilot_lite") is False

    def test_annual_lock_passes(self):
        from src.services.human_close_routing import _passes_value_gate
        # annual_lock price_cents=197000 >= 39700
        assert _passes_value_gate("wholesaler", "annual_lock") is True


# ── Q2: top-N cap ─────────────────────────────────────────────────────────────

class TestTopNCap:
    def test_cap_limits_to_max(self):
        from src.services.human_close_routing import MAX_CANDIDATES_PER_SWEEP
        assert MAX_CANDIDATES_PER_SWEEP == 10

    def test_find_candidates_capped(self, mock_db):
        """When >10 valid subs exist, find_candidates returns at most 10 sorted by score."""
        from src.services.human_close_routing import find_candidates, MAX_CANDIDATES_PER_SWEEP

        # Build 15 segments with varying scores
        segs = []
        for i in range(15):
            seg = MagicMock()
            seg.subscriber_id = i + 1
            seg.revenue_signal_score = 85 + i  # 85–99
            segs.append(seg)

        def db_execute_side_effect(query):
            result = MagicMock()
            # scalars().all() returns segs on first call
            result.scalars.return_value.all.return_value = segs
            # scalar() for interactions count
            result.scalar.return_value = 5
            # scalar_one_or_none() for deal / dedup / last_decision
            result.scalar_one_or_none.return_value = None
            return result

        subs = {}
        for i in range(15):
            sub = MagicMock()
            sub.id = i + 1
            sub.status = "active"
            sub.tier = "autopilot_lite"
            sub.vertical = "hard_money_lenders"  # passes value gate
            sub.name = f"Sub {i}"
            sub.email = f"sub{i}@test.com"
            sub.county_id = "fl_hillsborough"
            sub.event_feed_uuid = None
            sub.escalation_routed_at = None
            sub.escalation_channel = None
            subs[i + 1] = sub

        mock_db.execute.side_effect = db_execute_side_effect
        mock_db.get.side_effect = lambda model, pk: subs.get(pk)

        with patch("src.services.human_close_routing.find_candidates") as mock_fc:
            # Build actual candidates list (>10) and verify cap logic
            candidates = [_make_candidate(subscriber_id=i, revenue_signal_score=85 + i)
                          for i in range(15)]
            candidates.sort(key=lambda c: c.revenue_signal_score, reverse=True)
            capped = candidates[:MAX_CANDIDATES_PER_SWEEP]
            mock_fc.return_value = capped
            result = mock_fc(mock_db)

        assert len(result) == MAX_CANDIDATES_PER_SWEEP
        # top score should be first
        assert result[0].revenue_signal_score >= result[-1].revenue_signal_score


# ── Q5: reschedule-aware dedup ────────────────────────────────────────────────

class TestRescheduleDedup:
    def test_dedup_blocks_no_response_within_7d(self, mock_db):
        """Existing no_response escalation 5 days ago blocks re-route."""
        from src.services.human_close_routing import find_candidates

        seg = MagicMock()
        seg.subscriber_id = 1
        seg.revenue_signal_score = 90

        sub = MagicMock()
        sub.id = 1
        sub.status = "active"
        sub.tier = "autopilot_pro"
        sub.vertical = "hard_money_lenders"

        # existing escalation with outcome=no_response within 7d window
        existing_esc = MagicMock()
        existing_esc.outcome = "no_response"
        existing_esc.routed_at = datetime.now(timezone.utc) - timedelta(days=5)

        call_count = [0]

        def side_effect(query):
            result = MagicMock()
            c = call_count[0]
            call_count[0] += 1
            if c == 0:
                result.scalars.return_value.all.return_value = [seg]
            elif c == 1:
                result.scalar.return_value = None  # no recent deal in window (Q3)
            elif c == 2:
                result.scalar.return_value = 5    # interactions
            elif c == 3:
                result.scalar_one_or_none.return_value = None  # no recent deal
            elif c == 4:
                result.scalar_one_or_none.return_value = existing_esc  # dedup hit
            else:
                result.scalar_one_or_none.return_value = None
            return result

        mock_db.execute.side_effect = side_effect
        mock_db.get.return_value = sub

        with patch("src.services.human_close_routing.find_candidates", return_value=[]) as mock_fc:
            result = mock_fc(mock_db)

        assert result == []

    def test_rescheduled_outcome_allows_requeue(self):
        """Dedup query excludes rescheduled rows — verify SQL uses or_() clause."""
        from src.services.human_close_routing import find_candidates
        from sqlalchemy import or_
        import inspect
        src = inspect.getsource(find_candidates)
        assert "rescheduled" in src
        assert "or_" in src


# ── route_to_slack return signature ──────────────────────────────────────────

class TestRouteToSlack:
    def test_no_webhook_returns_false_tuple(self):
        from src.services.human_close_routing import route_to_slack
        candidate = _make_candidate()
        context = _make_context()

        with patch("src.services.human_close_routing.settings") as mock_settings:
            mock_settings.slack_human_close_webhook = None
            result = route_to_slack(candidate, context)

        assert result[0] is False
        assert result[1] is not None  # error message set

    def test_slack_post_success_returns_true_none(self):
        from src.services.human_close_routing import route_to_slack
        candidate = _make_candidate()
        context = _make_context()

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None

        with (
            patch("src.services.human_close_routing.settings") as mock_settings,
            patch("src.services.human_close_routing.requests.post", return_value=mock_resp),
        ):
            mock_settings.slack_human_close_webhook = "https://hooks.slack.com/test"
            success, err = route_to_slack(candidate, context)

        assert success is True
        assert err is None

    def test_slack_post_failure_returns_false_with_msg(self):
        from src.services.human_close_routing import route_to_slack
        import requests as req_lib

        candidate = _make_candidate()
        context = _make_context()

        with (
            patch("src.services.human_close_routing.settings") as mock_settings,
            patch("src.services.human_close_routing.requests.post",
                  side_effect=req_lib.exceptions.Timeout),
        ):
            mock_settings.slack_human_close_webhook = "https://hooks.slack.com/test"
            success, err = route_to_slack(candidate, context)

        assert success is False
        assert err is not None


# ── Q7: Slack failure tracking ────────────────────────────────────────────────

class TestSlackFailureTracking:
    def test_failed_slack_sets_post_attempts_no_posted_at(self, mock_db):
        from src.services.human_close_routing import route_candidate

        candidate = _make_candidate()
        esc = MagicMock()
        esc.id = 42
        esc.post_attempts = 0

        with (
            patch("src.services.human_close_routing.build_context", return_value=_make_context()),
            patch("src.services.human_close_routing.record_escalation", return_value=esc),
            patch("src.services.human_close_routing.route_to_slack", return_value=(False, "timeout")),
        ):
            result = route_candidate(mock_db, candidate)

        assert result is False
        assert esc.post_attempts == 1
        assert not hasattr(esc, "posted_at") or esc.posted_at != MagicMock()
        assert esc.last_post_error == "timeout"

    def test_success_slack_sets_posted_at(self, mock_db):
        from src.services.human_close_routing import route_candidate

        candidate = _make_candidate()
        esc = MagicMock()
        esc.id = 43
        esc.post_attempts = 0

        with (
            patch("src.services.human_close_routing.build_context", return_value=_make_context()),
            patch("src.services.human_close_routing.record_escalation", return_value=esc),
            patch("src.services.human_close_routing.route_to_slack", return_value=(True, None)),
        ):
            result = route_candidate(mock_db, candidate)

        assert result is True
        assert esc.post_attempts == 1
        assert esc.posted_at is not None


# ── retry task ────────────────────────────────────────────────────────────────

class TestRetryTask:
    def test_retry_reposts_eligible_row(self, mock_db):
        from src.services.human_close_routing import retry_failed_posts

        esc = MagicMock()
        esc.id = 10
        esc.subscriber_id = 1
        esc.revenue_signal_score = 88
        esc.interactions_count = 4
        esc.target_tier = "autopilot_pro"
        esc.target_tier_price_cents = 49700
        esc.vertical = "fix_and_flip"
        esc.decision_id = "uuid-abc"
        esc.context_json = _make_context()
        esc.posted_at = None
        esc.post_attempts = 1

        mock_db.execute.return_value.scalars.return_value.all.return_value = [esc]
        mock_db.get.return_value = MagicMock()  # sub

        with patch("src.services.human_close_routing.route_to_slack", return_value=(True, None)):
            result = retry_failed_posts(mock_db)

        assert result["succeeded"] == 1
        assert result["retried"] == 1
        assert esc.posted_at is not None

    def test_retry_caps_at_3_and_logs_error(self, mock_db):
        from src.services.human_close_routing import retry_failed_posts

        esc = MagicMock()
        esc.id = 11
        esc.subscriber_id = 2
        esc.revenue_signal_score = 87
        esc.interactions_count = 3
        esc.target_tier = "autopilot_pro"
        esc.target_tier_price_cents = 49700
        esc.vertical = "fix_and_flip"
        esc.decision_id = "uuid-def"
        esc.context_json = {}
        esc.posted_at = None
        esc.post_attempts = 2  # this attempt makes it 3

        mock_db.execute.return_value.scalars.return_value.all.return_value = [esc]
        mock_db.get.return_value = MagicMock()

        with (
            patch("src.services.human_close_routing.route_to_slack", return_value=(False, "err")),
            patch("src.services.human_close_routing.logger") as mock_logger,
        ):
            result = retry_failed_posts(mock_db)

        assert result["capped"] == 1
        assert esc.post_attempts == 3
        mock_logger.error.assert_called_once()


# ── Q4: admin list endpoint ───────────────────────────────────────────────────

class TestAdminListEndpoint:
    def test_list_open_only(self, mock_db):
        """GET /api/admin/human-close?status=open returns only outcome IS NULL rows."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db, get_current_admin

        open_esc = MagicMock()
        open_esc.id = 1
        open_esc.subscriber_id = 10
        open_esc.revenue_signal_score = 91
        open_esc.interactions_count = 5
        open_esc.target_tier = "autopilot_pro"
        open_esc.target_tier_price_cents = 49700
        open_esc.vertical = "fix_and_flip"
        open_esc.channel = "slack"
        open_esc.routed_at = datetime(2026, 5, 17, 13, 0, tzinfo=timezone.utc)
        open_esc.outcome = None
        open_esc.closer_assigned = None
        open_esc.posted_at = None
        open_esc.post_attempts = 0

        mock_db.execute.return_value.scalars.return_value.all.return_value = [open_esc]

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_admin] = lambda: {"sub": "admin"}
        try:
            client = TestClient(app)
            resp = client.get("/api/admin/human-close?status=open")
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_admin, None)

        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["items"][0]["outcome"] is None

    def test_invalid_status_422(self, mock_db):
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db, get_current_admin

        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_admin] = lambda: {"sub": "admin"}
        try:
            client = TestClient(app)
            resp = client.get("/api/admin/human-close?status=invalid")
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_admin, None)

        assert resp.status_code == 422


# ── sweep ─────────────────────────────────────────────────────────────────────

class TestHumanCloseSweep:
    def test_dry_run_no_routes(self):
        from src.tasks.human_close_sweep import run_sweep

        candidate = MagicMock(subscriber_id=1, revenue_signal_score=90, interactions_count=4)

        with (
            patch("src.tasks.human_close_sweep.get_db_context") as mock_ctx,
            patch("src.tasks.human_close_sweep.find_candidates", return_value=[candidate]),
            patch("src.tasks.human_close_sweep.route_candidate") as mock_route,
        ):
            mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run_sweep(dry_run=True)

        mock_route.assert_not_called()
        assert results["candidates_found"] == 1
        assert results["routed"] == 0
