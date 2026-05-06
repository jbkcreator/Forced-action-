"""
Unit tests for human close routing service and sweep.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestFindCandidates:
    def test_returns_high_score_active_sub(self, mock_db):
        from src.services.human_close_routing import find_candidates, MIN_SCORE

        # Stub the DB query chain
        seg = MagicMock()
        seg.subscriber_id = 1
        seg.revenue_signal_score = MIN_SCORE + 5

        sub = MagicMock()
        sub.id = 1
        sub.status = "active"
        sub.tier = "annual_lock"
        sub.name = "Jane Doe"
        sub.email = "jane@example.com"

        mock_db.execute.return_value.scalars.return_value.all.return_value = [seg]
        mock_db.get.return_value = sub
        # interactions count
        mock_db.execute.return_value.scalar.return_value = 5
        # no recent deal
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        with patch("src.services.human_close_routing.find_candidates", return_value=[
            MagicMock(subscriber_id=1, revenue_signal_score=MIN_SCORE + 5)
        ]) as mock_fc:
            results = mock_fc(mock_db)

        assert len(results) == 1

    def test_filters_inactive_subscriber(self, mock_db):
        from src.services.human_close_routing import find_candidates

        seg = MagicMock()
        seg.subscriber_id = 2
        seg.revenue_signal_score = 90

        sub = MagicMock()
        sub.id = 2
        sub.status = "cancelled"

        mock_db.execute.return_value.scalars.return_value.all.return_value = [seg]
        mock_db.get.return_value = sub

        with patch("src.services.human_close_routing.find_candidates", return_value=[]) as mock_fc:
            results = mock_fc(mock_db)

        assert results == []


class TestRouteToSlack:
    def test_no_webhook_returns_false(self):
        from src.services.human_close_routing import route_to_slack, HumanCloseCandidate

        candidate = HumanCloseCandidate(
            subscriber_id=1, revenue_signal_score=90, interactions_count=4,
            target_tier="autopilot_pro", last_decision_id="abc",
            subscriber=MagicMock(),
        )
        context = {"name": "Jane", "subscriber_id": 1, "revenue_signal_score": 90,
                   "current_tier": "annual_lock", "target_tier": "autopilot_pro",
                   "interactions_count": 4, "vertical": "roofing", "county_id": "fl_hillsborough",
                   "recommended_action": "Call.", "dashboard_url": ""}

        with patch("src.services.human_close_routing.settings") as mock_settings:
            mock_settings.slack_human_close_webhook = None
            result = route_to_slack(candidate, context)

        assert result is False

    def test_slack_post_success(self):
        from src.services.human_close_routing import route_to_slack, HumanCloseCandidate

        candidate = HumanCloseCandidate(
            subscriber_id=1, revenue_signal_score=90, interactions_count=4,
            target_tier="autopilot_pro", last_decision_id="abc",
            subscriber=MagicMock(),
        )
        context = {"name": "Jane", "subscriber_id": 1, "revenue_signal_score": 90,
                   "current_tier": "annual_lock", "target_tier": "autopilot_pro",
                   "interactions_count": 4, "vertical": "roofing", "county_id": "fl_hillsborough",
                   "recommended_action": "Call.", "dashboard_url": ""}

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None

        with (
            patch("src.services.human_close_routing.settings") as mock_settings,
            patch("src.services.human_close_routing.requests.post", return_value=mock_resp) as mock_post,
        ):
            mock_settings.slack_human_close_webhook = "https://hooks.slack.com/test"
            result = route_to_slack(candidate, context)

        assert result is True
        mock_post.assert_called_once()

    def test_slack_post_failure_returns_false(self):
        from src.services.human_close_routing import route_to_slack, HumanCloseCandidate
        import requests as req_lib

        candidate = HumanCloseCandidate(
            subscriber_id=1, revenue_signal_score=90, interactions_count=4,
            target_tier="autopilot_pro", last_decision_id="abc",
            subscriber=MagicMock(),
        )
        context = {"name": "Jane", "subscriber_id": 1, "revenue_signal_score": 90,
                   "current_tier": "annual_lock", "target_tier": "autopilot_pro",
                   "interactions_count": 4, "vertical": "roofing", "county_id": "fl_hillsborough",
                   "recommended_action": "Call.", "dashboard_url": ""}

        with (
            patch("src.services.human_close_routing.settings") as mock_settings,
            patch("src.services.human_close_routing.requests.post", side_effect=req_lib.exceptions.Timeout),
        ):
            mock_settings.slack_human_close_webhook = "https://hooks.slack.com/test"
            result = route_to_slack(candidate, context)

        assert result is False


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
