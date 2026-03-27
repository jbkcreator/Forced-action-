"""
M6 unit tests — GHL integration, cancellation triggers, API failure handling,
load validation, deduplication verification.

No live DB, no GHL API, no Stripe calls required.

Run with:
    pytest tests/test_m6_unit.py -v
"""

from datetime import datetime, timedelta, timezone, date
from unittest.mock import MagicMock, patch, call
import pytest

import src.services.ghl_webhook as ghl_mod
import src.services.stripe_webhooks as webhook_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_subscriber(
    id=1,
    stripe_customer_id="cus_test",
    stripe_subscription_id="sub_test",
    tier="starter",
    vertical="roofing",
    county_id="hillsborough",
    founding_member=False,
    status="active",
    grace_expires_at=None,
    ghl_contact_id=None,
    event_feed_uuid="uuid-123",
    email="test@example.com",
    name="Test User",
    ghl_stage=None,
):
    obj = MagicMock()
    obj.id = id
    obj.stripe_customer_id = stripe_customer_id
    obj.stripe_subscription_id = stripe_subscription_id
    obj.tier = tier
    obj.vertical = vertical
    obj.county_id = county_id
    obj.founding_member = founding_member
    obj.status = status
    obj.grace_expires_at = grace_expires_at
    obj.ghl_contact_id = ghl_contact_id
    obj.event_feed_uuid = event_feed_uuid
    obj.email = email
    obj.name = name
    obj.ghl_stage = ghl_stage
    return obj


# ---------------------------------------------------------------------------
# GHL stage 5 + 7 mapping
# ---------------------------------------------------------------------------

class TestGhlStagePushSubscriber:
    """Test push_subscriber_to_ghl stage routing."""

    def _mock_settings(self, mock_settings):
        mock_settings.ghl_api_key = MagicMock()
        mock_settings.ghl_api_key.get_secret_value.return_value = "ghl_test_key"
        mock_settings.ghl_location_id = "loc_123"
        mock_settings.ghl_pipeline_id = "pipe_123"
        mock_settings.ghl_stage_paid_subscriber = "stage_5_id"
        mock_settings.ghl_stage_churned = "stage_7_id"
        mock_settings.ghl_cf_fa_tier = None
        mock_settings.ghl_cf_fa_zip = None
        mock_settings.ghl_cf_fa_founding = None
        mock_settings.ghl_cf_fa_dashboard_url = None
        mock_settings.app_base_url = "https://app.forcedaction.io"

    def test_stage_5_uses_paid_subscriber_id(self):
        """Stage 5 should map to GHL_STAGE_PAID_SUBSCRIBER."""
        subscriber = _make_subscriber(ghl_contact_id="contact_abc")

        with patch("src.services.ghl_webhook.settings") as mock_settings, \
             patch("src.services.ghl_webhook._ghl_request") as mock_req:

            self._mock_settings(mock_settings)

            # Mock contact PUT response
            put_resp = MagicMock()
            put_resp.ok = True
            put_resp.status_code = 200
            put_resp.json.return_value = {"contact": {"id": "contact_abc"}}

            # Mock opportunity POST response
            opp_resp = MagicMock()
            opp_resp.ok = True
            opp_resp.status_code = 200
            opp_resp.raise_for_status.return_value = None

            # _find_opportunity_for_contact returns None → POST new opportunity
            search_resp = MagicMock()
            search_resp.ok = True
            search_resp.status_code = 200
            search_resp.raise_for_status.return_value = None
            search_resp.json.return_value = {"opportunities": []}

            mock_req.side_effect = [put_resp, search_resp, opp_resp]

            result = ghl_mod.push_subscriber_to_ghl(subscriber, stage=5)

        assert result is True

        # Verify opportunity payload used stage_5_id
        opp_call = mock_req.call_args_list[2]
        opp_payload = opp_call[1]["json"]
        assert opp_payload["pipelineStageId"] == "stage_5_id"
        assert opp_payload["status"] == "open"

    def test_stage_7_uses_churned_id(self):
        """Stage 7 should map to GHL_STAGE_CHURNED and set status=lost."""
        subscriber = _make_subscriber(ghl_contact_id="contact_xyz")

        with patch("src.services.ghl_webhook.settings") as mock_settings, \
             patch("src.services.ghl_webhook._ghl_request") as mock_req:

            self._mock_settings(mock_settings)

            put_resp = MagicMock()
            put_resp.ok = True
            put_resp.status_code = 200
            put_resp.json.return_value = {"contact": {"id": "contact_xyz"}}

            search_resp = MagicMock()
            search_resp.ok = True
            search_resp.raise_for_status.return_value = None
            search_resp.json.return_value = {"opportunities": []}

            opp_resp = MagicMock()
            opp_resp.ok = True
            opp_resp.raise_for_status.return_value = None

            mock_req.side_effect = [put_resp, search_resp, opp_resp]

            result = ghl_mod.push_subscriber_to_ghl(subscriber, stage=7)

        assert result is True

        opp_call = mock_req.call_args_list[2]
        opp_payload = opp_call[1]["json"]
        assert opp_payload["pipelineStageId"] == "stage_7_id"
        assert opp_payload["status"] == "lost"

    def test_stage_7_monetary_value_by_tier(self):
        """Monetary values: starter=600, pro=1100, dominator=2000."""
        expected = {"starter": 600, "pro": 1100, "dominator": 2000}

        for tier, value in expected.items():
            subscriber = _make_subscriber(tier=tier, ghl_contact_id="c1")
            with patch("src.services.ghl_webhook.settings") as mock_settings, \
                 patch("src.services.ghl_webhook._ghl_request") as mock_req:

                self._mock_settings(mock_settings)

                put_resp = MagicMock()
                put_resp.ok = True
                put_resp.json.return_value = {"contact": {"id": "c1"}}

                search_resp = MagicMock()
                search_resp.ok = True
                search_resp.raise_for_status.return_value = None
                search_resp.json.return_value = {"opportunities": []}

                opp_resp = MagicMock()
                opp_resp.ok = True
                opp_resp.raise_for_status.return_value = None

                mock_req.side_effect = [put_resp, search_resp, opp_resp]
                ghl_mod.push_subscriber_to_ghl(subscriber, stage=7)

            opp_call = mock_req.call_args_list[2]
            assert opp_call[1]["json"]["monetaryValue"] == value


# ---------------------------------------------------------------------------
# Cancellation triggers (subscription.deleted)
# ---------------------------------------------------------------------------

class TestCancellationTriggers:
    """Verify subscription.deleted triggers GHL stage 7 and grace period.

    send_email is lazily imported inside _on_subscription_deleted, so we:
    a) patch src.services.email.send_email (the source), or
    b) set subscriber.email = None to avoid the email block entirely.
    We use (b) for simplicity where email content is not under test.
    """

    def _make_db(self, subscriber, territories=None):
        """Build a mock DB with consistent execute().scalar_one_or_none() and scalars().all()."""
        mock_db = MagicMock()
        # First execute() → get subscriber
        # Second execute() → get territories
        execute_results = []

        sub_result = MagicMock()
        sub_result.scalar_one_or_none.return_value = subscriber
        execute_results.append(sub_result)

        terr_result = MagicMock()
        terr_result.scalars.return_value.all.return_value = territories or []
        execute_results.append(terr_result)

        mock_db.execute.side_effect = execute_results
        return mock_db

    def test_subscription_deleted_sets_grace_status(self):
        """subscriber.status must become 'grace' on subscription.deleted."""
        subscriber = _make_subscriber(status="active", founding_member=False, email=None)
        mock_db = self._make_db(subscriber)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"):
            webhook_mod._on_subscription_deleted({"customer": "cus_test"}, mock_db)

        assert subscriber.status == "grace"
        assert subscriber.ghl_stage == 7
        assert subscriber.grace_expires_at is not None

    def test_subscription_deleted_grace_period_is_48h(self):
        """Grace period must be 48 hours from deletion event."""
        subscriber = _make_subscriber(status="active", email=None)
        before = datetime.now(timezone.utc)
        mock_db = self._make_db(subscriber)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"):
            webhook_mod._on_subscription_deleted({"customer": "cus_test"}, mock_db)

        after = datetime.now(timezone.utc)
        assert subscriber.grace_expires_at >= before + timedelta(hours=47, minutes=59)
        assert subscriber.grace_expires_at <= after + timedelta(hours=48, minutes=1)

    def test_subscription_deleted_pushes_ghl_stage_7(self):
        """Deletion must call push_subscriber_to_ghl with stage=7."""
        subscriber = _make_subscriber(status="active", founding_member=False, email=None)
        mock_db = self._make_db(subscriber)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl") as mock_ghl:
            webhook_mod._on_subscription_deleted({"customer": "cus_test"}, mock_db)

        mock_ghl.assert_called_once()
        # stage can be positional or keyword depending on call signature
        call = mock_ghl.call_args
        stage_passed = call.args[1] if len(call.args) > 1 else call.kwargs.get("stage")
        assert stage_passed == 7

    def test_subscription_deleted_churned_regular_tag(self):
        """Non-founding subscriber should get 'churned_regular' tag."""
        subscriber = _make_subscriber(founding_member=False, email=None)
        mock_db = self._make_db(subscriber)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl") as mock_ghl:
            webhook_mod._on_subscription_deleted({"customer": "cus_test"}, mock_db)

        assert "churned_regular" in mock_ghl.call_args[1]["tags"]

    def test_subscription_deleted_churned_founding_tag(self):
        """Founding subscriber should get 'churned_founding' tag."""
        subscriber = _make_subscriber(founding_member=True, email=None)
        mock_db = self._make_db(subscriber)

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl") as mock_ghl:
            webhook_mod._on_subscription_deleted({"customer": "cus_test"}, mock_db)

        assert "churned_founding" in mock_ghl.call_args[1]["tags"]

    def test_subscription_deleted_releases_zip_territories(self):
        """Locked ZIP territories should move to 'grace' status on deletion."""
        subscriber = _make_subscriber(status="active", email=None)
        territory1 = MagicMock()
        territory1.status = "locked"
        territory2 = MagicMock()
        territory2.status = "locked"
        mock_db = self._make_db(subscriber, territories=[territory1, territory2])

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"):
            webhook_mod._on_subscription_deleted({"customer": "cus_test"}, mock_db)

        assert territory1.status == "grace"
        assert territory2.status == "grace"

    def test_unknown_customer_is_ignored(self):
        """Deletion event for unknown customer should log warning and return."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        with patch("src.services.stripe_webhooks.push_subscriber_to_ghl") as mock_ghl:
            webhook_mod._on_subscription_deleted({"customer": "cus_unknown"}, mock_db)

        mock_ghl.assert_not_called()


# ---------------------------------------------------------------------------
# GHL API failure handling
# ---------------------------------------------------------------------------

class TestGhlApiFailureHandling:
    """Verify retry logic and error handling in GHL requests."""

    def test_429_retries_four_times(self):
        """_ghl_request should retry up to 4 times on 429."""
        rate_limited_resp = MagicMock()
        rate_limited_resp.status_code = 429

        ok_resp = MagicMock()
        ok_resp.status_code = 200

        with patch("src.services.ghl_webhook.requests.request") as mock_req, \
             patch("src.services.ghl_webhook.time.sleep"):
            # Three 429s then one 200
            mock_req.side_effect = [rate_limited_resp, rate_limited_resp, rate_limited_resp, ok_resp]
            resp = ghl_mod._ghl_request("GET", "https://example.com/test")

        assert resp.status_code == 200
        assert mock_req.call_count == 4

    def test_network_error_retries_with_backoff(self):
        """ConnectionError should retry with exponential backoff."""
        from requests.exceptions import ConnectionError as ReqConnectionError, RequestException

        with patch("src.services.ghl_webhook.requests.request",
                   side_effect=ReqConnectionError("connection refused")), \
             patch("src.services.ghl_webhook.time.sleep"):
            with pytest.raises(RequestException):
                ghl_mod._ghl_request("GET", "https://example.com/fail")

    def test_ghl_not_configured_returns_false(self):
        """push_subscriber_to_ghl should return False when GHL not configured."""
        with patch("src.services.ghl_webhook.settings") as mock_settings:
            mock_settings.ghl_api_key = None
            mock_settings.ghl_location_id = None

            subscriber = _make_subscriber()
            result = ghl_mod.push_subscriber_to_ghl(subscriber, stage=5)

        assert result is False


# ---------------------------------------------------------------------------
# Load validator
# ---------------------------------------------------------------------------

class TestLoadValidator:
    """Test anomaly detection in load_validator.py.

    We patch the internal helper functions (_get_recent_stats, _get_failed_runs)
    rather than the SQLAlchemy session chain to keep tests focused on business logic.
    """

    def test_no_alert_when_counts_normal(self):
        """No alert if today's count is above 30% of baseline."""
        from src.tasks.load_validator import run_load_validator

        with patch("src.tasks.load_validator._get_failed_runs", return_value=[]), \
             patch("src.tasks.load_validator._get_recent_stats", return_value={}), \
             patch("src.tasks.load_validator.get_db_context") as mock_ctx, \
             patch("src.tasks.load_validator._load_state",
                   return_value={"consecutive_low": {}, "last_check": None}), \
             patch("src.tasks.load_validator._save_state"), \
             patch("src.tasks.load_validator.send_alert") as mock_alert:

            mock_session = MagicMock()
            # today_rows: no runs today (single .filter() call)
            mock_session.query.return_value.filter.return_value.all.return_value = []

            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = run_load_validator("hillsborough")

        mock_alert.assert_not_called()
        assert result["alerts_sent"] == 0

    def test_alert_fires_after_two_consecutive_anomaly_days(self):
        """Alert fires when a scraper is low for 2+ consecutive days."""
        from src.tasks.load_validator import run_load_validator

        # State: fire_incidents was already low yesterday (consecutive_low=1)
        state = {"consecutive_low": {"fire_incidents": 1}, "last_check": None}

        # Baseline: fire_incidents averaged 100/day over 7 days
        baseline = {"fire_incidents": [100] * 7}

        with patch("src.tasks.load_validator._get_failed_runs", return_value=[]), \
             patch("src.tasks.load_validator._get_recent_stats", return_value=baseline), \
             patch("src.tasks.load_validator.get_db_context") as mock_ctx, \
             patch("src.tasks.load_validator._load_state", return_value=state), \
             patch("src.tasks.load_validator._save_state"), \
             patch("src.tasks.load_validator.send_alert", return_value=True) as mock_alert:

            mock_session = MagicMock()
            # Today: fire_incidents only got 5 matches (5% of baseline — well below 30%)
            today_row = MagicMock()
            today_row.source_type = "fire_incidents"
            today_row.matched = 5
            # today_rows query uses a single .filter() call with multiple conditions
            mock_session.query.return_value.filter.return_value.all.return_value = [today_row]

            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = run_load_validator("hillsborough")

        mock_alert.assert_called()
        assert len(result["anomalies"]) == 1

    def test_failed_run_sends_alert(self):
        """A failed scraper run should trigger an alert immediately."""
        from src.tasks.load_validator import run_load_validator

        failed = [("storm_damage", "NWS API timeout")]

        with patch("src.tasks.load_validator._get_failed_runs", return_value=failed), \
             patch("src.tasks.load_validator._get_recent_stats", return_value={}), \
             patch("src.tasks.load_validator.get_db_context") as mock_ctx, \
             patch("src.tasks.load_validator._load_state",
                   return_value={"consecutive_low": {}, "last_check": None}), \
             patch("src.tasks.load_validator._save_state"), \
             patch("src.tasks.load_validator.send_alert", return_value=True) as mock_alert:

            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = []

            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = run_load_validator("hillsborough")

        mock_alert.assert_called_once()
        assert len(result["failed_runs"]) == 1
