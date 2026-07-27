"""Unit tests for the onboarding preference step (PATCH /api/subscriber/onboarding/{feed_uuid})."""
from unittest.mock import MagicMock, patch

import pytest


def _eligible_county_db_responses(existing_prospect=None):
    """db.execute side_effect sequence for a referral county that passes
    both eligibility checks: (1) not already launched, (2) a known
    not-yet-launched expansion candidate. Third call is the existing-row
    lookup for the upsert — None inserts, a MagicMock updates it."""
    return [
        MagicMock(**{"scalar_one_or_none.return_value": None}),      # not in counties table
        MagicMock(**{"scalar_one_or_none.return_value": "queued"}),  # eligible expansion candidate
        MagicMock(**{"scalar_one_or_none.return_value": existing_prospect}),  # existing ReferralProspect?
    ]


class TestOnboardingRequestValidation:
    def test_valid_options_accepted(self):
        from src.api.subscriber_router import OnboardingRequest

        req = OnboardingRequest(preferred_property_type="single_family", investment_budget_band="50k_150k")
        assert req.preferred_property_type == "single_family"
        assert req.investment_budget_band == "50k_150k"

    def test_invalid_property_type_rejected(self):
        from src.api.subscriber_router import OnboardingRequest

        with pytest.raises(Exception):
            OnboardingRequest(preferred_property_type="mansion", investment_budget_band="50k_150k")

    def test_invalid_budget_band_rejected(self):
        from src.api.subscriber_router import OnboardingRequest

        with pytest.raises(Exception):
            OnboardingRequest(preferred_property_type="land", investment_budget_band="a_million_gazillion")

    def test_referral_fields_omitted_is_valid(self):
        """The referral ask is fully optional — omitting it entirely is fine."""
        from src.api.subscriber_router import OnboardingRequest

        req = OnboardingRequest(preferred_property_type="land", investment_budget_band="50k_150k")
        assert req.referral_prospect_name is None
        assert req.referral_target_county_id is None

    def test_referral_name_and_county_together_is_valid(self):
        from src.api.subscriber_router import OnboardingRequest

        req = OnboardingRequest(
            preferred_property_type="land", investment_budget_band="50k_150k",
            referral_prospect_name="Dana Roofing", referral_target_county_id="polk",
        )
        assert req.referral_prospect_name == "Dana Roofing"
        assert req.referral_target_county_id == "polk"

    def test_referral_name_without_county_rejected(self):
        from src.api.subscriber_router import OnboardingRequest

        with pytest.raises(Exception):
            OnboardingRequest(
                preferred_property_type="land", investment_budget_band="50k_150k",
                referral_prospect_name="Dana Roofing",
            )

    def test_referral_county_without_name_rejected(self):
        from src.api.subscriber_router import OnboardingRequest

        with pytest.raises(Exception):
            OnboardingRequest(
                preferred_property_type="land", investment_budget_band="50k_150k",
                referral_target_county_id="polk",
            )


class TestSubmitOnboardingEndpoint:
    def test_patch_persists_and_completes(self, mock_db):
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            resp = client.patch(
                "/api/subscriber/onboarding/feed-uuid-abc",
                json={"preferred_property_type": "multi_family", "investment_budget_band": "150k_500k"},
            )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 200
        assert resp.json() == {"ok": True}
        assert subscriber.preferred_property_type == "multi_family"
        assert subscriber.investment_budget_band == "150k_500k"
        assert subscriber.onboarding_completed is True
        mock_db.flush.assert_called_once()

    def test_patch_stamps_onboarding_completed_activation_event(self, mock_db):
        """Section 4.10: the onboarding submit must stamp activation_events
        so the funnel can tell 'never onboarded' apart from 'onboarded, never
        saw a lead'."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            with patch(
                "src.services.activation_tracking.stamp_onboarding_completed"
            ) as mock_stamp:
                resp = client.patch(
                    "/api/subscriber/onboarding/feed-uuid-abc",
                    json={"preferred_property_type": "multi_family", "investment_budget_band": "150k_500k"},
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 200
        mock_stamp.assert_called_once_with(7, mock_db)

    def test_patch_creates_referral_prospect_when_given(self, mock_db):
        """Section 7.3: submitting name + county writes a ReferralProspect
        attached to the referring subscriber, once the county passes the
        eligibility checks (not launched, is a known upcoming candidate)."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)
        mock_db.execute.side_effect = _eligible_county_db_responses()

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            with patch("src.services.activation_tracking.stamp_onboarding_completed"):
                resp = client.patch(
                    "/api/subscriber/onboarding/feed-uuid-abc",
                    json={
                        "preferred_property_type": "multi_family",
                        "investment_budget_band": "150k_500k",
                        "referral_prospect_name": "Dana Roofing",
                        "referral_prospect_company": "Dana Roofing LLC",
                        "referral_target_county_id": "polk",
                    },
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 200
        assert mock_db.add.call_count == 1
        added = mock_db.add.call_args.args[0]
        assert added.referring_subscriber_id == 7
        assert added.prospect_name == "Dana Roofing"
        assert added.prospect_company == "Dana Roofing LLC"
        assert added.target_county_id == "polk"

    def test_patch_referral_updates_existing_row_not_duplicate(self, mock_db):
        """PR #178 review fix: a retried/resubmitted PATCH for the same
        (subscriber, county) must update the existing row, not insert a
        second one."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)
        existing_row = MagicMock()
        mock_db.execute.side_effect = _eligible_county_db_responses(existing_prospect=existing_row)

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            with patch("src.services.activation_tracking.stamp_onboarding_completed"):
                resp = client.patch(
                    "/api/subscriber/onboarding/feed-uuid-abc",
                    json={
                        "preferred_property_type": "multi_family",
                        "investment_budget_band": "150k_500k",
                        "referral_prospect_name": "Dana Roofing Updated",
                        "referral_target_county_id": "polk",
                    },
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 200
        mock_db.add.assert_not_called()
        assert existing_row.prospect_name == "Dana Roofing Updated"

    def test_patch_referral_rejects_already_launched_county(self, mock_db):
        """PR #178 review fix: a county that already has an active County row
        must be rejected — the ask is for counties not yet opened."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)
        mock_db.execute.side_effect = [MagicMock(**{"scalar_one_or_none.return_value": 1})]

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            with patch("src.services.activation_tracking.stamp_onboarding_completed"):
                resp = client.patch(
                "/api/subscriber/onboarding/feed-uuid-abc",
                json={
                    "preferred_property_type": "multi_family",
                    "investment_budget_band": "150k_500k",
                    "referral_prospect_name": "Dana Roofing",
                    "referral_target_county_id": "hillsborough",
                },
            )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "county_already_launched"
        mock_db.add.assert_not_called()

    def test_patch_referral_rejects_unknown_county(self, mock_db):
        """PR #178 review fix: a county with no expansion_candidates row at
        all (typo, made-up name) must be rejected, not silently stored."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)
        mock_db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": None}),   # not launched
            MagicMock(**{"scalar_one_or_none.return_value": None}),   # no expansion_candidates row
        ]

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            with patch("src.services.activation_tracking.stamp_onboarding_completed"):
                resp = client.patch(
                "/api/subscriber/onboarding/feed-uuid-abc",
                json={
                    "preferred_property_type": "multi_family",
                    "investment_budget_band": "150k_500k",
                    "referral_prospect_name": "Dana Roofing",
                    "referral_target_county_id": "not_a_real_county",
                },
            )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "unknown_county"
        mock_db.add.assert_not_called()

    def test_patch_referral_rejects_already_launched_status_county(self, mock_db):
        """An expansion_candidates row stuck at status='launched' must also
        be rejected, not just a missing row."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)
        mock_db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": None}),          # not in counties table
            MagicMock(**{"scalar_one_or_none.return_value": "launched"}),    # candidate already launched
        ]

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            with patch("src.services.activation_tracking.stamp_onboarding_completed"):
                resp = client.patch(
                "/api/subscriber/onboarding/feed-uuid-abc",
                json={
                    "preferred_property_type": "multi_family",
                    "investment_budget_band": "150k_500k",
                    "referral_prospect_name": "Dana Roofing",
                    "referral_target_county_id": "polk",
                },
            )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "unknown_county"
        mock_db.add.assert_not_called()

    def test_patch_no_referral_prospect_when_omitted(self, mock_db):
        """No referral fields submitted → no ReferralProspect row created."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            with patch("src.services.activation_tracking.stamp_onboarding_completed"):
                resp = client.patch(
                    "/api/subscriber/onboarding/feed-uuid-abc",
                    json={"preferred_property_type": "multi_family", "investment_budget_band": "150k_500k"},
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 200
        mock_db.add.assert_not_called()

    def test_patch_referral_company_optional(self, mock_db):
        """Company is the one truly optional piece within the referral itself."""
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)
        mock_db.execute.side_effect = _eligible_county_db_responses()

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            with patch("src.services.activation_tracking.stamp_onboarding_completed"):
                resp = client.patch(
                    "/api/subscriber/onboarding/feed-uuid-abc",
                    json={
                        "preferred_property_type": "multi_family",
                        "investment_budget_band": "150k_500k",
                        "referral_prospect_name": "Dana Roofing",
                        "referral_target_county_id": "polk",
                    },
                )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 200
        added = mock_db.add.call_args.args[0]
        assert added.prospect_company is None

    def test_patch_referral_name_without_county_422(self, mock_db):
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            resp = client.patch(
                "/api/subscriber/onboarding/feed-uuid-abc",
                json={
                    "preferred_property_type": "multi_family",
                    "investment_budget_band": "150k_500k",
                    "referral_prospect_name": "Dana Roofing",
                },
            )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 422

    def test_patch_rejects_invalid_choice_422(self, mock_db):
        from fastapi.testclient import TestClient
        from src.api.main import app, get_db
        from src.services.subscriber_auth import get_current_subscriber

        subscriber = MagicMock(id=7, onboarding_completed=False)

        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_subscriber] = lambda: subscriber
        try:
            client = TestClient(app)
            resp = client.patch(
                "/api/subscriber/onboarding/feed-uuid-abc",
                json={"preferred_property_type": "castle", "investment_budget_band": "150k_500k"},
            )
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_current_subscriber, None)

        assert resp.status_code == 422
