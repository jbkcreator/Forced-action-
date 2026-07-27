"""Unit tests for the onboarding preference step (PATCH /api/subscriber/onboarding/{feed_uuid})."""
from unittest.mock import MagicMock, patch

import pytest


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
        attached to the referring subscriber."""
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
