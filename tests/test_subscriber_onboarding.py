"""Unit tests for the onboarding preference step (PATCH /api/subscriber/onboarding/{feed_uuid})."""
from unittest.mock import MagicMock

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
