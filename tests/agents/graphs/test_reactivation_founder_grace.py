"""
Founder-tier (tier == 'founder') zip_held win-back copy/mechanic —
wayfinder map notion-pending-tasks, tickets F1/F2/F3: founders get a
one-time +14-day territory grace extension instead of the standard
50%-off coupon.
"""
from unittest.mock import MagicMock

import pytest

from src.agents.graphs.reactivation import _node_build_compose_context


def _base_state(tier: str, winback_branch: str = "zip_held") -> dict:
    return {
        "subscriber_id": 42,
        "subscriber_profile": {
            "tier": tier,
            "name": "Jane Roofer",
            "county_id": "hillsborough",
            "vertical": "roofing",
            "email": "jane@example.com",
        },
        "event_payload": {
            "cohort": "tier3_winback",
            "winback_branch": winback_branch,
        },
        "_channel": "sms",
    }


@pytest.fixture(autouse=True)
def _patch_db_session_scope(monkeypatch):
    """The node opens `db.session_scope()` for the grant/token calls — stub
    it to a no-op context manager so this stays a pure unit test."""
    import src.agents.graphs.reactivation as mod

    fake_session = MagicMock()
    fake_db = MagicMock()
    fake_db.session_scope.return_value.__enter__.return_value = fake_session
    fake_db.session_scope.return_value.__exit__.return_value = False
    monkeypatch.setattr(mod, "db", fake_db)
    return fake_session


class TestFounderZipHeldGetsGraceExtension:
    def test_founder_tier_zip_held_calls_grant_not_create_offer(self, monkeypatch):
        import src.services.winback_offers as winback_offers

        grant_mock = MagicMock(return_value=True)
        create_mock = MagicMock(return_value="should-not-be-called")
        monkeypatch.setattr(winback_offers, "grant_founder_grace_extension", grant_mock)
        monkeypatch.setattr(winback_offers, "create_or_reuse_offer", create_mock)

        _node_build_compose_context(_base_state("founder", "zip_held"))

        grant_mock.assert_called_once()
        assert grant_mock.call_args[0][0] == 42
        create_mock.assert_not_called()

    def test_founder_tier_zip_held_uses_grace_copy_not_discount_copy(self, monkeypatch):
        import src.services.winback_offers as winback_offers
        monkeypatch.setattr(winback_offers, "grant_founder_grace_extension", MagicMock(return_value=True))

        result = _node_build_compose_context(_base_state("founder", "zip_held"))

        assert "50% off" not in result["_fallback_body"]
        assert "extended your grace period by 14 days" in result["_fallback_body"]

    def test_non_founder_zip_held_still_gets_discount_copy_and_token(self, monkeypatch):
        import src.services.winback_offers as winback_offers

        grant_mock = MagicMock(return_value=True)
        create_mock = MagicMock(return_value="tok-abc123")
        monkeypatch.setattr(winback_offers, "grant_founder_grace_extension", grant_mock)
        monkeypatch.setattr(winback_offers, "create_or_reuse_offer", create_mock)

        result = _node_build_compose_context(_base_state("starter", "zip_held"))

        create_mock.assert_called_once()
        grant_mock.assert_not_called()
        assert "50% off" in result["_fallback_body"]
        assert "tok-abc123" in result["_fallback_body"]

    def test_founder_tier_zip_released_is_unaffected(self, monkeypatch):
        """The founder exception is zip_held-only — zip_released keeps the
        standard 5-credit token flow regardless of tier."""
        import src.services.winback_offers as winback_offers

        grant_mock = MagicMock(return_value=True)
        create_mock = MagicMock(return_value="tok-released-1")
        monkeypatch.setattr(winback_offers, "grant_founder_grace_extension", grant_mock)
        monkeypatch.setattr(winback_offers, "create_or_reuse_offer", create_mock)

        result = _node_build_compose_context(_base_state("founder", "zip_released"))

        grant_mock.assert_not_called()
        create_mock.assert_called_once()
        assert "5 free credits" in result["_fallback_body"]

    def test_grant_failure_does_not_raise(self, monkeypatch):
        import src.services.winback_offers as winback_offers
        monkeypatch.setattr(
            winback_offers, "grant_founder_grace_extension",
            MagicMock(side_effect=RuntimeError("db boom")),
        )
        result = _node_build_compose_context(_base_state("founder", "zip_held"))
        assert "extended your grace period by 14 days" in result["_fallback_body"]
