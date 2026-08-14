"""
Tests for ICP-scoped pricing (item 1) and kill-switch lifecycle (item 2).
No DB required — pure config + mocked DB.
"""
from __future__ import annotations
from unittest.mock import MagicMock, patch
import pytest


# ── Item 1: ICP-scoped pricing ────────────────────────────────────────────────

class TestIcpPricingConfig:
    def test_all_six_expansion_icps_have_prices(self):
        from config.icp_channels import ICP_CHANNELS, get_icp_price_cents
        expansion = [k for k, v in ICP_CHANNELS.items() if not v.get("is_default")]
        for key in expansion:
            assert get_icp_price_cents(key) is not None, f"{key} has no price"

    def test_contractor_has_no_icp_price(self):
        from config.icp_channels import get_icp_price_cents
        assert get_icp_price_cents("contractor") is None

    def test_expected_prices(self):
        from config.icp_channels import get_icp_price_cents
        assert get_icp_price_cents("rei_investor")        == 19700
        assert get_icp_price_cents("insurance_adjuster")  == 9700
        assert get_icp_price_cents("hard_money_lender")   == 39700
        assert get_icp_price_cents("property_manager")    == 19700
        assert get_icp_price_cents("bankruptcy_attorney") == 19700
        assert get_icp_price_cents("title_company")       == 9700

    def test_stripe_key_to_icp_map_correct(self):
        from config.icp_channels import STRIPE_KEY_TO_ICP
        assert STRIPE_KEY_TO_ICP["icp_rei_investor"] == "rei_investor"
        assert STRIPE_KEY_TO_ICP["icp_hard_money_lender"] == "hard_money_lender"
        assert "contractor" not in STRIPE_KEY_TO_ICP.values() or all(
            v != "contractor" for v in STRIPE_KEY_TO_ICP.values()
        )


class TestValidateIcpCheckout:
    def _make_db(self):
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        return db

    def _call(self, subscriber_icp, stripe_price_key):
        from fastapi.testclient import TestClient
        from src.api.main import app
        from src.api.admin_router import create_access_token
        from src.core.database import get_db_context
        from contextlib import contextmanager

        token = create_access_token({"sub": "test_admin", "scope": "admin"})

        @contextmanager
        def fake_db():
            yield self._make_db()

        app.dependency_overrides[get_db_context] = lambda: iter([])
        # Use TestClient without DB dependency for this pure-config test
        client = TestClient(app)
        r = client.post(
            "/api/admin/icp-channels/validate-checkout",
            json={"subscriber_icp_channel_key": subscriber_icp, "stripe_price_key": stripe_price_key},
            headers={"Authorization": f"Bearer {token}"},
        )
        app.dependency_overrides.clear()
        return r

    def test_valid_icp_price_returns_200(self):
        r = self._call("rei_investor", "icp_rei_investor")
        assert r.status_code == 200
        assert r.json()["valid"] is True

    def test_cross_icp_purchase_returns_409(self):
        r = self._call("insurance_adjuster", "icp_rei_investor")
        assert r.status_code == 409
        body = r.json()
        assert "cross_icp_purchase" in body.get("detail", {}).get("error", "")

    def test_contractor_with_any_price_is_valid(self):
        r = self._call("contractor", "icp_rei_investor")
        assert r.status_code == 200

    def test_non_icp_price_always_valid(self):
        # Standard tier price not in STRIPE_KEY_TO_ICP
        r = self._call("rei_investor", "starter_founding")
        assert r.status_code == 200


# ── Item 2: Kill-switch lifecycle ─────────────────────────────────────────────

class TestKillswitchStatus:
    def _row(self, **kw):
        from datetime import datetime, timezone, timedelta
        r = MagicMock()
        r.launch_started_at = kw.get("launch_started_at",
            datetime.now(timezone.utc) - timedelta(days=10))
        r.launch_ends_at = kw.get("launch_ends_at",
            datetime.now(timezone.utc) + timedelta(days=18))
        r.killswitch_decision = kw.get("killswitch_decision", None)
        r.killswitch_reason = kw.get("killswitch_reason", None)
        r.killswitch_decided_at = kw.get("killswitch_decided_at", None)
        r.killswitch_decided_by = kw.get("killswitch_decided_by", None)
        return r

    def test_all_green_returns_green(self):
        from src.api.icp_channel_router import _killswitch_status
        snapshot = {"a": {"color": "green"}, "b": {"color": "green"}}
        s = _killswitch_status(self._row(), snapshot)
        assert s["status"] == "green"

    def test_any_red_returns_red(self):
        from src.api.icp_channel_router import _killswitch_status
        snapshot = {"a": {"color": "green"}, "b": {"color": "red"}}
        s = _killswitch_status(self._row(), snapshot)
        assert s["status"] == "red"

    def test_any_yellow_returns_yellow(self):
        from src.api.icp_channel_router import _killswitch_status
        snapshot = {"a": {"color": "green"}, "b": {"color": "yellow"}}
        s = _killswitch_status(self._row(), snapshot)
        assert s["status"] == "yellow"

    def test_unknown_color_returns_yellow(self):
        from src.api.icp_channel_router import _killswitch_status
        snapshot = {"a": {"color": "unknown"}}
        s = _killswitch_status(self._row(), snapshot)
        assert s["status"] == "yellow"

    def test_not_started_when_no_launch_date(self):
        from src.api.icp_channel_router import _killswitch_status
        r = self._row(launch_started_at=None)
        s = _killswitch_status(r, {})
        assert s["status"] == "not_started"

    def test_days_remaining_calculated(self):
        from datetime import datetime, timezone, timedelta
        from src.api.icp_channel_router import _killswitch_status
        r = self._row(
            launch_started_at=datetime.now(timezone.utc) - timedelta(days=10),
            launch_ends_at=datetime.now(timezone.utc) + timedelta(days=18),
        )
        s = _killswitch_status(r, {"a": {"color": "green"}})
        assert s["days_elapsed"] == 10
        assert s["days_remaining"] in (17, 18)  # rounding tolerance


class TestKillswitchDecisionRequest:
    def test_valid_decisions_accepted(self):
        from src.api.icp_channel_router import KillswitchDecisionRequest
        for decision in ("keep", "adjust", "kill"):
            r = KillswitchDecisionRequest(decision=decision, reason="A" * 10)
            assert r.decision == decision

    def test_invalid_decision_rejected(self):
        from src.api.icp_channel_router import KillswitchDecisionRequest
        with pytest.raises(Exception):
            KillswitchDecisionRequest(decision="ignore", reason="A" * 10)

    def test_short_reason_rejected(self):
        from src.api.icp_channel_router import KillswitchDecisionRequest
        with pytest.raises(Exception):
            KillswitchDecisionRequest(decision="keep", reason="too short")
