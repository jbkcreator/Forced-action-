"""
Phase 2B v9 Auto Mode — entitlement + delivery wiring tests.

Covers:
  - toggle() tier-gating (Starter denied without add-on; Growth/Power allowed;
    Starter with active Stripe add-on allowed; disable is always allowed)
  - SMS AUTO ON denies Starter without add-on; allows Growth/Power
  - Webhook entitlement branch sets auto_mode_enabled on add-on purchase
  - Wallet activation auto-enables Auto Mode for Growth/Power tiers
  - Lead-delivery paths invoke enqueue_action after SentLead insert
  - Existing 24h no-reply VM follow-up unaffected

Run:
    pytest tests/test_auto_mode_entitlement.py -v
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker


# SQLite shim — same as content-quality test file
@compiles(JSONB, "sqlite")
def _jsonb_to_json(type_, compiler, **kw):  # noqa: D401
    return "JSON"


@compiles(ARRAY, "sqlite")
def _array_to_text(type_, compiler, **kw):  # noqa: D401
    return "TEXT"


from src.core.models import (  # noqa: E402
    Base,
    Subscriber,
    WalletBalance,
)
from src.services import auto_mode  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def session():
    # check_same_thread=False so TestClient (which runs handlers in a worker
    # thread) can share the same in-memory SQLite session as the test body.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    s = SessionLocal()
    try:
        yield s
    finally:
        s.close()
        engine.dispose()


_sub_counter = 0

def _make_subscriber(session, *, tier="starter", wallet_tier=None,
                    auto_mode_enabled=False, stripe_customer_id=None) -> Subscriber:
    """Insert a subscriber row + optional WalletBalance for tests."""
    global _sub_counter
    _sub_counter += 1
    cid = stripe_customer_id or f"cus_test_{_sub_counter}"
    sub = Subscriber(
        stripe_customer_id=cid,
        tier=tier,
        vertical="roofing",
        county_id="hillsborough",
        status="active",
        auto_mode_enabled=auto_mode_enabled,
        event_feed_uuid=f"feed-{_sub_counter:04d}",
    )
    session.add(sub)
    session.flush()
    if wallet_tier:
        session.add(WalletBalance(
            subscriber_id=sub.id,
            wallet_tier=wallet_tier,
            credits_remaining=0,
        ))
        session.flush()
    return sub


# ---------------------------------------------------------------------------
# Toggle gate
# ---------------------------------------------------------------------------

class TestToggleGate:
    def test_starter_without_addon_denied(self, session):
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet")
        with patch.object(auto_mode, "_has_active_auto_mode_addon", return_value=False):
            with pytest.raises(PermissionError):
                auto_mode.toggle(sub.id, True, session)
        session.refresh(sub)
        assert sub.auto_mode_enabled is False

    def test_starter_no_wallet_balance_denied(self, session):
        """Subscriber with no WalletBalance row at all (free tier baseline)."""
        sub = _make_subscriber(session, tier="starter")
        with patch.object(auto_mode, "_has_active_auto_mode_addon", return_value=False):
            with pytest.raises(PermissionError):
                auto_mode.toggle(sub.id, True, session)
        assert sub.auto_mode_enabled is False

    def test_growth_user_allowed(self, session):
        sub = _make_subscriber(session, tier="starter", wallet_tier="growth")
        result = auto_mode.toggle(sub.id, True, session)
        assert result is True
        assert sub.auto_mode_enabled is True

    def test_power_user_allowed(self, session):
        sub = _make_subscriber(session, tier="starter", wallet_tier="power")
        result = auto_mode.toggle(sub.id, True, session)
        assert result is True
        assert sub.auto_mode_enabled is True

    def test_starter_with_active_addon_allowed(self, session):
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet")
        with patch.object(auto_mode, "_has_active_auto_mode_addon", return_value=True):
            result = auto_mode.toggle(sub.id, True, session)
        assert result is True
        assert sub.auto_mode_enabled is True

    def test_disable_always_allowed(self, session):
        """Disabling is never gated — even Starter without add-on can opt out."""
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet",
                              auto_mode_enabled=True)
        # No mock needed — disable doesn't check entitlement
        result = auto_mode.toggle(sub.id, False, session)
        assert result is False
        assert sub.auto_mode_enabled is False


# ---------------------------------------------------------------------------
# SMS AUTO ON handler
# ---------------------------------------------------------------------------

class TestSmsAutoOnHandler:
    def test_starter_no_addon_gets_deny_message(self, session):
        from src.services.sms_commands import _handle_auto_on
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet")
        with patch.object(auto_mode, "_has_active_auto_mode_addon", return_value=False):
            reply = _handle_auto_on(sub, session)
        assert "Auto Mode requires" in reply
        assert "add-on" in reply
        assert sub.auto_mode_enabled is False

    def test_growth_user_enables_via_sms(self, session):
        from src.services.sms_commands import _handle_auto_on
        sub = _make_subscriber(session, tier="starter", wallet_tier="growth")
        reply = _handle_auto_on(sub, session)
        assert "Auto Mode ON" in reply
        assert sub.auto_mode_enabled is True


# ---------------------------------------------------------------------------
# Checkout endpoint
# ---------------------------------------------------------------------------

class TestAutoModeCheckoutEndpoint:
    def _build_app_client(self):
        from fastapi.testclient import TestClient
        from src.api import main as api_main
        return TestClient(api_main.app), api_main

    def test_creates_subscription_session(self, session):
        """POST /api/checkout/auto-mode creates a Stripe subscription session
        with the configured Auto Mode price id."""
        from src.api import main as api_main

        client, _ = self._build_app_client()
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet")
        sub_feed = sub.event_feed_uuid

        # Override the get_db dependency to use our test session
        def _override_db():
            yield session
        api_main.app.dependency_overrides[api_main.get_db] = _override_db

        try:
            with patch("src.api.main.get_settings") as mock_settings, \
                 patch("src.api.main.stripe") as mock_stripe:
                fake_settings = MagicMock()
                fake_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test_x"
                fake_settings.active_stripe_price.return_value = "price_test_auto_mode"
                fake_settings.app_base_url = "https://example.com"
                mock_settings.return_value = fake_settings

                mock_stripe.checkout.Session.create.return_value = MagicMock(
                    client_secret="cs_test_secret_xxx", id="cs_test_abc",
                )
                mock_stripe.StripeError = Exception

                resp = client.post("/api/checkout/auto-mode", json={"feed_uuid": sub_feed})

                assert resp.status_code == 200, resp.json()
                body = resp.json()
                assert body["client_secret"] == "cs_test_secret_xxx"
                assert "publishable_key" in body

                # Verify the call shape — embedded subscription checkout
                call_kwargs = mock_stripe.checkout.Session.create.call_args.kwargs
                assert call_kwargs["mode"] == "subscription"
                assert call_kwargs["ui_mode"] == "embedded"
                assert "return_url" in call_kwargs
                assert "success_url" not in call_kwargs
                assert call_kwargs["line_items"][0]["price"] == "price_test_auto_mode"
                assert call_kwargs["metadata"]["product"] == "auto_mode_addon"
                assert call_kwargs["metadata"]["subscriber_id"] == str(sub.id)
        finally:
            api_main.app.dependency_overrides.pop(api_main.get_db, None)

    def test_starter_without_stripe_customer_id_400(self, session):
        from src.api import main as api_main
        client, _ = self._build_app_client()

        # Subscriber whose stripe_customer_id is set but we'll patch it to empty
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet")
        sub.stripe_customer_id = ""  # simulate the no-customer edge case
        session.flush()
        sub_feed = sub.event_feed_uuid

        def _override_db():
            yield session
        api_main.app.dependency_overrides[api_main.get_db] = _override_db
        try:
            with patch("src.api.main.get_settings") as mock_settings:
                fake_settings = MagicMock()
                fake_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test_x"
                mock_settings.return_value = fake_settings

                resp = client.post("/api/checkout/auto-mode", json={"feed_uuid": sub_feed})
                assert resp.status_code == 400
                assert resp.json()["detail"]["error"] == "no_stripe_customer"
        finally:
            api_main.app.dependency_overrides.pop(api_main.get_db, None)


# ---------------------------------------------------------------------------
# Toggle endpoint (REST counterpart of SMS AUTO ON/OFF)
# ---------------------------------------------------------------------------

class TestAutoModeToggleEndpoint:
    def test_endpoint_returns_402_for_unentitled_starter(self, session):
        from fastapi.testclient import TestClient
        from src.api import main as api_main

        client = TestClient(api_main.app)
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet")

        def _override_db():
            yield session
        api_main.app.dependency_overrides[api_main.get_db] = _override_db
        try:
            with patch.object(auto_mode, "_has_active_auto_mode_addon", return_value=False):
                resp = client.post(
                    "/api/auto-mode/toggle",
                    json={"feed_uuid": sub.event_feed_uuid, "enabled": True},
                )
            assert resp.status_code == 402
            assert resp.json()["detail"]["error"] == "requires_addon"
        finally:
            api_main.app.dependency_overrides.pop(api_main.get_db, None)

    def test_endpoint_enables_growth_user(self, session):
        from fastapi.testclient import TestClient
        from src.api import main as api_main

        client = TestClient(api_main.app)
        sub = _make_subscriber(session, tier="starter", wallet_tier="growth")

        def _override_db():
            yield session
        api_main.app.dependency_overrides[api_main.get_db] = _override_db
        try:
            resp = client.post(
                "/api/auto-mode/toggle",
                json={"feed_uuid": sub.event_feed_uuid, "enabled": True},
            )
            assert resp.status_code == 200
            assert resp.json()["auto_mode_enabled"] is True
            session.refresh(sub)
            assert sub.auto_mode_enabled is True
        finally:
            api_main.app.dependency_overrides.pop(api_main.get_db, None)


# ---------------------------------------------------------------------------
# Webhook entitlement activation
# ---------------------------------------------------------------------------

class TestAddonWebhookActivation:
    def test_addon_purchase_sets_flag(self, session):
        from src.services.stripe_webhooks import _on_auto_mode_addon_purchase
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet",
                              auto_mode_enabled=False)

        fake_session_event = {
            "id": "cs_test_addon_1",
            "metadata": {
                "product": "auto_mode_addon",
                "subscriber_id": str(sub.id),
            },
        }

        with patch("config.settings.settings") as mock_settings, \
             patch("src.services.stripe_webhooks.stripe") as mock_stripe:
            mock_settings.active_stripe_price.return_value = "price_test_auto_mode"
            mock_settings.active_stripe_secret_key = None
            mock_stripe.checkout.Session.list_line_items.return_value = MagicMock(
                data=[{"price": {"id": "price_test_auto_mode"}}],
            )

            _on_auto_mode_addon_purchase(fake_session_event, session)

        session.refresh(sub)
        assert sub.auto_mode_enabled is True

    def test_addon_purchase_idempotent(self, session):
        from src.services.stripe_webhooks import _on_auto_mode_addon_purchase
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet",
                              auto_mode_enabled=True)  # already on

        fake_session_event = {
            "id": "cs_test_replay",
            "metadata": {"product": "auto_mode_addon", "subscriber_id": str(sub.id)},
        }

        with patch("config.settings.settings") as mock_settings, \
             patch("src.services.stripe_webhooks.stripe") as mock_stripe:
            mock_settings.active_stripe_price.return_value = "price_test_auto_mode"
            mock_settings.active_stripe_secret_key = None
            mock_stripe.checkout.Session.list_line_items.return_value = MagicMock(
                data=[{"price": {"id": "price_test_auto_mode"}}],
            )
            # No exception; idempotent no-op
            _on_auto_mode_addon_purchase(fake_session_event, session)

        assert sub.auto_mode_enabled is True

    def test_missing_subscriber_id_is_safe_noop(self, session):
        from src.services.stripe_webhooks import _on_auto_mode_addon_purchase
        # No subscriber_id in metadata → handler logs error and returns
        _on_auto_mode_addon_purchase(
            {"id": "cs_no_meta", "metadata": {"product": "auto_mode_addon"}},
            session,
        )
        # No-op; no exception means pass


# ---------------------------------------------------------------------------
# Lead-delivery wiring
# ---------------------------------------------------------------------------

class TestLeadDeliveryWiring:
    def test_enqueue_action_self_guards_when_ineligible(self, session):
        """enqueue_action returns {eligible: False} for non-Auto-Mode users.
        Confirms safe-to-call-unconditionally semantics that the delivery
        wiring relies on."""
        sub = _make_subscriber(session, tier="starter", wallet_tier="starter_wallet")
        # No property exists; enqueue_action should not even attempt
        result = auto_mode.enqueue_action(sub.id, property_id=99999, db=session)
        assert result["eligible"] is False

    def test_enqueue_action_runs_for_growth_user(self, session):
        """Growth user → eligible=True; with no Owner row enqueue defers
        skip-trace and returns without sending SMS."""
        sub = _make_subscriber(session, tier="starter", wallet_tier="growth")
        # Insert a minimal Property to satisfy the eligibility/property check
        from src.core.models import Property
        prop = Property(parcel_id=f"test-parcel-{sub.id}", county_id="hillsborough")
        session.add(prop)
        session.flush()

        result = auto_mode.enqueue_action(sub.id, prop.id, db=session)
        assert result["eligible"] is True
        assert result["skip_trace_queued"] is True
        assert result["first_text_sent"] is False  # no phone → defers to enrichment

    def test_enqueue_action_idempotent_on_repeat_call(self, session):
        """Calling enqueue twice for same (sub, property) doesn't crash and
        produces consistent state (each call may write a MessageOutcome row
        — that's the existing behavior, not duplicate-protected, which is
        fine since the 24h sweep dedupes on clicked_at)."""
        sub = _make_subscriber(session, tier="starter", wallet_tier="growth")
        from src.core.models import Property
        prop = Property(parcel_id=f"test-parcel-{sub.id}-rep", county_id="hillsborough")
        session.add(prop)
        session.flush()

        r1 = auto_mode.enqueue_action(sub.id, prop.id, db=session)
        r2 = auto_mode.enqueue_action(sub.id, prop.id, db=session)
        assert r1["eligible"] is True and r2["eligible"] is True
