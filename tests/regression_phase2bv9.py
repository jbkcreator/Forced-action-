"""
Phase 2B v9 regression smoke tests.

Verifies the 7 implemented flows remain wired end-to-end. Each test is a
lightweight smoke check: correct routing, key state outputs, no crashes.
Not a full integration test — external services are mocked.

Run:
    pytest tests/regression_phase2bv9.py -v
"""
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest


# ─── helpers ─────────────────────────────────────────────────────────────────

def _stripe_mock():
    m = MagicMock()
    m.Subscription = MagicMock()
    return m


def _make_sub(status="active", sub_id=1, score=80, phone="+13135550101"):
    s = MagicMock()
    s.id = sub_id
    s.status = status
    s.stripe_subscription_id = "sub_abc"
    s.paused_at = None
    s.pause_resume_at = None
    s.phone_1 = phone
    s.vertical = "roofing"
    s.sms_opt_in = True
    return s


def _settings_mock():
    s = MagicMock()
    s.synthflow_api_key = None
    s.synthflow_outbound_agent_roofing = None
    s.active_stripe_secret_key = MagicMock()
    s.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
    return s


# ─── Flow 1: Territory Lock (wallet_to_lock) ─────────────────────────────────

class TestTerritoryLockFlow:
    def test_router_wired(self):
        from src.agents.router import EVENT_TO_GRAPH
        assert "subscriber_crossed_lock_threshold" in EVENT_TO_GRAPH
        spec = EVENT_TO_GRAPH["subscriber_crossed_lock_threshold"]
        assert spec.graph_name == "wallet_to_lock_close"

    def test_event_dispatch_reaches_graph(self):
        """dispatch_event resolves the correct graph runner."""
        from src.agents.router import get_graph_spec
        spec = get_graph_spec("subscriber_crossed_lock_threshold")
        assert spec is not None
        assert callable(spec.runner)


# ─── Flow 2: Charter Annual Push ─────────────────────────────────────────────

class TestCharterAnnualPushFlow:
    def test_annual_push_module_importable(self):
        from src.tasks import annual_push  # noqa: F401

    def test_annual_push_has_run(self):
        import importlib
        mod = importlib.import_module("src.tasks.annual_push")
        assert hasattr(mod, "run") or hasattr(mod, "annual_push_task") or hasattr(mod, "run_annual_push")


# ─── Flow 3: Data-Only Save (pause/resume) ───────────────────────────────────

class TestDataOnlySaveFlow:
    def test_pause_active_sub(self):
        from src.services.pause_subscription import pause_subscriber
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()

        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            result = pause_subscriber(db, 1, days=60)

        assert result is True
        assert sub.status == "paused"

    def test_resume_paused_sub(self):
        from src.services.pause_subscription import resume_subscriber
        sub = _make_sub(status="paused")
        sub.paused_at = datetime.now(timezone.utc)
        sub.pause_resume_at = datetime.now(timezone.utc) + timedelta(days=30)
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()

        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            result = resume_subscriber(db, 1)

        assert result is True
        assert sub.status == "active"

    def test_sms_pause_command_registered(self):
        from src.services.sms_commands import COMMANDS
        assert "PAUSE" in COMMANDS

    def test_sms_resume_command_registered(self):
        from src.services.sms_commands import COMMANDS
        assert "RESUME" in COMMANDS


# ─── Flow 4: Cora Lock Close (ap_lite) ───────────────────────────────────────

class TestCoraLockCloseFlow:
    def test_router_wired(self):
        from src.agents.router import EVENT_TO_GRAPH
        assert "subscriber_crossed_ap_lite_threshold" in EVENT_TO_GRAPH

    def test_ap_lite_graph_buildable(self):
        from src.agents.graphs.ap_lite_close import build_ap_lite_graph
        graph = build_ap_lite_graph()
        assert graph is not None


# ─── Flow 5: Stripe Recovery (stripe webhooks) ───────────────────────────────

class TestStripeRecoveryFlow:
    def test_webhook_handler_importable(self):
        import sys
        stripe_m = MagicMock()
        with patch.dict(sys.modules, {"stripe": stripe_m}):
            from src.services.stripe_webhooks import handle_webhook  # noqa: F401

    def test_partner_provision_branch_exists(self):
        import sys, inspect
        stripe_m = MagicMock()
        with patch.dict(sys.modules, {"stripe": stripe_m}):
            from src.services import stripe_webhooks
            src_code = inspect.getsource(stripe_webhooks)
        assert "provision_partner_access" in src_code


# ─── Flow 6: What You Missed (retention / FOMO) ──────────────────────────────

class TestWhatYouMissedFlow:
    def test_fomo_router_wired(self):
        from src.agents.router import EVENT_TO_GRAPH
        assert "competitor_acted_on_lead" in EVENT_TO_GRAPH

    def test_retention_router_wired(self):
        from src.agents.router import EVENT_TO_GRAPH
        assert "retention_summary_due" in EVENT_TO_GRAPH

    def test_retention_producer_importable(self):
        from src.tasks import retention_event_producer  # noqa: F401


# ─── Flow 7: Synthflow Voice Drop (Phase C) ──────────────────────────────────

class TestSynthflowVoiceDropFlow:
    def test_router_wired(self):
        from src.agents.router import EVENT_TO_GRAPH
        assert "high_intent_no_convert" in EVENT_TO_GRAPH
        assert EVENT_TO_GRAPH["high_intent_no_convert"].graph_name == "synthflow_voice_drop"

    def test_graph_buildable(self):
        from src.agents.graphs.synthflow_voice_drop import build_synthflow_voice_drop_graph
        g = build_synthflow_voice_drop_graph()
        assert g is not None

    def test_client_module_importable(self):
        from src.services import synthflow_client  # noqa: F401

    def test_sweep_task_importable(self):
        from src.tasks import synthflow_voice_drop_sweep  # noqa: F401

    def test_settings_fields_present(self):
        from config.settings import AppSettings
        fields = AppSettings.model_fields
        assert "synthflow_api_key" in fields
        assert "synthflow_api_base" in fields
        assert "synthflow_outbound_agent_roofing" in fields


# ─── Schema smoke (partner_subscriptions table model) ────────────────────────

class TestSchemaSmoke:
    def test_partner_subscription_model_importable(self):
        from src.core.models import PartnerSubscription  # noqa: F401

    def test_manual_action_log_importable(self):
        from src.core.models import ManualActionLog  # noqa: F401

    def test_human_close_escalations_importable(self):
        from src.core.models import HumanCloseEscalation  # noqa: F401

    def test_subscriber_has_paused_at(self):
        from src.core.models import Subscriber
        assert hasattr(Subscriber, "paused_at")

    def test_subscriber_has_pause_resume_at(self):
        from src.core.models import Subscriber
        assert hasattr(Subscriber, "pause_resume_at")
