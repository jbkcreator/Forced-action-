"""
Unit tests for the synthflow voice-drop cold-dial compliance block.
"""
import sys
from unittest.mock import MagicMock, patch

import pytest

_agents_stub = MagicMock()
_agents_stub.get_agents_settings = MagicMock(return_value=MagicMock())
if "config.agents" not in sys.modules:
    sys.modules["config.agents"] = _agents_stub


def _make_sub_profile(sub_id=1, phone="+13135550101", vertical="roofing", ghl_contact_id="ghl_123"):
    return {
        "id": sub_id,
        "name": "Test User",
        "phone": phone,
        "vertical": vertical,
        "ghl_contact_id": ghl_contact_id,
    }


def _settings_mock(agent_id="agent_123", api_key="sf_key"):
    s = MagicMock()
    s.synthflow_api_key = MagicMock()
    s.synthflow_api_key.get_secret_value.return_value = api_key
    s.synthflow_api_base = "https://api.synthflow.ai/v2"
    s.synthflow_outbound_agent_roofing = agent_id
    s.app_base_url = "https://forcedactionleads.com"
    return s


class TestVoiceDropGraph:
    def _invoke(
        self,
        profile=None,
        recent_drop=None,
        agent_id="agent_123",
        voice_consent=True,
        compliance_allowed=True,
    ):
        from src.agents.graphs.synthflow_voice_drop import build_synthflow_voice_drop_graph
        from src.services.compliance_gator import ComplianceResult

        profile = profile or _make_sub_profile()

        db_ctx = MagicMock()
        db_ctx.__enter__ = MagicMock(return_value=db_ctx)
        db_ctx.__exit__ = MagicMock(return_value=False)
        db_ctx.execute.return_value.first.return_value = recent_drop

        hierarchy_result = {"action_allowed": True, "kill_switch_color": "green"}
        compliance_result = ComplianceResult(
            allowed=compliance_allowed,
            reason=None if compliance_allowed else "dnc_check_required",
        )
        apply_tags = MagicMock(return_value=True)

        with patch("src.agents.tools.read_tools.get_subscriber_profile", return_value=profile), \
             patch("src.agents.graphs.synthflow_voice_drop.get_subscriber_profile", return_value=profile), \
             patch("src.agents.graphs.synthflow_voice_drop.get_db_context", return_value=db_ctx), \
             patch("src.agents.graphs.synthflow_voice_drop.run_decision_hierarchy", return_value=hierarchy_result), \
             patch("src.agents.graphs.synthflow_voice_drop.validate_outbound", return_value=compliance_result), \
             patch("src.agents.graphs.synthflow_voice_drop.has_voice_consent", return_value=voice_consent), \
             patch("src.services.synthflow_service._apply_tags_to_contact", apply_tags), \
             patch("config.settings.get_settings", return_value=_settings_mock(agent_id=agent_id)):
            graph = build_synthflow_voice_drop_graph().compile()
            result = graph.invoke({
                "decision_id": "d-test-1",
                "subscriber_id": 1,
                "event_type": "high_intent_no_convert",
                "event_payload": {"vertical": "roofing"},
            })

        result["_apply_tags"] = apply_tags
        return result

    def test_routes_cold_dial_to_human_queue(self):
        result = self._invoke()
        assert result["sent"] is False
        assert result["call_id"] is None
        assert result["terminal_status"] == "aborted"
        assert result["failure_reason"] == "compliance:cold_dial_human_only"
        result["_apply_tags"].assert_called_once_with("ghl_123", ["cold_dial_human_required"])

    def test_no_phone_aborts(self):
        result = self._invoke(profile=_make_sub_profile(phone=None))
        assert result["terminal_status"] == "aborted"
        assert "no_phone" in result.get("failure_reason", "")

    def test_dedup_7d_skips(self):
        result = self._invoke(recent_drop=MagicMock())
        assert result["terminal_status"] == "aborted"
        assert "dedup" in result.get("failure_reason", "")

    def test_followup_sms_skipped_when_human_routed(self):
        result = self._invoke()
        assert result.get("followup_skipped_reason") == "voice_drop_not_sent"

    def test_compliance_gate_still_blocks_before_tagging(self):
        result = self._invoke(compliance_allowed=False)
        assert result["terminal_status"] == "aborted"
        assert result["failure_reason"] == "compliance:dnc_check_required"
        result["_apply_tags"].assert_not_called()

    def test_voice_consent_gate_unchanged(self):
        result = self._invoke(voice_consent=False)
        assert result["terminal_status"] == "aborted"
        assert result["failure_reason"] == "voice_consent_required"
        result["_apply_tags"].assert_not_called()

    def test_no_agent_configured_aborts(self):
        from src.agents.graphs.synthflow_voice_drop import build_synthflow_voice_drop_graph

        s = _settings_mock(agent_id=None)
        s.synthflow_outbound_agent_roofing = None
        db_ctx = MagicMock()
        db_ctx.__enter__ = MagicMock(return_value=db_ctx)
        db_ctx.__exit__ = MagicMock(return_value=False)
        db_ctx.execute.return_value.first.return_value = None
        profile = _make_sub_profile()

        with patch("src.agents.graphs.synthflow_voice_drop.get_subscriber_profile", return_value=profile), \
             patch("src.agents.graphs.synthflow_voice_drop.get_db_context", return_value=db_ctx), \
             patch("config.settings.get_settings", return_value=s):
            graph = build_synthflow_voice_drop_graph().compile()
            result = graph.invoke({
                "decision_id": "d-test-2",
                "subscriber_id": 1,
                "event_type": "high_intent_no_convert",
                "event_payload": {},
            })

        assert result["terminal_status"] == "aborted"
        assert "no_agent" in result.get("failure_reason", "")


class TestRouterWiring:
    def test_high_intent_no_convert_registered(self):
        from src.agents.router import EVENT_TO_GRAPH

        assert "high_intent_no_convert" in EVENT_TO_GRAPH
        spec = EVENT_TO_GRAPH["high_intent_no_convert"]
        assert spec.graph_name == "synthflow_voice_drop"

    def test_runner_callable(self):
        from src.agents.router import EVENT_TO_GRAPH

        spec = EVENT_TO_GRAPH["high_intent_no_convert"]
        assert callable(spec.runner)
