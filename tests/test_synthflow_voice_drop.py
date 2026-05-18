"""
Unit tests for Phase C — Synthflow outbound voice drop.
"""
import sys
from unittest.mock import MagicMock, patch, call
import pytest

# Stub config.agents so src.agents.__init__ doesn't require real env on import.
_agents_stub = MagicMock()
_agents_stub.get_agents_settings = MagicMock(return_value=MagicMock())
if "config.agents" not in sys.modules:
    sys.modules["config.agents"] = _agents_stub


def _make_sub_profile(sub_id=1, phone="+13135550101", vertical="roofing"):
    return {"id": sub_id, "name": "Test User", "phone": phone, "vertical": vertical}


def _settings_mock(agent_id="agent_123", api_key="sf_key"):
    s = MagicMock()
    s.synthflow_api_key = MagicMock()
    s.synthflow_api_key.get_secret_value.return_value = api_key
    s.synthflow_api_base = "https://api.synthflow.ai/v2"
    s.synthflow_outbound_agent_roofing = agent_id
    return s


# ─── synthflow_client tests ───────────────────────────────────────────────────

class TestSynthflowClient:
    def test_returns_call_id_on_success(self):
        from src.services.synthflow_client import initiate_call
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"call_id": "call_abc"}
        mock_resp.raise_for_status = MagicMock()
        httpx_mock = MagicMock()
        httpx_mock.post.return_value = mock_resp

        with patch.dict(sys.modules, {"httpx": httpx_mock}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            result = initiate_call("+13135550101", "agent_123", {"foo": "bar"})

        assert result == "call_abc"

    def test_returns_none_when_no_api_key(self):
        from src.services.synthflow_client import initiate_call
        s = MagicMock()
        s.synthflow_api_key = None

        with patch("config.settings.get_settings", return_value=s):
            result = initiate_call("+13135550101", "agent_123", {})

        assert result is None

    def test_returns_none_on_http_error(self):
        from src.services.synthflow_client import initiate_call
        httpx_mock = MagicMock()
        httpx_mock.post.side_effect = Exception("timeout")

        with patch.dict(sys.modules, {"httpx": httpx_mock}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            result = initiate_call("+13135550101", "agent_123", {})

        assert result is None

    def test_uses_id_field_fallback(self):
        """Synthflow may return 'id' instead of 'call_id'."""
        from src.services.synthflow_client import initiate_call
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"id": "call_xyz"}
        mock_resp.raise_for_status = MagicMock()
        httpx_mock = MagicMock()
        httpx_mock.post.return_value = mock_resp

        with patch.dict(sys.modules, {"httpx": httpx_mock}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            result = initiate_call("+13135550101", "agent_123", {})

        assert result == "call_xyz"

    def test_passes_context_as_metadata(self):
        from src.services.synthflow_client import initiate_call
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"call_id": "c1"}
        mock_resp.raise_for_status = MagicMock()
        httpx_mock = MagicMock()
        httpx_mock.post.return_value = mock_resp
        ctx = {"subscriber_id": 7, "vertical": "roofing"}

        with patch.dict(sys.modules, {"httpx": httpx_mock}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            initiate_call("+13135550101", "agent_123", ctx)

        _, kwargs = httpx_mock.post.call_args
        assert kwargs["json"]["metadata"] == ctx


# ─── VoiceDropGraph tests ────────────────────────────────────────────────────

class TestVoiceDropGraph:
    def _invoke(
        self,
        profile=None,
        call_id="c1",
        recent_drop=None,
        agent_id="agent_123",
        sms_result=True,
        sms_mock=None,
    ):
        from src.agents.graphs.synthflow_voice_drop import build_synthflow_voice_drop_graph

        profile = profile or _make_sub_profile()

        db_ctx = MagicMock()
        db_ctx.__enter__ = MagicMock(return_value=db_ctx)
        db_ctx.__exit__ = MagicMock(return_value=False)
        db_ctx.execute.return_value.first.return_value = recent_drop
        db_ctx.add = MagicMock()
        db_ctx.commit = MagicMock()

        hierarchy_result = {"action_allowed": True, "kill_switch_color": "green"}

        # Build a fake sms_compliance module so the lazy import inside
        # _node_followup_sms resolves to our mock without needing real telnyx env.
        sms_module = MagicMock()
        sms_module.send_sms = sms_mock or MagicMock(return_value=sms_result)

        with patch("src.agents.tools.read_tools.get_subscriber_profile", return_value=profile), \
             patch("src.agents.graphs.synthflow_voice_drop.get_subscriber_profile", return_value=profile), \
             patch("src.agents.graphs.synthflow_voice_drop.get_db_context", return_value=db_ctx), \
             patch("src.agents.graphs.synthflow_voice_drop.run_decision_hierarchy", return_value=hierarchy_result), \
             patch("src.services.synthflow_client.initiate_call", return_value=call_id), \
             patch("src.agents.graphs.synthflow_voice_drop.initiate_call", return_value=call_id), \
             patch.dict(sys.modules, {"src.services.sms_compliance": sms_module}), \
             patch("config.settings.get_settings", return_value=_settings_mock(agent_id=agent_id)):
            graph = build_synthflow_voice_drop_graph().compile()
            result = graph.invoke({
                "decision_id": "d-test-1",
                "subscriber_id": 1,
                "event_type": "high_intent_no_convert",
                "event_payload": {"vertical": "roofing"},
            })
        result["_sms_mock"] = sms_module.send_sms
        return result

    def test_happy_path_sent_true(self):
        result = self._invoke()
        assert result["sent"] is True
        assert result["call_id"] == "c1"
        assert result["terminal_status"] == "completed"

    def test_no_phone_aborts(self):
        profile = _make_sub_profile(phone=None)
        result = self._invoke(profile=profile)
        assert result["terminal_status"] == "aborted"
        assert "no_phone" in result.get("failure_reason", "")

    def test_subscriber_not_found_aborts(self):
        result = self._invoke(profile=None)
        assert result["terminal_status"] == "aborted"

    def test_dedup_7d_skips(self):
        result = self._invoke(recent_drop=MagicMock())
        assert result["terminal_status"] == "aborted"
        assert "dedup" in result.get("failure_reason", "")

    def test_initiate_fail_marks_failed(self):
        result = self._invoke(call_id=None)
        assert result["sent"] is False
        assert result["terminal_status"] == "failed"

    def test_followup_sms_fires_on_success(self):
        """SMS reinforcement must fire after a successful voice drop dispatch
        (covers all eligible outcomes — outcome isn't known yet at this point)."""
        result = self._invoke()
        assert result["followup_sent"] is True
        assert result["_sms_mock"].call_count == 1
        _, kwargs = result["_sms_mock"].call_args
        assert kwargs["task_type"] == "synthflow_voice_drop_followup"
        assert kwargs["message_type"] == "marketing"
        assert kwargs["decision_id"] == "d-test-1"

    def test_followup_sms_skipped_when_voice_drop_failed(self):
        result = self._invoke(call_id=None)
        assert result.get("followup_sent") is False
        assert result["_sms_mock"].call_count == 0

    def test_followup_sms_blocked_by_compliance(self):
        """send_sms returning False (opt-out / quiet hours / no opt-in) must
        not break the graph; followup_sent stays False and terminal_status
        still reflects the underlying voice-drop send."""
        result = self._invoke(sms_result=False)
        assert result["sent"] is True
        assert result["followup_sent"] is False
        assert result.get("followup_skipped_reason") == "compliance_suppressed"
        assert result["terminal_status"] == "completed"

    def test_no_duplicate_sms_on_rerun(self):
        """The 7-day dedup at assemble_context aborts the graph before
        initiate_drop / followup_sms, so a rerun within the window fires
        exactly zero SMS."""
        result = self._invoke(recent_drop=MagicMock())
        assert result["terminal_status"] == "aborted"
        assert result["_sms_mock"].call_count == 0

    def test_no_agent_configured_aborts(self):
        s = _settings_mock(agent_id=None)
        s.synthflow_outbound_agent_roofing = None

        db_ctx = MagicMock()
        db_ctx.__enter__ = MagicMock(return_value=db_ctx)
        db_ctx.__exit__ = MagicMock(return_value=False)
        db_ctx.execute.return_value.first.return_value = None

        from src.agents.graphs.synthflow_voice_drop import build_synthflow_voice_drop_graph
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


# ─── router wiring test ──────────────────────────────────────────────────────

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
