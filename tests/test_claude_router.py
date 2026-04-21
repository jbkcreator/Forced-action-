"""
Claude router service tests.

Unit tests:  mock Anthropic client — no real API calls.
Integration: real Postgres fresh_db for ApiUsageLog persistence tests.

Run:
    pytest tests/test_claude_router.py -v
    pytest tests/test_claude_router.py -v -k "unit"
    pytest tests/test_claude_router.py -v -k "integration"
"""

from unittest.mock import MagicMock, patch, call

import pytest
from sqlalchemy import select

import src.services.claude_router  # noqa: F401 — ensure importable

from src.core.models import ApiUsageLog, Subscriber
from src.services.claude_router import (
    _COST_TABLE,
    _TASK_ROUTING,
    _extract_text,
    _log_usage,
    _model_id,
    call_claude,
    call_claude_batch,
)


# ============================================================================
# Helpers
# ============================================================================


def _make_response(text="Test response", input_tokens=100, output_tokens=50, model="claude-sonnet-4-6"):
    """Build a mock Anthropic response object."""
    block = MagicMock()
    block.text = text
    # TextBlock identity check uses isinstance — patch via spec
    from anthropic.types import TextBlock
    block.__class__ = TextBlock

    usage = MagicMock()
    usage.input_tokens = input_tokens
    usage.output_tokens = output_tokens

    response = MagicMock()
    response.content = [block]
    response.usage = usage
    response.model = model
    return response


# ============================================================================
# Unit tests — no DB, no API calls
# ============================================================================


class TestTaskRoutingUnit:
    @pytest.mark.parametrize("task_type,expected_tier", [
        ("sms_copy",            "haiku"),
        ("classification",      "haiku"),
        ("command_parsing",     "haiku"),
        ("batch_summarization", "haiku"),
        ("address_matching",    "haiku"),
        ("keyword_extraction",  "haiku"),
        ("conversational_close","sonnet"),
        ("complex_reasoning",   "sonnet"),
        ("lead_analysis",       "sonnet"),
        ("learning_card",       "sonnet"),
        ("retention_copy",      "sonnet"),
        ("edge_case",           "opus"),
    ])
    def test_task_routes_to_correct_tier(self, task_type, expected_tier):
        assert _TASK_ROUTING[task_type] == expected_tier

    def test_unknown_task_defaults_to_sonnet_in_call_claude(self):
        response = _make_response()
        with patch("src.services.claude_router.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = response
            with patch("src.services.claude_router.settings") as mock_settings:
                mock_settings.anthropic_api_key.get_secret_value.return_value = "sk-test"
                mock_settings.claude_sonnet_model = "claude-sonnet-4-6"
                mock_settings.claude_haiku_model = "claude-haiku-4-5"
                mock_settings.claude_opus_model = "claude-opus-4-7"
                call_claude("unknown_task_xyz", [{"role": "user", "content": "hi"}])
        kwargs = mock_anthropic.return_value.messages.create.call_args[1]
        assert kwargs["model"] == "claude-sonnet-4-6"


class TestModelIdUnit:
    def test_haiku_returns_haiku_model(self):
        with patch("src.services.claude_router.settings") as s:
            s.claude_haiku_model = "claude-haiku-4-5-20251001"
            s.claude_sonnet_model = "claude-sonnet-4-6"
            s.claude_opus_model = "claude-opus-4-7"
            assert _model_id("haiku") == "claude-haiku-4-5-20251001"

    def test_sonnet_returns_sonnet_model(self):
        with patch("src.services.claude_router.settings") as s:
            s.claude_haiku_model = "claude-haiku-4-5-20251001"
            s.claude_sonnet_model = "claude-sonnet-4-6"
            s.claude_opus_model = "claude-opus-4-7"
            assert _model_id("sonnet") == "claude-sonnet-4-6"

    def test_opus_returns_opus_model(self):
        with patch("src.services.claude_router.settings") as s:
            s.claude_haiku_model = "claude-haiku-4-5-20251001"
            s.claude_sonnet_model = "claude-sonnet-4-6"
            s.claude_opus_model = "claude-opus-4-7"
            assert _model_id("opus") == "claude-opus-4-7"

    def test_unknown_tier_falls_back_to_sonnet(self):
        with patch("src.services.claude_router.settings") as s:
            s.claude_haiku_model = "claude-haiku-4-5-20251001"
            s.claude_sonnet_model = "claude-sonnet-4-6"
            s.claude_opus_model = "claude-opus-4-7"
            assert _model_id("unknown") == "claude-sonnet-4-6"


class TestExtractTextUnit:
    def test_extracts_text_from_text_block(self):
        response = _make_response(text="Hello world")
        assert _extract_text(response) == "Hello world"

    def test_returns_empty_string_when_no_text_block(self):
        response = MagicMock()
        response.content = []
        assert _extract_text(response) == ""

    def test_returns_first_block_only(self):
        from anthropic.types import TextBlock
        block1 = MagicMock()
        block1.__class__ = TextBlock
        block1.text = "first"
        block2 = MagicMock()
        block2.__class__ = TextBlock
        block2.text = "second"
        response = MagicMock()
        response.content = [block1, block2]
        assert _extract_text(response) == "first"


class TestCostTableUnit:
    def test_all_tiers_have_input_and_output_costs(self):
        for tier in ("haiku", "sonnet", "opus"):
            assert "input" in _COST_TABLE[tier]
            assert "output" in _COST_TABLE[tier]

    def test_haiku_cheapest_sonnet_middle_opus_most_expensive(self):
        assert _COST_TABLE["haiku"]["input"] < _COST_TABLE["sonnet"]["input"]
        assert _COST_TABLE["sonnet"]["input"] < _COST_TABLE["opus"]["input"]


class TestLogUsageUnit:
    def test_logs_to_db_when_session_provided(self):
        db = MagicMock()
        response = _make_response(input_tokens=200, output_tokens=100)
        _log_usage(response, "haiku", "sms_copy", subscriber_id=1, db=db)
        db.add.assert_called_once()
        logged: ApiUsageLog = db.add.call_args[0][0]
        assert logged.service == "claude"
        assert logged.model == "haiku"
        assert logged.input_tokens == 200
        assert logged.output_tokens == 100
        assert logged.task_type == "sms_copy"
        assert logged.subscriber_id == 1
        assert logged.cost_usd > 0

    def test_does_not_crash_when_db_is_none(self):
        response = _make_response(input_tokens=100, output_tokens=50)
        _log_usage(response, "sonnet", "classification", subscriber_id=None, db=None)

    def test_cost_calculation_haiku(self):
        db = MagicMock()
        response = _make_response(input_tokens=1_000_000, output_tokens=1_000_000)
        _log_usage(response, "haiku", "sms_copy", subscriber_id=None, db=db)
        logged: ApiUsageLog = db.add.call_args[0][0]
        expected = (_COST_TABLE["haiku"]["input"] + _COST_TABLE["haiku"]["output"]) / 1
        assert abs(float(logged.cost_usd) - expected) < 0.001

    def test_cost_calculation_sonnet(self):
        db = MagicMock()
        response = _make_response(input_tokens=1_000_000, output_tokens=1_000_000)
        _log_usage(response, "sonnet", "complex_reasoning", subscriber_id=None, db=db)
        logged: ApiUsageLog = db.add.call_args[0][0]
        expected = (_COST_TABLE["sonnet"]["input"] + _COST_TABLE["sonnet"]["output"]) / 1
        assert abs(float(logged.cost_usd) - expected) < 0.001

    def test_does_not_crash_on_missing_usage(self):
        db = MagicMock()
        response = MagicMock()
        response.usage = None
        _log_usage(response, "haiku", "sms_copy", subscriber_id=None, db=db)
        db.add.assert_not_called()

    def test_db_exception_does_not_propagate(self):
        db = MagicMock()
        db.add.side_effect = Exception("DB error")
        response = _make_response()
        # Must not raise
        _log_usage(response, "haiku", "sms_copy", subscriber_id=None, db=db)


class TestCallClaudeUnit:
    def _patched_call(self, task_type, messages, **kwargs):
        response = _make_response()
        with patch("src.services.claude_router.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = response
            with patch("src.services.claude_router.settings") as mock_settings:
                mock_settings.anthropic_api_key.get_secret_value.return_value = "sk-test"
                mock_settings.claude_haiku_model = "claude-haiku-4-5-20251001"
                mock_settings.claude_sonnet_model = "claude-sonnet-4-6"
                mock_settings.claude_opus_model = "claude-opus-4-7"
                result = call_claude(task_type, messages, **kwargs)
                create_kwargs = mock_anthropic.return_value.messages.create.call_args[1]
        return result, create_kwargs

    def test_returns_text_response(self):
        result, _ = self._patched_call("sms_copy", [{"role": "user", "content": "Write SMS"}])
        assert result == "Test response"

    def test_haiku_task_uses_haiku_model(self):
        _, kwargs = self._patched_call("sms_copy", [{"role": "user", "content": "hi"}])
        assert kwargs["model"] == "claude-haiku-4-5-20251001"

    def test_sonnet_task_uses_sonnet_model(self):
        _, kwargs = self._patched_call("conversational_close", [{"role": "user", "content": "hi"}])
        assert kwargs["model"] == "claude-sonnet-4-6"

    def test_opus_task_uses_opus_model(self):
        _, kwargs = self._patched_call("edge_case", [{"role": "user", "content": "hi"}])
        assert kwargs["model"] == "claude-opus-4-7"

    def test_system_prompt_passed_as_string_without_caching(self):
        _, kwargs = self._patched_call(
            "sms_copy",
            [{"role": "user", "content": "hi"}],
            system="You are Cora.",
        )
        assert kwargs["system"] == "You are Cora."

    def test_system_prompt_wrapped_with_cache_control_when_cache_true(self):
        _, kwargs = self._patched_call(
            "conversational_close",
            [{"role": "user", "content": "hi"}],
            system="You are Cora.",
            cache_system=True,
        )
        assert isinstance(kwargs["system"], list)
        block = kwargs["system"][0]
        assert block["type"] == "text"
        assert block["text"] == "You are Cora."
        assert block["cache_control"] == {"type": "ephemeral"}

    def test_no_system_prompt_omits_system_key(self):
        _, kwargs = self._patched_call("sms_copy", [{"role": "user", "content": "hi"}])
        assert "system" not in kwargs

    def test_max_tokens_passed_through(self):
        _, kwargs = self._patched_call(
            "sms_copy",
            [{"role": "user", "content": "hi"}],
            max_tokens=256,
        )
        assert kwargs["max_tokens"] == 256

    def test_messages_passed_through(self):
        msgs = [{"role": "user", "content": "Hello"}]
        _, kwargs = self._patched_call("sms_copy", msgs)
        assert kwargs["messages"] == msgs


class TestCallClaudeBatchUnit:
    def test_submits_batch_and_returns_id(self):
        mock_batch = MagicMock()
        mock_batch.id = "msgbatch_test123"
        with patch("src.services.claude_router.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.beta.messages.batches.create.return_value = mock_batch
            with patch("src.services.claude_router.settings") as mock_settings:
                mock_settings.anthropic_api_key.get_secret_value.return_value = "sk-test"
                mock_settings.claude_haiku_model = "claude-haiku-4-5-20251001"
                mock_settings.claude_sonnet_model = "claude-sonnet-4-6"
                mock_settings.claude_opus_model = "claude-opus-4-7"
                batch_id = call_claude_batch(
                    "batch_summarization",
                    [{"custom_id": "req1", "params": {"messages": []}}],
                )
        assert batch_id == "msgbatch_test123"

    def test_injects_model_into_each_request(self):
        mock_batch = MagicMock()
        mock_batch.id = "msgbatch_test456"
        requests = [
            {"custom_id": "r1", "params": {"messages": []}},
            {"custom_id": "r2", "params": {"messages": []}},
        ]
        with patch("src.services.claude_router.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.beta.messages.batches.create.return_value = mock_batch
            with patch("src.services.claude_router.settings") as mock_settings:
                mock_settings.anthropic_api_key.get_secret_value.return_value = "sk-test"
                mock_settings.claude_haiku_model = "claude-haiku-4-5-20251001"
                mock_settings.claude_sonnet_model = "claude-sonnet-4-6"
                mock_settings.claude_opus_model = "claude-opus-4-7"
                call_claude_batch("batch_summarization", requests)

        submitted = mock_anthropic.return_value.beta.messages.batches.create.call_args[1]["requests"]
        for req in submitted:
            assert req["params"]["model"] == "claude-haiku-4-5-20251001"


# ============================================================================
# Integration tests — real Postgres, ApiUsageLog written and rolled back
# ============================================================================


class TestApiUsageLogIntegration:
    def test_log_usage_persists_to_db(self, fresh_db):
        response = _make_response(input_tokens=500, output_tokens=200)
        _log_usage(response, "haiku", "sms_copy", subscriber_id=None, db=fresh_db)
        fresh_db.flush()
        row = fresh_db.execute(
            select(ApiUsageLog).where(ApiUsageLog.task_type == "sms_copy")
        ).scalar_one_or_none()
        assert row is not None
        assert row.service == "claude"
        assert row.model == "haiku"
        assert row.input_tokens == 500
        assert row.output_tokens == 200
        assert float(row.cost_usd) > 0

    def test_cost_stored_with_precision(self, fresh_db):
        response = _make_response(input_tokens=1234, output_tokens=567)
        _log_usage(response, "sonnet", "lead_analysis", subscriber_id=None, db=fresh_db)
        fresh_db.flush()
        row = fresh_db.execute(
            select(ApiUsageLog).where(ApiUsageLog.task_type == "lead_analysis")
        ).scalar_one_or_none()
        assert row is not None
        expected = (1234 * 3.00 + 567 * 15.00) / 1_000_000
        assert abs(float(row.cost_usd) - expected) < 0.000001

    def test_multiple_calls_create_multiple_rows(self, fresh_db):
        response = _make_response()
        _log_usage(response, "haiku", "classification", subscriber_id=None, db=fresh_db)
        _log_usage(response, "haiku", "classification", subscriber_id=None, db=fresh_db)
        fresh_db.flush()
        rows = fresh_db.execute(
            select(ApiUsageLog).where(ApiUsageLog.task_type == "classification")
        ).scalars().all()
        assert len(rows) == 2

    def test_subscriber_id_stored(self, fresh_db):
        sub = Subscriber(
            stripe_customer_id="cus_router_test",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            founding_member=False,
            status="active",
            email="router_test@example.com",
            name="Router Test",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        response = _make_response()
        _log_usage(response, "sonnet", "retention_copy", subscriber_id=sub.id, db=fresh_db)
        fresh_db.flush()
        row = fresh_db.execute(
            select(ApiUsageLog).where(ApiUsageLog.task_type == "retention_copy")
        ).scalar_one_or_none()
        assert row is not None
        assert row.subscriber_id == sub.id
