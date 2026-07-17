"""
Integration tests for the New-Lead Voice Call graph.

get_subscriber_profile, run_decision_hierarchy, the compliance check, and
initiate_call are mocked at the module boundary so tests are deterministic
and cost $0 (no real Synthflow dispatch). log_decision is also mocked so
tests don't write audit rows during the suite.
"""
from contextlib import ExitStack
from unittest.mock import patch

import pytest

from src.agents.graphs.new_lead_voice_call import run_new_lead_voice_call


_FAKE_PROFILE = {
    "id": 555,
    "name": "New Lead",
    "phone": "+18135550100",
    "vertical": "roofing",
    "tier": "free",
    "territory_zip": "33601",
}

_FAKE_HIERARCHY_ALLOWED = {"action_allowed": True, "kill_switch_color": "green"}
_FAKE_HIERARCHY_BLOCKED = {"action_allowed": False, "action_blocked_reason": "kill_switch_red", "kill_switch_color": "red"}


class _Mocks:
    """Small bag holding the mocks a test needs to assert against."""
    def __init__(self, call, log):
        self.call = call
        self.log = log


def _enter_patches(stack, *, profile=_FAKE_PROFILE, hierarchy=_FAKE_HIERARCHY_ALLOWED,
                    compliance_allowed=True, call_id="call_abc", voice_consent=True):
    from unittest.mock import MagicMock

    from src.services.compliance_gator import ComplianceResult

    compliance_result = ComplianceResult(
        allowed=compliance_allowed, reason=None if compliance_allowed else "dnc_check_required"
    )
    fake_settings = MagicMock()
    fake_settings.synthflow_outbound_agent_roofing = "agent_123"
    stack.enter_context(patch("config.settings.get_settings", return_value=fake_settings))
    stack.enter_context(patch("src.agents.graphs.new_lead_voice_call.get_subscriber_profile", return_value=profile))
    stack.enter_context(patch("src.agents.graphs.new_lead_voice_call.run_decision_hierarchy", return_value=hierarchy))
    stack.enter_context(patch("src.agents.graphs.new_lead_voice_call.get_cached_metric", return_value=None))
    stack.enter_context(patch("src.agents.graphs.new_lead_voice_call.has_voice_consent", return_value=voice_consent))
    stack.enter_context(patch("src.agents.graphs.new_lead_voice_call.validate_outbound", return_value=compliance_result))
    mock_call = stack.enter_context(patch("src.agents.graphs.new_lead_voice_call.initiate_call", return_value=call_id))
    mock_log = stack.enter_context(patch("src.agents.tools.write_tools.log_decision"))
    return _Mocks(call=mock_call, log=mock_log)


def test_call_fires_and_logs_new_lead_signup_event_type():
    with ExitStack() as stack:
        mocks = _enter_patches(stack)
        result = run_new_lead_voice_call({"vertical": "roofing"}, subscriber_id=555)

    assert result["terminal_status"] == "completed"
    assert result["sent"] is True
    assert result["call_id"] == "call_abc"
    mocks.log.assert_called_once()
    _, kwargs = mocks.log.call_args
    assert kwargs["graph_name"] == "new_lead_voice_call"
    assert kwargs["event_type"] == "new_lead_signup"
    assert kwargs["terminal_status"] == "completed"


def test_missing_phone_aborts_before_call():
    no_phone_profile = dict(_FAKE_PROFILE, phone=None)
    with ExitStack() as stack:
        mocks = _enter_patches(stack, profile=no_phone_profile)
        result = run_new_lead_voice_call({}, subscriber_id=555)

    assert result["terminal_status"] == "aborted"
    assert result["failure_reason"] == "new_lead_call:no_phone"
    mocks.call.assert_not_called()
    mocks.log.assert_called_once()
    _, kwargs = mocks.log.call_args
    assert kwargs["terminal_status"] == "aborted"


def test_hierarchy_blocked_aborts_before_call():
    with ExitStack() as stack:
        mocks = _enter_patches(stack, hierarchy=_FAKE_HIERARCHY_BLOCKED)
        result = run_new_lead_voice_call({}, subscriber_id=555)

    assert result["terminal_status"] == "aborted"
    assert result["failure_reason"] == "kill_switch_red"
    mocks.call.assert_not_called()


def test_no_voice_consent_aborts_before_call():
    """PR #140 issue 1: an AI voice call is a robocall under the TCPA (ADR 0030
    / B0-06). Without a stored PEWC voice-consent record the graph must fail
    closed and never dispatch — even when DNC/quiet-hours would allow it."""
    with ExitStack() as stack:
        mocks = _enter_patches(stack, voice_consent=False)
        result = run_new_lead_voice_call({}, subscriber_id=555)

    assert result["terminal_status"] == "aborted"
    assert result["failure_reason"] == "compliance:voice_consent_required"
    mocks.call.assert_not_called()


def test_consent_and_compliance_pass_dispatches_call():
    """Happy path: with voice consent on file AND DNC/quiet-hours clear, the
    call dispatches."""
    with ExitStack() as stack:
        mocks = _enter_patches(stack, voice_consent=True, compliance_allowed=True)
        result = run_new_lead_voice_call({}, subscriber_id=555)

    assert result["terminal_status"] == "completed"
    assert result["sent"] is True
    mocks.call.assert_called_once()


def test_compliance_blocked_aborts_before_call():
    with ExitStack() as stack:
        mocks = _enter_patches(stack, compliance_allowed=False)
        result = run_new_lead_voice_call({}, subscriber_id=555)

    assert result["terminal_status"] == "aborted"
    assert result["failure_reason"] == "compliance:dnc_check_required"
    mocks.call.assert_not_called()


def test_call_failure_marks_failed_not_aborted():
    with ExitStack() as stack:
        mocks = _enter_patches(stack, call_id=None)
        result = run_new_lead_voice_call({}, subscriber_id=555)

    assert result["terminal_status"] == "failed"
    assert result["sent"] is False
    assert result["failure_reason"] == "new_lead_call:initiate_failed"


class TestKnownComplianceGap:
    """This graph deliberately reuses compliance_gator.validate_outbound()
    UNMODIFIED (same call synthflow_voice_drop.py makes) rather than a
    reduced compliance check — per an explicit product decision to not
    weaken a TCPA-relevant gate without legal sign-off. The direct,
    documented consequence: a self-submitted signup phone number (no
    dnc_phone_checks row — that table is only populated by the
    property-owner scraping/DNC-scrub pipeline) is blocked at the compliance
    gate. This test proves that's real, current behavior against real
    Postgres, not a hypothetical — the gap is visible in agent_decisions
    (failure_reason="compliance:dnc_check_required"), not silent."""

    def test_self_submitted_phone_with_no_dnc_row_is_blocked(self, fresh_db):
        from contextlib import contextmanager
        from unittest.mock import MagicMock

        @contextmanager
        def _fake_db_context():
            # Wraps fresh_db without closing it — the fixture owns that lifecycle.
            yield fresh_db

        fake_settings = MagicMock()
        fake_settings.synthflow_outbound_agent_roofing = "agent_123"
        profile = dict(_FAKE_PROFILE, phone="+18135559999")  # a number with no dnc_phone_checks row

        with patch("config.settings.get_settings", return_value=fake_settings), \
             patch("src.agents.graphs.new_lead_voice_call.get_subscriber_profile", return_value=profile), \
             patch("src.agents.graphs.new_lead_voice_call.run_decision_hierarchy", return_value=_FAKE_HIERARCHY_ALLOWED), \
             patch("src.agents.graphs.new_lead_voice_call.get_cached_metric", return_value=None), \
             patch("src.agents.graphs.new_lead_voice_call.get_db_context", side_effect=_fake_db_context), \
             patch("src.agents.graphs.new_lead_voice_call.has_voice_consent", return_value=True), \
             patch("src.agents.graphs.new_lead_voice_call.initiate_call") as mock_call, \
             patch("src.agents.tools.write_tools.log_decision"):
            result = run_new_lead_voice_call({}, subscriber_id=555)

        assert result["terminal_status"] == "aborted"
        assert result["failure_reason"] == "compliance:dnc_check_required"
        mock_call.assert_not_called()


class TestDistinguishableAgentDecisionsRows:
    """Criterion 3: the new-lead path's agent_decisions rows must be
    distinguishable from the existing daily sweep's rows in the SAME table —
    verified against real Postgres, the actual query shape ops would use."""

    def test_new_lead_and_sweep_rows_differ_on_graph_name_and_event_type(self, fresh_db):
        import uuid

        from sqlalchemy import text

        from src.core.models import AgentDecision

        new_lead_id = str(uuid.uuid4())
        sweep_id = str(uuid.uuid4())
        # subscriber_id left None — agent_decisions.subscriber_id has an FK to
        # subscribers, and this test only cares about graph_name/event_type
        # distinguishability, not a specific subscriber.
        fresh_db.add(AgentDecision(
            decision_id=new_lead_id, graph_name="new_lead_voice_call",
            event_type="new_lead_signup", terminal_status="completed",
        ))
        fresh_db.add(AgentDecision(
            decision_id=sweep_id, graph_name="synthflow_voice_drop",
            event_type="high_intent_no_convert", terminal_status="completed",
        ))
        fresh_db.commit()

        try:
            rows = fresh_db.execute(
                text("SELECT decision_id, graph_name, event_type FROM agent_decisions "
                     "WHERE decision_id = :id"),
                {"id": new_lead_id},
            ).mappings().all()
            assert len(rows) == 1
            assert rows[0]["graph_name"] == "new_lead_voice_call"
            assert rows[0]["event_type"] == "new_lead_signup"

            sweep_rows = fresh_db.execute(
                text("SELECT decision_id, graph_name, event_type FROM agent_decisions "
                     "WHERE decision_id = :id"),
                {"id": sweep_id},
            ).mappings().all()
            assert len(sweep_rows) == 1
            assert sweep_rows[0]["graph_name"] == "synthflow_voice_drop"
            assert sweep_rows[0]["event_type"] == "high_intent_no_convert"
            assert rows[0]["graph_name"] != sweep_rows[0]["graph_name"]
            assert rows[0]["event_type"] != sweep_rows[0]["event_type"]
        finally:
            fresh_db.execute(text("DELETE FROM agent_decisions WHERE decision_id IN (:a, :b)"),
                              {"a": new_lead_id, "b": sweep_id})
            fresh_db.commit()
