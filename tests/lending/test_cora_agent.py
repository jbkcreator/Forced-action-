"""Cora's entry point: settings mapping, fail-closed startup and the lending send-time check.

No Slack or database is touched: the send check runs against a scripted connection.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from packages.agent_core.pending_actions import ActionStatus, PendingAction
from src.lending import cora_agent


def _settings(**overrides) -> SimpleNamespace:
    values = dict(
        lending_cora_slack_bot_token=SecretStr("xoxb-test"),
        lending_cora_slack_app_token=SecretStr("xapp-test"),
        lending_cora_channel="C_CORA",
        lending_cora_approver_user_ids="U_JOSH",
        lending_cora_operator_user_ids="U_OPS1, U_OPS2,",
        lending_cora_model="claude-sonnet-5-5",
        lending_cora_pending_action_ttl_hours=48,
        lending_cora_calendar_id="",
        lending_cora_calendar_service_account_key_path="",
        database_url="postgresql://unused",
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def test_build_config_maps_settings() -> None:
    config = cora_agent.build_config(_settings())
    assert config.agent_name == "Cora"
    assert config.slack_bot_token == "xoxb-test"
    assert config.approver_user_ids == frozenset({"U_JOSH"})
    assert config.allowed_user_ids == frozenset({"U_JOSH", "U_OPS1", "U_OPS2"})
    assert config.db_schema == "lending"
    assert config.pending_action_ttl_hours == 48
    assert config.calendar_id is None
    assert config.missing_for_live_session() == []


def test_main_refuses_to_start_without_approvers(monkeypatch) -> None:
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings(lending_cora_approver_user_ids=""))
    assert cora_agent.main() == 1


def test_main_refuses_to_start_without_tokens(monkeypatch) -> None:
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings(lending_cora_slack_app_token=None))
    assert cora_agent.main() == 1


def test_no_egress_channel_is_registered_yet() -> None:
    assert cora_agent.EGRESS_EXECUTORS == {}


class ScriptedEngine:
    """Answers the send check's queries in order: suppression first, then text consent."""

    def __init__(self, *answers: bool) -> None:
        self._answers = list(answers)
        self.queries: list[dict] = []

    @contextmanager
    def connect(self):
        yield self

    def execute(self, statement, params):
        self.queries.append(dict(params))
        return SimpleNamespace(scalar=lambda: self._answers.pop(0))


def _action(channel: str = "ghl_sms", phone: str | None = "(727) 555-0100", email: str | None = None) -> PendingAction:
    return PendingAction(
        action_id=1, tool_name="send_sms", channel=channel, payload={}, summary="", status=ActionStatus.SENDING,
        requested_by=None, source_channel=None, source_thread_ts=None, card_channel=None, card_ts=None,
        decided_by="U_JOSH", revised_by=None, revision_note=None, revisions=(), recipient_phone=phone,
        recipient_email=email, contact_ref=None, deal_ref=None, idempotency_key=None, expires_at=None,
        provider_ref=None, error=None,
    )


def test_consented_unsuppressed_text_may_send() -> None:
    engine = ScriptedEngine(False, True)
    assert cora_agent.make_send_check(engine)(_action()) is None
    assert engine.queries[0]["p"] == "+17275550100"


def test_suppressed_recipient_is_blocked() -> None:
    reason = cora_agent.make_send_check(ScriptedEngine(True))(_action())
    assert reason == "recipient is suppressed or marked do-not-contact"


def test_text_without_consent_is_blocked() -> None:
    reason = cora_agent.make_send_check(ScriptedEngine(False, False))(_action())
    assert reason == "no text consent on record for this number"


def test_email_needs_no_text_consent_but_is_checked_for_suppression() -> None:
    engine = ScriptedEngine(False)
    assert cora_agent.make_send_check(engine)(_action("ghl_email", phone=None, email=" Sam@Example.com")) is None
    assert engine.queries == [{"p": None, "e": "sam@example.com"}]


@pytest.mark.parametrize("channel", ["ghl_sms", "ghl_email"])
def test_no_recipient_is_blocked_without_querying(channel: str) -> None:
    engine = ScriptedEngine()
    reason = cora_agent.make_send_check(engine)(_action(channel, phone=None, email=None))
    assert reason == "no recipient recorded, so opt-out status cannot be verified"
    assert engine.queries == []
