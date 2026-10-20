"""Cora's entry point: settings mapping, fail-closed startup and the lending send-time check.

No Slack or database is touched: the send check runs against a scripted connection.
"""
from __future__ import annotations

import signal
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from packages.agent_core.relay import SendDeferred
from packages.agent_core.pending_actions import ActionStatus, PendingAction
from src.lending import cora_agent


def _settings(**overrides) -> SimpleNamespace:
    values = dict(
        lending_cora_slack_bot_token=SecretStr("xoxb-test"),
        lending_cora_slack_app_token=SecretStr("xapp-test"),
        anthropic_api_key=SecretStr("sk-test"),
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
    assert config.slack_app_token == "xapp-test"
    assert cora_agent.missing_settings(config) == []


def test_cora_uses_her_own_slack_app_not_the_lending_app() -> None:
    config = cora_agent.build_config(_settings(lending_slack_bot_token=SecretStr("xoxb-lending")))
    assert config.slack_bot_token == "xoxb-test"


@pytest.fixture(autouse=True)
def _no_real_sigterm_handler(monkeypatch):
    """main() installs a SIGTERM handler; keep it out of the test process."""
    monkeypatch.setattr(cora_agent.signal, "signal", lambda *args: None)


@pytest.fixture
def socket_runs(monkeypatch) -> list[dict]:
    """Replaces the Socket Mode connection: records each call and blocks until stop, like the real one."""
    runs: list[dict] = []

    def fake_run_socket_mode(session, *, app_token, web_client, stop):
        runs.append({"session": session, "app_token": app_token, "web_client": web_client})
        stop.wait()

    monkeypatch.setattr(cora_agent, "run_socket_mode", fake_run_socket_mode)
    return runs


class _Gate:
    def __init__(self, error: Exception | None = None) -> None:
        self.adopted = 0
        self._error = error

    def adopt_cards(self) -> int:
        self.adopted += 1
        if self._error:
            raise self._error
        return 0


def _services(session="session", web="web", gate: _Gate | None = None) -> SimpleNamespace:
    return SimpleNamespace(session=session, web_client=web, gate=gate or _Gate())


def test_main_adopts_open_cards_once_before_connecting(monkeypatch, socket_runs) -> None:
    gate = _Gate()
    stop = threading.Event()
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings())
    monkeypatch.setattr(cora_agent, "build_live_services", lambda settings: _services(gate=gate))
    monkeypatch.setattr(cora_agent, "run_sweep", lambda services: stop.set())
    monkeypatch.setattr(cora_agent, "SWEEP_SECONDS", 0.01)
    assert cora_agent.main(stop=stop) == 0
    assert gate.adopted == 1 and len(socket_runs) == 1


def test_a_failing_card_check_does_not_block_startup(monkeypatch, socket_runs) -> None:
    stop = threading.Event()
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings())
    monkeypatch.setattr(cora_agent, "build_live_services",
                        lambda settings: _services(gate=_Gate(error=RuntimeError("db down"))))
    monkeypatch.setattr(cora_agent, "run_sweep", lambda services: stop.set())
    monkeypatch.setattr(cora_agent, "SWEEP_SECONDS", 0.01)
    assert cora_agent.main(stop=stop) == 0 and len(socket_runs) == 1


def test_main_refuses_to_start_without_approvers(monkeypatch) -> None:
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings(lending_cora_approver_user_ids=""))
    assert cora_agent.main() == 1


def test_main_refuses_to_start_without_the_app_token(monkeypatch) -> None:
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings(lending_cora_slack_app_token=None))
    assert cora_agent.main() == 1


def test_main_runs_the_socket_session_and_the_sweep_until_sigterm(monkeypatch, socket_runs) -> None:
    services = _services()
    handlers: list = []
    swept: list = []
    stop = threading.Event()

    def sweep(given):
        swept.append(given)
        handlers[0][1]()  # SIGTERM sets the stop event

    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings())
    monkeypatch.setattr(cora_agent, "build_live_services", lambda settings: services)
    monkeypatch.setattr(cora_agent, "run_sweep", sweep)
    monkeypatch.setattr(cora_agent, "SWEEP_SECONDS", 0.01)
    monkeypatch.setattr(cora_agent.signal, "signal", lambda signum, handler: handlers.append((signum, handler)))
    assert cora_agent.main(stop=stop) == 0
    assert swept[0] is services and stop.is_set()
    assert [signum for signum, _ in handlers] == [signal.SIGTERM, signal.SIGINT]
    assert socket_runs == [{"session": "session", "app_token": "xapp-test", "web_client": "web"}]


def test_a_failing_sweep_does_not_stop_the_worker(monkeypatch, socket_runs) -> None:
    stop = threading.Event()
    runs: list = []

    def sweep(_):
        runs.append(1)
        if len(runs) == 1:
            raise RuntimeError("db down")
        stop.set()

    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings())
    monkeypatch.setattr(cora_agent, "build_live_services", lambda settings: _services())
    monkeypatch.setattr(cora_agent, "run_sweep", sweep)
    monkeypatch.setattr(cora_agent, "SWEEP_SECONDS", 0.01)
    assert cora_agent.main(stop=stop) == 0 and len(runs) == 2


def test_a_dropped_socket_session_reconnects(monkeypatch) -> None:
    stop = threading.Event()
    attempts: list = []

    def flaky_run_socket_mode(session, *, app_token, web_client, stop):
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError("socket refused")
        stop.set()

    monkeypatch.setattr(cora_agent, "run_socket_mode", flaky_run_socket_mode)
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings())
    monkeypatch.setattr(cora_agent, "build_live_services", lambda settings: _services())
    monkeypatch.setattr(cora_agent, "run_sweep", lambda services: None)
    monkeypatch.setattr(cora_agent, "SWEEP_SECONDS", 0.01)
    assert cora_agent.main(stop=stop) == 0 and len(attempts) == 2


def test_main_retries_slack_at_boot_instead_of_exiting(monkeypatch, socket_runs) -> None:
    """A Slack outage at boot must not exhaust systemd's restart limit: the worker waits and retries."""
    attempts: list = []
    stop = threading.Event()

    def build(settings):
        attempts.append(1)
        return None if len(attempts) < 3 else _services()

    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings())
    monkeypatch.setattr(cora_agent, "build_live_services", build)
    monkeypatch.setattr(cora_agent, "run_sweep", lambda services: stop.set())
    monkeypatch.setattr(cora_agent, "SWEEP_SECONDS", 0.01)
    assert cora_agent.main(stop=stop) == 0
    assert len(attempts) == 3 and len(socket_runs) == 1


def test_main_stops_cleanly_while_waiting_for_slack(monkeypatch, socket_runs) -> None:
    stop = threading.Event()
    stop.set()
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings())
    monkeypatch.setattr(cora_agent, "build_live_services", lambda settings: None)
    assert cora_agent.main(stop=stop) == 0 and socket_runs == []


def test_main_exits_only_for_missing_settings(monkeypatch, socket_runs) -> None:
    builds: list = []
    monkeypatch.setattr(cora_agent, "get_settings", lambda: _settings(lending_cora_channel=""))
    monkeypatch.setattr(cora_agent, "build_live_services", lambda settings: builds.append(1))
    assert cora_agent.main(stop=threading.Event()) == 1
    assert builds == [] and socket_runs == []  # no Slack call, no retry loop


def test_missing_settings_are_named() -> None:
    config = cora_agent.build_config(_settings(lending_cora_slack_bot_token=None, lending_cora_slack_app_token=None,
                                               lending_cora_channel="", anthropic_api_key=None))
    assert cora_agent.missing_settings(config) == ["slack_bot_token", "slack_app_token", "slack_channel_id",
                                                   "anthropic_api_key"]
    assert cora_agent.build_live_services(_settings(lending_cora_approver_user_ids="")) is None


def test_only_email_is_registered_when_the_ghl_sender_is_configured(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(cora_agent, "get_email_sender",
                        lambda: lambda *args, **kwargs: calls.append((args, kwargs)) or "m1")
    executors = cora_agent.build_egress_executors()
    assert set(executors) == {"ghl_email"}
    assert executors["ghl_email"]({"lead_ref": "web_lead:7", "to_email": "sam@example.com",
                                   "subject": "Hello", "body": "Hi Sam"}) == "m1"
    assert calls == [(("sam@example.com", "Hello", "Hi Sam"), {"phone": None})]


def test_nothing_is_registered_without_a_configured_ghl_sender(monkeypatch) -> None:
    monkeypatch.setattr(cora_agent, "get_email_sender", lambda: None)
    assert cora_agent.build_egress_executors() == {}


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
    check = cora_agent.make_send_check(engine, carrier_approved=True, text_window=lambda phone: True)
    assert check(_action()) is None
    assert engine.queries[0]["p"] == "+17275550100"


def test_text_waits_for_carrier_approval() -> None:
    reason = cora_agent.make_send_check(ScriptedEngine(False, True), text_window=lambda phone: True)(_action())
    assert reason == "texting is not approved by the carrier yet (10DLC)" and isinstance(reason, SendDeferred)


def test_approved_consented_text_outside_the_borrower_window_waits() -> None:
    check = cora_agent.make_send_check(ScriptedEngine(False, True), carrier_approved=True,
                                       text_window=lambda phone: False)
    reason = check(_action())
    assert reason == "outside the borrower's 8am-8pm text window" and isinstance(reason, SendDeferred)


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


# ── the inbox digest in the sweep ────────────────────────────────────────────

class _SweepParts:
    """Stands in for the session, gate and relay: nothing to expire or retry, records whether sends ran."""

    def __init__(self, halted: bool = False):
        self.halted, self.relay_runs = halted, 0

    def expire_stale_actions(self) -> int:
        return 0

    def retry_missing_cards(self) -> int:
        return 0

    def run(self):
        from packages.agent_core.halt import AgentHalted
        from packages.agent_core.relay import RelayResult

        self.relay_runs += 1
        if self.halted:
            raise AgentHalted("halted")
        return RelayResult((), (), (), ())


def _sweep_services(parts: _SweepParts, digest) -> cora_agent.CoraServices:
    return cora_agent.CoraServices(session=parts, relay=parts, gate=parts, inbox_digest=digest)


def test_the_sweep_posts_the_inbox_digest_even_while_halted() -> None:
    digests: list[int] = []
    parts = _SweepParts(halted=True)
    cora_agent.run_sweep(_sweep_services(parts, lambda: digests.append(1) or 1))
    assert digests == [1]


def test_a_failing_digest_never_stops_the_sends() -> None:
    def database_down() -> int:
        raise RuntimeError("database unavailable")

    parts = _SweepParts()
    cora_agent.run_sweep(_sweep_services(parts, database_down))
    assert parts.relay_runs == 1


def test_inbox_channel_ids_skip_unset_channels() -> None:
    settings = SimpleNamespace(lending_replies_channel="C_REPLIES", lending_ops_channel="",
                               lending_dial_tasks_channel="C_DIAL", lending_daily_channel="")
    assert cora_agent.lending_channel_ids(settings) == {"replies": "C_REPLIES", "dial_tasks": "C_DIAL"}
