"""T-03 "@Cora emergency halt": the command, the LENDING_GLOBAL_HALT flag, and every lending send path it stops.
SQLite with an attached ``lending`` schema, in-memory Slack and recorded GHL calls: nothing real is sent."""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from packages.agent_core.chatport import FakeChatPort, PostResult
from packages.agent_core.slack_session import InboundMessage
from src.lending import global_halt, lendingflow, lendingflow_ghl
from src.lending.dialer_load import LoadRefused, run_dialer_load
from src.lending.ghl_account import GhlAccount
from src.lending.ghl_sms import GhlSmsError, GhlSmsSender
from src.lending.global_halt import EmergencyHalt, emergency_command, is_globally_halted, set_global_halt

JOSH, OPS = "U_JOSH", "U_OPS"


@pytest.fixture
def halt_db(monkeypatch):
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE lending.global_halt (id INTEGER PRIMARY KEY, halted BOOLEAN, reason TEXT,"
                          " set_by TEXT, set_at TIMESTAMP)"))
        conn.execute(text("CREATE TABLE lending.global_halt_events (id INTEGER PRIMARY KEY, halted BOOLEAN,"
                          " reason TEXT, set_by TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"))

    @contextmanager
    def session():
        with Session(engine) as db, db.begin():
            yield db

    monkeypatch.setattr(global_halt, "_session_factory", session)
    yield engine
    engine.dispose()


class CoraHalt:
    def __init__(self) -> None:
        self.halted_by: str | None = None

    def set(self, *, reason: str, user_id: str) -> None:
        self.halted_by = user_id


def _message(text_value: str, user: str = JOSH) -> InboundMessage:
    return InboundMessage(channel="C_CORA", user_id=user, text=text_value, ts="1.0", thread_ts=None, is_direct=False)


def _command(chat: FakeChatPort, cora_halt: CoraHalt, alert_channel: str | None = "C_NDL") -> EmergencyHalt:
    return EmergencyHalt(chat=chat, cora_halt=cora_halt, approver_ids={JOSH}, alert_channel=alert_channel)


@pytest.mark.parametrize("message,command", [
    ("<@U_BOT> emergency halt", "halt"), ("@Cora emergency halt", "halt"), ("emergency halt now!", "halt"),
    ("Cora, emergency stop please", "halt"), ("<@U_BOT> emergency resume", "resume"),
    ("what does emergency halt do?", None), ("halt", None), ("emergency", None), ("emergency halt texting Sam", None),
])
def test_only_the_exact_command_counts(message, command) -> None:
    assert emergency_command(message) == command


def test_flag_set_and_cleared_with_history(halt_db) -> None:
    assert not is_globally_halted()
    set_global_halt(True, reason="test", set_by=JOSH)
    assert is_globally_halted()
    set_global_halt(False, reason="test", set_by=JOSH)
    assert not is_globally_halted()
    with halt_db.connect() as conn:
        assert [tuple(r) for r in conn.execute(text(
            "SELECT halted, set_by FROM lending.global_halt_events ORDER BY id"))] == [(1, JOSH), (0, JOSH)]


def test_unreadable_flag_counts_as_halted(monkeypatch) -> None:
    def broken():
        raise RuntimeError("db down")

    monkeypatch.setattr(global_halt, "_session_factory", broken)
    assert is_globally_halted()


def test_approver_halt_sets_the_flag_halts_cora_and_alerts_the_channel(halt_db) -> None:
    chat, cora_halt = FakeChatPort(default_channel="C_CORA"), CoraHalt()
    assert _command(chat, cora_halt)(_message("<@U_BOT> emergency halt"))
    assert is_globally_halted() and cora_halt.halted_by == JOSH
    alert = next(post for post in chat.posts if post["channel"] == "C_NDL")
    assert "Lending emergency halt" in alert["text"]
    assert ":white_square: Pause the lending campaigns in BatchDialer" in alert["text"]
    assert ":white_square: Turn off GoHighLevel workflows" in alert["text"]
    assert any(post["channel"] == "C_CORA" for post in chat.posts)


def test_operator_cannot_halt(halt_db) -> None:
    chat, cora_halt = FakeChatPort(default_channel="C_CORA"), CoraHalt()
    assert _command(chat, cora_halt)(_message("emergency halt", user=OPS))
    assert not is_globally_halted() and cora_halt.halted_by is None
    assert "Only an approver" in chat.posts[-1]["text"]


def test_resume_clears_the_flag_but_not_coras_own_halt(halt_db) -> None:
    chat, cora_halt = FakeChatPort(default_channel="C_CORA"), CoraHalt()
    command = _command(chat, cora_halt)
    command(_message("emergency halt"))
    command(_message("emergency resume"))
    assert not is_globally_halted() and cora_halt.halted_by == JOSH
    assert "Cora stays halted" in chat.posts[-1]["text"]


def test_a_failed_alert_post_does_not_undo_the_halt(halt_db) -> None:
    class AlertFails(FakeChatPort):
        def post(self, *, text, blocks=None, channel=None, thread_ts=None):
            if channel == "C_NDL":
                return PostResult(ok=False, error="not_in_channel")
            return super().post(text=text, blocks=blocks, channel=channel, thread_ts=thread_ts)

    chat = AlertFails(default_channel="C_CORA")
    _command(chat, CoraHalt())(_message("emergency halt"))
    assert is_globally_halted()
    assert "could not be posted" in chat.posts[-1]["text"]


def test_if_the_flag_cannot_be_saved_cora_still_halts(monkeypatch) -> None:
    def broken():
        raise RuntimeError("db down")

    monkeypatch.setattr(global_halt, "_session_factory", broken)
    chat, cora_halt = FakeChatPort(default_channel="C_CORA"), CoraHalt()
    _command(chat, cora_halt)(_message("emergency halt"))
    assert cora_halt.halted_by == JOSH and "could NOT be saved" in chat.posts[-1]["text"]


class Resp:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code, self._body = status, body

    def json(self) -> dict:
        return self._body


def test_ghl_text_and_email_are_refused_while_halted(halt_db) -> None:
    set_global_halt(True, reason="test", set_by=JOSH)
    calls = []

    def request(method, url, **kwargs):
        calls.append(url)
        return Resp(200, {"contact": {"id": "ct1"}})

    with pytest.raises(GhlSmsError, match="emergency halt"):
        GhlSmsSender(GhlAccount(api_key="k", location_id="loc"), "+18135550000", request)("+17275550100", "Hi")
    assert not any("/conversations/messages" in url for url in calls)


def test_intro_sms_tag_is_not_added_while_halted(halt_db, monkeypatch) -> None:
    monkeypatch.setattr(lendingflow_ghl, "get_settings",
                        lambda: SimpleNamespace(lending_lendingflow_intro_sms_enabled=True))
    lead = {"consent_status": "present", "sms_clear": True}
    assert lendingflow_ghl._intro_ready_tags(lead) == [lendingflow_ghl.GHL_TAG_INTRO_READY]
    set_global_halt(True, reason="test", set_by=JOSH)
    assert lendingflow_ghl._intro_ready_tags(lead) == []


def test_held_intro_texts_are_not_released_while_halted(halt_db, monkeypatch) -> None:
    monkeypatch.setattr(lendingflow, "_intro_sms_on", lambda: True)
    set_global_halt(True, reason="test", set_by=JOSH)
    assert lendingflow.release_held_intro_sms(db=None, sink=object()) == 0


def test_no_dialer_load_while_halted(halt_db) -> None:
    set_global_halt(True, reason="test", set_by=JOSH)
    with pytest.raises(LoadRefused, match="emergency halt"):
        run_dialer_load([], None, run_id="r1", dry_run=False, scrubber=object(), dialer=object(), backflip_check=False)


def test_typed_in_coras_channel_the_alert_is_posted_once(halt_db) -> None:
    """Client answer 12: the alert goes to Cora's channel; a command typed there is not posted twice."""
    chat, cora_halt = FakeChatPort(default_channel="C_CORA"), CoraHalt()
    _command(chat, cora_halt, alert_channel="C_CORA")(_message("<@U_BOT> emergency halt"))
    assert is_globally_halted()
    alerts = [post for post in chat.posts if "Lending emergency halt" in post["text"]]
    assert len(alerts) == 1 and alerts[0]["channel"] == "C_CORA"
    assert ":white_square: Pause the lending campaigns in BatchDialer" in alerts[0]["text"]
