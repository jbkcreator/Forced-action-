import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from config.settings import get_settings
from src.lending.watchdog import daemon
from src.lending.watchdog.rules import Observation

NOW = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
JOSH = "+18132345678"


@pytest.fixture
def env(monkeypatch, tmp_path):
    for name, value in {
        "LENDING_WATCHDOG_ENABLED": "true",
        "LENDING_WATCHDOG_FAIL_POLLS": "1",
        "LENDING_WATCHDOG_STATE_PATH": str(tmp_path / "sub" / "state.json"),
        "LENDING_WATCHDOG_UNITS": "fa-lending-cora",
        "LENDING_ALARM_SMS_TO": "(813) 234-5678",
        "LENDING_OPS_CHANNEL": "C123",
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("LENDING_WATCHDOG_BACKUP_SMS_TO", "LENDING_WATCHDOG_PUBLIC_BASE_URL"):
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    yield tmp_path / "sub" / "state.json"
    get_settings.cache_clear()


@pytest.fixture
def fakes(monkeypatch):
    ghl, slack = MagicMock(return_value="m1"), MagicMock()
    monkeypatch.setattr(daemon, "unit_observations", lambda units: [Observation("unit:fa-lending-cora", False, "inactive")])
    monkeypatch.setattr(daemon, "health_observation", lambda url: Observation("health:lending-api", True, "ok"))
    monkeypatch.setattr(daemon, "door_observations", lambda base: [])
    monkeypatch.setattr(daemon, "db_observations", lambda factory, now, cfg: [Observation("database", True, "ok")])
    monkeypatch.setattr(daemon, "_ghl_route", lambda: ghl)
    monkeypatch.setattr(daemon, "_slack", lambda settings: slack)
    return ghl, slack


def test_disabled_does_nothing(env, fakes, monkeypatch):
    monkeypatch.setenv("LENDING_WATCHDOG_ENABLED", "false")
    get_settings.cache_clear()
    assert daemon.run_cycle(now=NOW) == []
    assert not env.exists()
    fakes[0].assert_not_called()


def test_cycle_alerts_and_persists_state(env, fakes):
    ghl, slack = fakes
    alerts = daemon.run_cycle(now=NOW)
    assert [a.key for a in alerts] == ["unit:fa-lending-cora"]
    ghl.assert_called_once()
    assert ghl.call_args.args[0] == JOSH
    slack.chat_postMessage.assert_called_once()
    assert "unit:fa-lending-cora" in json.loads(env.read_text())


def test_dry_run_sends_nothing_and_saves_nothing(env, fakes):
    ghl, slack = fakes
    assert len(daemon.run_cycle(now=NOW, dry_run=True)) == 1
    ghl.assert_not_called()
    slack.chat_postMessage.assert_not_called()
    assert not env.exists()


def test_empty_unit_list_skips_systemctl(env, fakes, monkeypatch):
    monkeypatch.setenv("LENDING_WATCHDOG_UNITS", "")
    get_settings.cache_clear()
    monkeypatch.setattr(daemon, "unit_observations", lambda units: pytest.fail("systemctl called"))
    assert daemon.run_cycle(now=NOW) == []


def test_state_write_failure_does_not_crash_cycle(env, fakes, monkeypatch):
    def boom(path, state):
        raise OSError("disk full")

    monkeypatch.setattr(daemon, "save_state", boom)
    assert len(daemon.run_cycle(now=NOW)) == 1


def test_recipients_dedup_and_skip_empty():
    settings = MagicMock(lending_alarm_sms_to="(813) 234-5678", lending_watchdog_backup_sms_to="+1 813 234 5678")
    assert daemon.recipients(settings) == [JOSH]
    settings = MagicMock(lending_alarm_sms_to=None, lending_watchdog_backup_sms_to="813-987-6543")
    assert daemon.recipients(settings) == ["+18139876543"]
    settings = MagicMock(lending_alarm_sms_to=None, lending_watchdog_backup_sms_to="")
    assert daemon.recipients(settings) == []


def test_telnyx_route_ignores_telnyx_sms_enabled(monkeypatch):
    sent = MagicMock(return_value={"message_id": "t1"})
    monkeypatch.setattr(daemon, "send_message", sent)
    settings = MagicMock(
        telnyx_sms_enabled=False,
        telnyx_sms_api_key="key",
        lending_watchdog_telnyx_from_number="+15550001111",
        lending_watchdog_telnyx_messaging_profile_id=None,
        telnyx_messaging_profile_id="prof",
    )
    route = daemon.telnyx_route(settings)
    assert route is not None
    route(JOSH, "hi")
    sent.assert_called_once_with(JOSH, "hi", "transactional", from_number="+15550001111", messaging_profile_id="prof")
    settings.lending_watchdog_telnyx_from_number = None
    assert daemon.telnyx_route(settings) is None
    settings.lending_watchdog_telnyx_from_number = "+15550001111"
    settings.telnyx_messaging_profile_id = None
    assert daemon.telnyx_route(settings) is None


def test_session_sets_statement_timeout(monkeypatch):
    executed = []
    db = MagicMock()
    db.execute.side_effect = lambda stmt, *a: executed.append(str(stmt))
    inner = MagicMock()
    inner.__enter__.return_value = db
    monkeypatch.setattr(daemon, "lending_session", lambda: inner)
    with daemon._timed_session() as session:
        assert session is db
    assert "statement_timeout" in executed[0]


def test_cycle_exception_does_not_stop_loop(env, fakes, monkeypatch):
    calls = []

    def run_cycle(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        daemon._running = False
        return []

    monkeypatch.setattr(daemon, "run_cycle", run_cycle)
    monkeypatch.setattr(daemon.time, "sleep", lambda s: None)
    monkeypatch.setattr(daemon.signal, "signal", lambda *a: None)
    daemon._running = True
    assert daemon.main([]) == 0
    assert len(calls) == 2
