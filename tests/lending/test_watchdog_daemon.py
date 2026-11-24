import json
import logging
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from config.settings import get_settings
from src.lending.watchdog import daemon
from src.lending.watchdog.rules import Observation

NOW = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
JOSH = "+18132345678"


@pytest.fixture(autouse=True)
def reset_daemon_state():
    daemon._memory_state = None
    daemon._stop_event.clear()
    yield
    daemon._memory_state = None
    daemon._stop_event.clear()


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
    monkeypatch.setattr(daemon, "door_observations", lambda base, doors: [])
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


def test_session_factory_sets_connect_and_statement_timeouts(monkeypatch):
    monkeypatch.setattr(daemon, "_session_factory", None)
    engine = MagicMock()
    create = MagicMock(return_value=engine)
    monkeypatch.setattr(daemon, "create_engine", create)
    daemon._factory()
    assert create.call_args.kwargs["connect_args"] == {"connect_timeout": 5, "options": "-c statement_timeout=5000"}
    monkeypatch.setattr(daemon, "_session_factory", None)


def test_timed_session_commits_and_rolls_back(monkeypatch):
    session = MagicMock()
    monkeypatch.setattr(daemon, "_factory", lambda: lambda: session)
    with daemon._timed_session() as db:
        assert db is session
    session.commit.assert_called_once()
    session.close.assert_called_once()
    session = MagicMock()
    monkeypatch.setattr(daemon, "_factory", lambda: lambda: session)
    with pytest.raises(ValueError):
        with daemon._timed_session():
            raise ValueError("x")
    session.rollback.assert_called_once()
    session.commit.assert_not_called()


def test_cycle_exception_does_not_stop_loop(env, fakes, monkeypatch):
    calls = []

    def run_cycle(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        daemon._stop_event.set()
        return []

    monkeypatch.setattr(daemon, "run_cycle", run_cycle)
    monkeypatch.setattr(daemon.signal, "signal", lambda *a: None)
    monkeypatch.setattr(daemon, "_wait", lambda seconds: None)
    assert daemon.main([]) == 0
    assert len(calls) == 2


def test_signal_wakes_the_wait(monkeypatch):
    daemon._stop_event.clear()
    daemon._on_signal(15, None)
    assert daemon._stop_event.is_set()
    start = time.monotonic()
    daemon._wait(30)
    assert time.monotonic() - start < 1
    daemon._stop_event.clear()


def test_main_configures_logging_when_no_handlers(env, fakes, monkeypatch):
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    configured = MagicMock()
    monkeypatch.setattr(daemon.logging, "basicConfig", configured)
    monkeypatch.setattr(daemon.signal, "signal", lambda *a: None)
    monkeypatch.setattr(daemon, "run_cycle", lambda **kw: [])
    daemon.main(["--once"])
    configured.assert_called_once()
    assert configured.call_args.kwargs["level"] == logging.INFO


def test_dry_run_works_while_disabled(env, fakes, monkeypatch):
    monkeypatch.setenv("LENDING_WATCHDOG_ENABLED", "false")
    get_settings.cache_clear()
    assert len(daemon.run_cycle(now=NOW, dry_run=True)) == 1
    fakes[0].assert_not_called()


def test_undelivered_down_alert_is_retried_next_poll(env, fakes):
    ghl, slack = fakes
    ghl.side_effect = RuntimeError("ghl down")
    slack.chat_postMessage.side_effect = RuntimeError("slack down")
    assert len(daemon.run_cycle(now=NOW)) == 1
    saved = json.loads(env.read_text())["unit:fa-lending-cora"]
    assert saved["open_since"] is None and saved["last_alert_at"] is None
    ghl.side_effect = None
    again = daemon.run_cycle(now=NOW.replace(minute=2))
    assert [a.kind for a in again] == ["down"]
    assert json.loads(env.read_text())["unit:fa-lending-cora"]["open_since"] is not None


def test_failed_state_save_does_not_resend_down_every_cycle(env, fakes, monkeypatch):
    ghl, _ = fakes
    real_save = daemon.save_state
    monkeypatch.setattr(daemon, "save_state", lambda path, state: (_ for _ in ()).throw(OSError("ro")))
    assert len(daemon.run_cycle(now=NOW)) == 1
    assert daemon.run_cycle(now=NOW.replace(minute=2)) == []
    assert ghl.call_count == 1
    monkeypatch.setattr(daemon, "save_state", real_save)
    daemon.run_cycle(now=NOW.replace(minute=4))
    assert daemon._memory_state is None
    assert "unit:fa-lending-cora" in json.loads(env.read_text())


@pytest.mark.parametrize("route", ["_ghl_route", "_slack"])
def test_route_construction_failure_does_not_lose_other_routes(env, fakes, monkeypatch, route):
    ghl, slack = fakes

    def boom(*args):
        raise RuntimeError("bad config")

    monkeypatch.setattr(daemon, route, boom)
    assert len(daemon.run_cycle(now=NOW)) == 1
    survivor = slack.chat_postMessage if route == "_ghl_route" else ghl
    survivor.assert_called_once()


def test_failing_health_drops_door_checks_for_the_cycle(env, fakes, monkeypatch):
    monkeypatch.setattr(daemon, "health_observation", lambda url: Observation("health:lending-api", False, "HTTP 500"))
    monkeypatch.setattr(daemon, "door_observations", lambda base, doors: pytest.fail("doors checked while health is down"))
    monkeypatch.setenv("LENDING_WATCHDOG_PUBLIC_BASE_URL", "https://x.example")
    get_settings.cache_clear()
    keys = {a.key for a in daemon.run_cycle(now=NOW)}
    assert "health:lending-api" in keys


def test_selected_doors_default_and_unknown_warning(caplog):
    daemon._warned_doors.clear()
    settings = MagicMock(lending_watchdog_doors="ghl-stage, website ,nope")
    with caplog.at_level(logging.WARNING):
        doors = daemon.selected_doors(settings)
        daemon.selected_doors(settings)
    assert [d.name for d in doors] == ["ghl-stage", "website"]
    assert caplog.text.count("unknown door names") == 1 and "nope" in caplog.text
    full = MagicMock(lending_watchdog_doors="ghl-stage,ghl-disposition,lendingflow,instantly,website")
    assert len(daemon.selected_doors(full)) == 5
    assert daemon.selected_doors(MagicMock(lending_watchdog_doors="")) == ()


def test_cycle_passes_only_selected_doors(env, fakes, monkeypatch):
    seen = []
    monkeypatch.setattr(daemon, "door_observations", lambda base, doors: seen.append([d.name for d in doors]) or [])
    monkeypatch.setenv("LENDING_WATCHDOG_DOORS", "website")
    get_settings.cache_clear()
    daemon.run_cycle(now=NOW)
    assert seen == [["website"]]


def _configured(**overrides):
    values = dict(
        lending_alarm_sms_to="(813) 234-5678", lending_watchdog_backup_sms_to=None,
        lending_slack_bot_token="xoxb", lending_ops_channel="C1", lending_watchdog_public_base_url="https://x.example",
        lending_watchdog_telnyx_from_number="+18130001111", lending_watchdog_telnyx_messaging_profile_id="p",
        telnyx_messaging_profile_id=None, telnyx_sms_api_key="k",
    )
    return MagicMock(**{**values, **overrides})


def test_fully_configured_setup_logs_no_error(monkeypatch, caplog):
    monkeypatch.setattr(daemon, "_ghl_route", lambda: (lambda to, body: "id"))
    with caplog.at_level(logging.ERROR):
        daemon.log_setup_problems(_configured())
    assert caplog.text == ""


@pytest.mark.parametrize("override,name", [
    ({"lending_alarm_sms_to": None}, "LENDING_ALARM_SMS_TO"),
    ({"lending_watchdog_telnyx_from_number": None}, "LENDING_WATCHDOG_TELNYX_FROM_NUMBER"),
    ({"lending_slack_bot_token": None}, "LENDING_SLACK_BOT_TOKEN"),
    ({"lending_ops_channel": ""}, "LENDING_OPS_CHANNEL"),
    ({"lending_watchdog_public_base_url": ""}, "LENDING_WATCHDOG_PUBLIC_BASE_URL"),
])
def test_each_missing_setup_piece_is_named(monkeypatch, caplog, override, name):
    monkeypatch.setattr(daemon, "_ghl_route", lambda: (lambda to, body: "id"))
    with caplog.at_level(logging.ERROR):
        daemon.log_setup_problems(_configured(**override))
    assert caplog.text.count("setup incomplete") == 1 and name in caplog.text


def test_missing_ghl_route_is_named(monkeypatch, caplog):
    monkeypatch.setattr(daemon, "_ghl_route", lambda: None)
    with caplog.at_level(logging.ERROR):
        daemon.log_setup_problems(_configured())
    assert "GHL SMS route" in caplog.text


def test_main_logs_setup_problems_at_startup(env, fakes, monkeypatch):
    logged = MagicMock()
    monkeypatch.setattr(daemon, "log_setup_problems", logged)
    monkeypatch.setattr(daemon.signal, "signal", lambda *a: None)
    monkeypatch.setattr(daemon, "run_cycle", lambda **kw: [])
    daemon.main(["--dry-run"])
    logged.assert_called_once()


def test_numeric_settings_are_clamped():
    settings = MagicMock(
        lending_watchdog_units="", lending_watchdog_health_url="", lending_watchdog_public_base_url="",
        lending_watchdog_fail_polls=0, lending_watchdog_repeat_minutes=1, lending_watchdog_ghl_window_minutes=0,
        lending_watchdog_ghl_min_calls=0, lending_watchdog_card_age_minutes=-5, lending_watchdog_reply_backlog=0,
        lending_watchdog_open_failures=0, lending_watchdog_poll_seconds=1,
    )
    cfg = daemon.load_config(settings)
    assert (cfg.fail_polls, cfg.repeat_minutes, cfg.ghl_window_minutes, cfg.card_age_minutes) == (1, 10, 1, 1)
    assert (cfg.reply_backlog, cfg.open_failures, cfg.ghl_min_calls) == (1, 1, 0)
    assert daemon.poll_seconds(settings) == 30


def _seed_state(path, **entry):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"unit:fa-lending-cora": entry}))


def test_undelivered_still_down_keeps_previous_alert_time_and_retries(env, fakes):
    ghl, slack = fakes
    ghl.side_effect = RuntimeError("down")
    slack.chat_postMessage.side_effect = RuntimeError("down")
    old = "2026-11-24T12:00:00+00:00"
    _seed_state(env, failures=5, open_since=old, last_alert_at=old)
    assert [a.kind for a in daemon.run_cycle(now=NOW)] == ["still_down"]
    assert json.loads(env.read_text())["unit:fa-lending-cora"]["last_alert_at"] == old
    ghl.side_effect = None
    assert [a.kind for a in daemon.run_cycle(now=NOW.replace(minute=2))] == ["still_down"]
    assert json.loads(env.read_text())["unit:fa-lending-cora"]["last_alert_at"] != old


def test_undelivered_recovery_keeps_open_alert_and_retries(env, fakes, monkeypatch):
    ghl, slack = fakes
    monkeypatch.setattr(daemon, "unit_observations", lambda units: [Observation("unit:fa-lending-cora", True, "active")])
    ghl.side_effect = RuntimeError("down")
    slack.chat_postMessage.side_effect = RuntimeError("down")
    old = "2026-11-24T12:00:00+00:00"
    _seed_state(env, failures=5, open_since=old, last_alert_at=old)
    assert [a.kind for a in daemon.run_cycle(now=NOW)] == ["recovered"]
    assert json.loads(env.read_text())["unit:fa-lending-cora"]["open_since"] == old
    ghl.side_effect = None
    assert [a.kind for a in daemon.run_cycle(now=NOW.replace(minute=2))] == ["recovered"]
    assert json.loads(env.read_text())["unit:fa-lending-cora"]["open_since"] is None
