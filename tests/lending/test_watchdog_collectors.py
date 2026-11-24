import subprocess

import pytest
import requests

import src.lending.watchdog.collectors as collectors
from config.lending_watchdog import DOORS
from src.lending.watchdog.collectors import door_observations, health_observation, unit_observations


def _completed(stdout: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=3, stdout=stdout, stderr="")


class _Resp:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def test_units_parse_each_line():
    obs = unit_observations(["a", "b", "c"], run=lambda cmd: _completed("active\nfailed\nactivating\n"))
    assert [o.ok for o in obs] == [True, False, False]
    assert [o.key for o in obs] == ["unit:a", "unit:b", "unit:c"]


def test_units_make_one_systemctl_call():
    seen = []
    unit_observations(["a", "b"], run=lambda cmd: seen.append(cmd) or _completed("active\nactive\n"))
    assert seen == [["systemctl", "is-active", "a", "b"]]


def test_units_systemctl_error_marks_all_down():
    def boom(cmd):
        raise FileNotFoundError("nope")

    obs = unit_observations(["a", "b"], run=boom)
    assert [o.ok for o in obs] == [False, False]
    assert all(o.detail == "systemctl unavailable: FileNotFoundError" for o in obs)


def test_units_missing_output_line_is_down():
    obs = unit_observations(["a", "b"], run=lambda cmd: _completed("active\n"))
    assert [o.ok for o in obs] == [True, False]


def test_health_ok():
    obs = health_observation("http://x/health", get=lambda *a, **k: _Resp(200, {"status": "ok"}))
    assert obs.key == "health:lending-api" and obs.ok


def test_health_non_200_is_down():
    obs = health_observation("http://x/health", get=lambda *a, **k: _Resp(502))
    assert not obs.ok and obs.detail == "HTTP 502"


def test_health_wrong_body_is_down():
    assert not health_observation("http://x/health", get=lambda *a, **k: _Resp(200, {"status": "degraded"})).ok
    assert not health_observation("http://x/health", get=lambda *a, **k: _Resp(200)).ok


def test_health_timeout_is_down():
    def boom(*a, **k):
        raise requests.exceptions.Timeout()

    obs = health_observation("http://x/health", get=boom)
    assert not obs.ok and obs.detail == "Timeout"


def _doors_with(status_by_name):
    def request(method, url, **kwargs):
        name = next(d.name for d in DOORS if url.endswith(d.path))
        return _Resp(status_by_name[name])

    return request


def test_door_refused_is_healthy():
    healthy = {d.name: next(iter(d.healthy)) for d in DOORS}
    obs = door_observations("https://pub", request=_doors_with(healthy))
    assert all(o.ok for o in obs)
    assert {o.key for o in obs} == {f"door:{d.name}" for d in DOORS}
    assert next(d for d in DOORS if d.name == "website").healthy == frozenset({405})


@pytest.mark.parametrize("code", [404, 502, 503])
def test_door_404_502_503_are_broken(code):
    obs = door_observations("https://pub", request=_doors_with({d.name: code for d in DOORS}))
    assert all(not o.ok and o.detail == f"HTTP {code}" for o in obs)


def test_door_timeout_is_broken():
    def boom(*a, **k):
        raise requests.exceptions.ConnectTimeout()

    obs = door_observations("https://pub", request=boom)
    assert all(not o.ok and o.detail == "ConnectTimeout" for o in obs)


def test_door_sends_no_secret_header():
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return _Resp(401)

    door_observations("https://pub/", request=request)
    assert len(calls) == len(DOORS)
    for method, url, kwargs in calls:
        headers = {k.lower() for k in (kwargs.get("headers") or {})}
        assert "x-webhook-secret" not in headers and "x-instantly-webhook-secret" not in headers
        assert kwargs["timeout"] == 10 and kwargs["allow_redirects"] is False
        assert "//" not in url.split("://", 1)[1]


def test_doors_off_when_base_url_unset(monkeypatch):
    monkeypatch.setattr(collectors, "_warned_doors_off", False)
    assert door_observations("", request=lambda *a, **k: pytest.fail("no call")) == []
    assert door_observations("") == []


# ── database collectors ──────────────────────────────────────────────────────

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from src.lending.watchdog.collectors import (
    DB_CHECKS,
    card_observation,
    db_observations,
    failure_observation,
    ghl_outcome_observation,
    reply_observation,
)
from src.lending.watchdog.rules import Observation, WatchdogConfig

ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 11, 24, 15, 0, tzinfo=ET)  # Tue
MONDAY_930 = datetime(2026, 11, 23, 9, 30, tzinfo=ET)
SATURDAY = datetime(2026, 11, 28, 12, 0, tzinfo=ET)


def _cfg(**overrides) -> WatchdogConfig:
    base = dict(
        units=(), health_url="", public_base_url="", fail_polls=2, repeat_minutes=60,
        ghl_window_minutes=240, ghl_min_calls=5, card_age_minutes=120, reply_backlog=3, open_failures=5,
    )
    return WatchdogConfig(**{**base, **overrides})


def _ago(minutes: float, now: datetime = NOW) -> datetime:
    return now - timedelta(minutes=minutes)


# Pure threshold logic (no database).

def test_ghl_decision_alerts_on_calls_without_outcomes():
    calls = [_ago(30 + i) for i in range(5)]
    obs = collectors._ghl_decision(calls, [], NOW, _cfg())
    assert obs == Observation("ghl_outcomes", False, "5 GHL calls, 0 outcomes in 240 business min")


def test_ghl_decision_one_outcome_is_ok():
    assert collectors._ghl_decision([_ago(30)] * 5, [_ago(10)], NOW, _cfg()).ok


def test_ghl_decision_below_min_calls_is_quiet():
    obs = collectors._ghl_decision([_ago(30)] * 4, [], NOW, _cfg())
    assert obs == Observation("ghl_outcomes", True, "quiet: 4 calls")


def test_ghl_decision_ignores_calls_outside_the_window():
    assert collectors._ghl_decision([_ago(241 + 24 * 60)] * 5, [], NOW, _cfg()).ok


def test_ghl_decision_window_counts_business_minutes_over_a_weekend():
    friday = datetime(2026, 11, 20, 17, 0, tzinfo=ET)
    calls = [friday + timedelta(minutes=12 * i) for i in range(5)]
    assert not collectors._ghl_decision(calls, [], MONDAY_930, _cfg()).ok


def test_ghl_decision_ignores_outcomes_outside_the_window():
    assert not collectors._ghl_decision([_ago(30)] * 5, [_ago(241 + 24 * 60)], NOW, _cfg()).ok


def test_card_decision_threshold():
    assert not collectors._card_decision(_ago(121), 2, NOW, _cfg()).ok
    assert collectors._card_decision(_ago(119), 2, NOW, _cfg()).ok
    assert collectors._card_decision(None, 0, NOW, _cfg()) == Observation("cards", True, "0 waiting")
    assert collectors._card_decision(_ago(121), 2, NOW, _cfg()).detail == "2 waiting, oldest 121 business min"


def test_reply_decision_counts_rows_past_the_sla_only():
    posted = [_ago(90), _ago(200), _ago(300), _ago(30)]
    assert not collectors._reply_decision(posted, NOW, _cfg()).ok
    assert collectors._reply_decision(posted, NOW, _cfg(reply_backlog=4)).ok
    assert collectors._reply_decision(posted, NOW, _cfg(reply_backlog=3)).detail == "3 handoffs past SLA"


def test_failure_decision_open_and_gave_up():
    assert collectors._failure_decision(5, 0, _cfg()).ok is False
    assert collectors._failure_decision(4, 0, _cfg()).ok is True
    assert collectors._failure_decision(0, 1, _cfg()).ok is False
    assert collectors._failure_decision(0, 0, _cfg()) == Observation("failures", True, "0 open, 0 gave up in 24 h")


# db_observations without a database.

class _FakeDb:
    def __init__(self):
        self.statements = []

    def execute(self, statement, params=None):
        self.statements.append(str(statement))


def _factory(db):
    @contextmanager
    def factory():
        yield db
    return factory


def test_db_checks_skipped_outside_business_hours(monkeypatch):
    monkeypatch.setattr(collectors, "DB_CHECKS", (lambda *a: pytest.fail("ran outside business hours"),))
    fake = _FakeDb()
    assert db_observations(_factory(fake), SATURDAY, _cfg()) == [Observation("database", True, "ok")]
    assert fake.statements == ["SELECT 1"]


def test_db_checks_run_in_business_hours_and_none_is_dropped(monkeypatch):
    keep = Observation("x", True, "d")
    monkeypatch.setattr(collectors, "DB_CHECKS", (lambda *a: keep, lambda *a: None))
    assert db_observations(_factory(_FakeDb()), NOW, _cfg()) == [Observation("database", True, "ok"), keep]


def test_db_error_yields_single_database_observation():
    def broken():
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    assert db_observations(broken, NOW, _cfg()) == [Observation("database", False, "OperationalError")]


def test_db_check_error_yields_single_database_observation(monkeypatch):
    def boom(*a):
        raise ValueError("bad row")

    monkeypatch.setattr(collectors, "DB_CHECKS", (boom,))
    assert db_observations(_factory(_FakeDb()), NOW, _cfg()) == [Observation("database", False, "ValueError")]


def test_db_checks_registry_lists_the_four_checks():
    assert DB_CHECKS == (ghl_outcome_observation, card_observation, reply_observation, failure_observation)


# Queries against Postgres (skipped without TEST_DATABASE_URL).

@pytest.fixture
def db(lending_db):
    from migrations.apply_lending_booking_messages import apply_to as apply_handoffs
    from migrations.apply_lending_call_gate_failed import apply_to as apply_gate_failed
    from migrations.apply_lending_ghl_disposition_events import apply_to as apply_events
    from migrations.apply_lending_webhook_failures import apply_to as apply_failures
    from packages.agent_core.schema import base_statements

    conn = lending_db.connection()
    apply_gate_failed(conn)
    apply_events(conn)
    apply_handoffs(conn)
    apply_failures(conn)
    for statement in base_statements("lending"):
        conn.execute(text(statement))
    return lending_db


def _ghl_call(db, started):
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, call_started_at, raw_event) "
        "VALUES (:id, 'ghl', 'outbound', '+18137200000', :s, '{}')"),
        {"id": f"ghl:t-{uuid.uuid4().hex}", "s": started})


def _ghl_outcome(db, received):
    db.execute(text(
        "INSERT INTO lending.ghl_disposition_events (received_at, disposition_raw, code) VALUES (:r, 'x', 'no_answer')"),
        {"r": received})


def test_ghl_calls_without_outcomes_alerts(db):
    for i in range(5):
        _ghl_call(db, _ago(10 + i * 20))
    assert ghl_outcome_observation(db, NOW, _cfg()).ok is False


def test_ghl_calls_with_one_outcome_ok(db):
    for i in range(5):
        _ghl_call(db, _ago(10 + i * 20))
    _ghl_outcome(db, _ago(5))
    assert ghl_outcome_observation(db, NOW, _cfg()).ok is True


def test_ghl_quiet_day_no_alert(db):
    assert ghl_outcome_observation(db, NOW, _cfg()).ok is True


def test_ghl_below_min_calls_no_alert(db):
    for i in range(4):
        _ghl_call(db, _ago(10 + i * 20))
    assert ghl_outcome_observation(db, NOW, _cfg()).ok is True


def test_ghl_window_counts_business_minutes(db):
    friday = datetime(2026, 11, 20, 17, 0, tzinfo=ET)
    for i in range(5):
        _ghl_call(db, friday + timedelta(minutes=12 * i))
    assert ghl_outcome_observation(db, MONDAY_930, _cfg()).ok is False


def test_ghl_ignores_inbound_and_dialer_calls(db):
    for source, direction in (("ghl", "inbound"), ("batchdialer", "outbound")):
        for i in range(5):
            db.execute(text(
                "INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, call_started_at, "
                "raw_event) VALUES (:id, :src, :dir, '+18137200000', :s, '{}')"),
                {"id": f"t-{uuid.uuid4().hex}", "src": source, "dir": direction, "s": _ago(30)})
    assert ghl_outcome_observation(db, NOW, _cfg()).detail == "quiet: 0 calls"


def test_ghl_check_off_when_min_calls_zero(db):
    assert ghl_outcome_observation(db, NOW, _cfg(ghl_min_calls=0)) is None


def _card(db, created, status="pending"):
    db.execute(text(
        "INSERT INTO lending.pending_actions (tool_name, channel, payload, status, created_at) "
        "VALUES ('t', 'ghl_sms', '{}', :st, :c)"), {"st": status, "c": created})


def test_card_age_threshold(db):
    _card(db, _ago(121))
    assert card_observation(db, NOW, _cfg()).ok is False
    db.execute(text("DELETE FROM lending.pending_actions"))
    _card(db, _ago(119))
    _card(db, _ago(500), status="approved")
    assert card_observation(db, NOW, _cfg()).ok is True


def _handoff(db, posted, alerted=None, responded=None, kind="rate_terms_handoff"):
    db.execute(text(
        "INSERT INTO lending.reply_handoffs (message_id, kind, contact_id, phone_hash, posted_at, overdue_alerted_at, "
        "responded_at) VALUES (:m, :k, 'c', 'abc', :p, :a, :r)"),
        {"m": uuid.uuid4().hex, "k": kind, "p": posted, "a": alerted, "r": responded})


def test_reply_backlog_counts_already_alerted_rows(db):
    _handoff(db, _ago(200))
    _handoff(db, _ago(210))
    _handoff(db, _ago(220), alerted=_ago(100))
    _handoff(db, _ago(230), responded=_ago(100))
    assert reply_observation(db, NOW, _cfg()).ok is False
    assert reply_observation(db, NOW, _cfg(reply_backlog=4)).ok is True


def _failure(db, *, gave_up=None, resolved=None):
    db.execute(text(
        "INSERT INTO lending.webhook_failures (endpoint, payload, gave_up_at, resolved_at) "
        "VALUES ('ghl_opt_out', '{}', :g, :r)"), {"g": gave_up, "r": resolved})


def test_failures_open_and_gave_up(db):
    for _ in range(5):
        _failure(db)
    assert failure_observation(db, NOW, _cfg()).ok is False
    db.execute(text("DELETE FROM lending.webhook_failures"))
    _failure(db, gave_up=_ago(120))
    assert failure_observation(db, NOW, _cfg()).ok is False
    db.execute(text("DELETE FROM lending.webhook_failures"))
    _failure(db, gave_up=_ago(30 * 60))
    assert failure_observation(db, NOW, _cfg()).ok is True
