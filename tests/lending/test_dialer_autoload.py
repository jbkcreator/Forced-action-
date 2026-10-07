"""Daily automatic dialer load: mode, pause, guardrails, ET-hour gate. No database."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.lending.dialer_load import LoadAborted, LoadReport
from src.tasks import lending_dialer_autoload as autoload


def _settings(**over):
    base = dict(lending_dialer_autoload_mode="live", lending_dialer_autoload_max_records=1000,
                lending_dialer_autoload_max_growth=2.0, lending_dialer_autoload_max_scrub_credits=200,
                lending_dialer_autoload_max_consecutive_failures=10, lending_dialer_alert_channel="C-ALERT",
                lending_dial_tasks_channel="C-TASKS")
    return SimpleNamespace(**{**base, **over})


def _report(loadable=300, needs_scrub=0, dry_run=True, **extra):
    r = LoadReport(run_id="r", dry_run=dry_run, loadable=loadable, needs_scrub=needs_scrub)
    for k, v in extra.items():
        setattr(r, k, v)
    return r


class FakeSession:
    def rollback(self):
        pass

    def commit(self):
        pass


@contextmanager
def _factory():
    yield FakeSession()


class FakeDialer:
    def missing_for_load(self):
        return []


@pytest.fixture
def wired(monkeypatch):
    state = {"paused": False, "active": 0, "preview": _report(), "live_calls": [], "live_error": None}
    monkeypatch.setattr(autoload, "_paused", lambda s: state["paused"])
    monkeypatch.setattr(autoload, "_active_contacts", lambda s: state["active"])
    monkeypatch.setattr(autoload, "staged_pool_records", lambda s: [])
    monkeypatch.setattr(autoload, "launch_queue_records", lambda recs: [{"pool": "builders"}])

    def fake_load(records, session, *, run_id, dry_run, **kw):
        if dry_run:
            return state["preview"]
        state["live_calls"].append(kw)
        if state["live_error"]:
            raise state["live_error"]
        return _report(dry_run=False, loaded=5)

    monkeypatch.setattr(autoload, "run_dialer_load", fake_load)
    return state


def _run(settings, dialer=FakeDialer()):
    return autoload.run_autoload(settings=settings, session_factory=_factory, get_dialer=lambda: dialer,
                                 scrubber=lambda phones: [])


class TestGuardrails:
    def test_a_normal_run_trips_nothing(self):
        assert autoload.guardrail_trips(_report(), 300, _settings()) == []

    def test_too_many_records(self):
        assert "max 1000" in autoload.guardrail_trips(_report(loadable=1001), 0, _settings())[0]

    def test_growth_over_the_active_base(self):
        assert "2.0x active 100" in autoload.guardrail_trips(_report(loadable=201), 100, _settings())[0]

    def test_growth_is_skipped_while_nothing_is_active(self):
        assert autoload.guardrail_trips(_report(loadable=900), 0, _settings()) == []

    def test_scrub_credit_cap(self):
        assert "credit cap 200" in autoload.guardrail_trips(_report(needs_scrub=201), 0, _settings())[0]

    def test_queue_without_a_campaign(self):
        trips = autoload.guardrail_trips(_report(unmapped_pools=["builders"]), 0, _settings())
        assert "builders" in trips[0]


class TestRunAutoload:
    def test_off_touches_nothing(self, wired):
        assert _run(_settings(lending_dialer_autoload_mode="off")).status == "off"
        assert wired["live_calls"] == []

    def test_unknown_mode_is_refused(self, wired):
        assert _run(_settings(lending_dialer_autoload_mode="yes")).status == "refused"

    def test_active_pause_stops_the_run(self, wired):
        wired["paused"] = True
        assert _run(_settings()).status == "paused" and wired["live_calls"] == []

    def test_dry_run_mode_never_pushes(self, wired):
        out = _run(_settings(lending_dialer_autoload_mode="dry_run"))
        assert out.status == "dry_run" and wired["live_calls"] == []

    def test_a_tripped_guardrail_halts_before_the_push(self, wired):
        wired["preview"] = _report(loadable=5000)
        out = _run(_settings())
        assert out.status == "halted" and out.reasons and wired["live_calls"] == []

    def test_live_push_passes_the_failure_limit(self, wired):
        out = _run(_settings())
        assert out.status == "loaded" and wired["live_calls"][0]["max_consecutive_failures"] == 10

    def test_no_dialer_is_refused(self, wired):
        assert _run(_settings(), dialer=None).status == "refused"

    def test_abort_is_reported(self, wired):
        wired["live_error"] = LoadAborted("10 consecutive dialer failures")
        out = _run(_settings())
        assert out.status == "aborted" and "10 consecutive" in out.reasons[0]


class TestNotify:
    def _posts(self, monkeypatch, outcome):
        posts = []
        monkeypatch.setattr(autoload, "_post", lambda ch, msg: posts.append(ch))
        autoload.notify(outcome, _settings())
        return posts

    def test_summary_goes_to_the_tasks_channel(self, monkeypatch):
        assert self._posts(monkeypatch, autoload.Outcome("loaded", report=_report(dry_run=False))) == ["C-TASKS"]

    def test_halt_goes_to_the_alert_channel(self, monkeypatch):
        assert self._posts(monkeypatch, autoload.Outcome("halted", report=_report(), reasons=["x"])) == ["C-ALERT"]

    def test_off_posts_nothing(self, monkeypatch):
        assert self._posts(monkeypatch, autoload.Outcome("off")) == []


class TestRunHour:
    def test_outside_9am_et_does_nothing(self, monkeypatch):
        monkeypatch.setattr(autoload, "run_autoload", lambda **kw: pytest.fail("must not run"))
        assert autoload.main([], now=datetime(2026, 10, 7, 14, 5, tzinfo=timezone.utc)) == 0  # 10:05 EDT

    def test_9am_et_runs(self, monkeypatch):
        ran = []
        monkeypatch.setattr(autoload, "run_autoload", lambda **kw: ran.append(1) or autoload.Outcome("off"))
        monkeypatch.setattr(autoload, "notify", lambda o, s: None)
        assert autoload.main([], now=datetime(2026, 10, 7, 13, 5, tzinfo=timezone.utc)) == 0  # 09:05 EDT
        assert ran == [1]
