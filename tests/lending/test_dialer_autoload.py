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
                lending_dialer_autoload_max_consecutive_failures=10, lending_dialer_autoload_tracerfy_floor=500,
                lending_dial_tasks_channel="C-TASKS", lending_slack_bot_token=None)
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
    state = {"paused": [False, False], "active": 0, "preview": _report(), "live_calls": [], "live_error": None}
    monkeypatch.setattr(autoload, "_paused", lambda s: state["paused"].pop(0))
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


def _run(settings, dialer=FakeDialer(), balance=10_000, scrubber=lambda phones: []):
    return autoload.run_autoload(settings=settings, session_factory=_factory, get_dialer=lambda: dialer,
                                 scrubber=scrubber, read_balance=lambda: balance)


class TestGuardrails:
    def test_a_normal_run_trips_nothing(self):
        assert autoload.guardrail_trips(_report(), 300, _settings()) == []

    def test_too_many_records(self):
        assert "max 1000" in autoload.guardrail_trips(_report(loadable=1001), 0, _settings())[0]

    def test_growth_over_the_active_base(self):
        assert "2.0x active 100" in autoload.guardrail_trips(_report(loadable=201), 100, _settings())[0]

    def test_growth_is_skipped_while_nothing_is_active(self):
        assert autoload.guardrail_trips(_report(loadable=900), 0, _settings()) == []

    def test_numbers_needing_a_scrub_never_halt_the_run(self):
        assert autoload.guardrail_trips(_report(needs_scrub=5000), 0, _settings()) == []


class TestScrubBudget:
    def test_per_run_cap(self):
        assert autoload.scrub_budget(10_000, _settings()) == 200

    def test_balance_floor(self):
        assert autoload.scrub_budget(550, _settings()) == 50

    def test_below_the_floor_scrubs_nothing(self):
        assert autoload.scrub_budget(400, _settings()) == 0

    def test_unreadable_balance_scrubs_nothing(self):
        assert autoload.scrub_budget(None, _settings()) == 0

    def test_capped_scrubber_sends_only_the_budget(self):
        sent = []
        autoload.capped_scrubber(lambda phones: sent.extend(phones) or [], 2)(["a", "b", "c"])
        assert sent == ["a", "b"]

    def test_zero_budget_never_calls_tracerfy(self):
        assert autoload.capped_scrubber(lambda phones: pytest.fail("called"), 0)(["a"]) == []

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
        wired["paused"] = [True]
        assert _run(_settings()).status == "paused" and wired["live_calls"] == []

    def test_dry_run_mode_never_pushes(self, wired):
        out = _run(_settings(lending_dialer_autoload_mode="dry_run"))
        assert out.status == "dry_run" and wired["live_calls"] == []

    def test_a_tripped_guardrail_halts_before_the_push(self, wired):
        wired["preview"] = _report(loadable=5000)
        out = _run(_settings())
        assert out.status == "halted" and out.reasons and wired["live_calls"] == []

    def test_pause_set_before_the_push_stops_it(self, wired):
        wired["paused"] = [False, True]
        assert _run(_settings()).status == "paused" and wired["live_calls"] == []

    def test_over_cap_numbers_are_deferred_not_halted(self, wired):
        wired["preview"] = _report(needs_scrub=350)
        out = _run(_settings())
        assert out.status == "loaded" and out.scrub_deferred == 150

    def test_live_push_passes_the_failure_limit(self, wired):
        out = _run(_settings())
        assert out.status == "loaded" and wired["live_calls"][0]["max_consecutive_failures"] == 10

    def test_no_dialer_is_refused(self, wired):
        assert _run(_settings(), dialer=None).status == "refused"

    def test_abort_is_reported(self, wired):
        live = _report(dry_run=False, loaded=7)
        wired["live_error"] = LoadAborted("10 consecutive dialer failures", live)
        out = _run(_settings())
        assert out.status == "aborted" and "10 consecutive" in out.reasons[0]
        assert out.report is live and out.report.loaded == 7


class TestNotify:
    def _posts(self, monkeypatch, outcome):
        posts = []
        monkeypatch.setattr(autoload, "_post_summary", lambda settings, msg: posts.append("summary"))
        monkeypatch.setattr(autoload, "post_exceptions_alert", lambda **kw: posts.append("exceptions"))
        autoload.notify(outcome, _settings())
        return posts

    def test_a_clean_run_posts_only_the_summary(self, monkeypatch):
        failed = _report(dry_run=False, failed=[{"record_ref": "x"}])
        assert self._posts(monkeypatch, autoload.Outcome(autoload.Status.LOADED, report=failed)) == ["summary"]

    def test_a_halt_also_goes_to_exceptions(self, monkeypatch):
        out = autoload.Outcome(autoload.Status.HALTED, report=_report(), reasons=["x"])
        assert self._posts(monkeypatch, out) == ["summary", "exceptions"]

    def test_off_posts_nothing(self, monkeypatch):
        assert self._posts(monkeypatch, autoload.Outcome(autoload.Status.OFF)) == []


class TestRunHour:
    def test_outside_9am_et_does_nothing(self, monkeypatch):
        monkeypatch.setattr(autoload, "run_autoload", lambda **kw: pytest.fail("must not run"))
        assert autoload.main([], now=datetime(2026, 10, 7, 14, 5, tzinfo=timezone.utc)) == 0  # 10:05 EDT

    def test_9am_et_runs(self, monkeypatch):
        ran = []
        monkeypatch.setattr(autoload, "run_autoload", lambda **kw: ran.append(1) or autoload.Outcome(autoload.Status.OFF))
        monkeypatch.setattr(autoload, "notify", lambda o, s: None)
        assert autoload.main([], now=datetime(2026, 10, 7, 13, 5, tzinfo=timezone.utc)) == 0  # 09:05 EDT
        assert ran == [1]


class TestUnexpectedError:
    def test_an_unexpected_failure_still_alerts(self, monkeypatch):
        sent = []
        def boom(**kw):
            raise RuntimeError("db down")
        monkeypatch.setattr(autoload, "run_autoload", boom)
        monkeypatch.setattr(autoload, "notify", lambda o, s: sent.append(o))
        assert autoload.main(["--force"]) == 1
        assert sent[0].status == "error" and "RuntimeError" in sent[0].reasons[0]
