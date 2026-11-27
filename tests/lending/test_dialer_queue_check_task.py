"""R1 1.6: the 6 AM dialer queue check task (switch, hour, weekdays, ops ping)."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from src.lending.dialer import delivery_state
from src.lending.dialer.queue_manager import QueueReport, SourceCount
from src.tasks import lending_dialer_queue_check as task

# 2026-11-24 is a Tuesday, EST: 11:10 UTC is 6:10 AM ET.
TUE_6_10 = datetime(2026, 11, 24, 11, 10, tzinfo=timezone.utc)
TUE_5_59 = datetime(2026, 11, 24, 10, 59, tzinfo=timezone.utc)
SAT_6_10 = datetime(2026, 11, 28, 11, 10, tzinfo=timezone.utc)


def _report(eligible: int) -> QueueReport:
    return QueueReport(day=date(2026, 11, 24), target=50,
                       sources=(SourceCount("wholesalers", eligible, eligible, {}),))


@pytest.fixture
def run(monkeypatch, tmp_path):
    monkeypatch.setattr(delivery_state, "ROOT", tmp_path)
    state = SimpleNamespace(enabled=True, channel="C_DAVID", token=SecretStr("xoxb"), report=_report(50),
                            ops=[], posts=[], reads=0, unset=[], autoload=True, read_kw={})
    monkeypatch.setattr(task, "get_settings", lambda: SimpleNamespace(
        lending_dialer_queue_check_enabled=state.enabled, lending_david_channel=state.channel,
        lending_slack_bot_token=state.token, lending_morning_list_lead_days=14,
        lending_pr_dialer_autoload_enabled=state.autoload))

    @contextmanager
    def session():
        yield None

    def read(db, now, *, lead_days, autoload_enabled):
        state.reads += 1
        state.read_kw = {"autoload_enabled": autoload_enabled}
        if isinstance(state.report, Exception):
            raise state.report
        return state.report

    monkeypatch.setattr(task, "lending_session", session)
    monkeypatch.setattr(task, "read_queue", read)
    state.ops_ok, state.post_fail = True, False
    monkeypatch.setattr(task, "post_ops_notice", lambda m: state.ops.append(m) or state.ops_ok)
    monkeypatch.setattr(task, "report_unset", lambda post, needed: state.unset.append(post) or [])

    def post(message):
        if state.post_fail:
            raise RuntimeError("slack down")
        state.posts.append(message)
        return True

    def go(now=TUE_6_10, argv=(), **kw):
        return task.main(list(argv), now=now, post=post, **kw)

    state.go = go
    return state


def test_acts_only_at_6_et_on_weekdays(run):
    assert run.go(TUE_5_59) == 0 and run.posts == []
    assert run.go(SAT_6_10) == 0 and run.posts == []
    assert run.go(TUE_6_10) == 0 and len(run.posts) == 1


def test_short_queue_also_goes_to_ops(run):
    run.report = _report(4)
    run.go()
    assert run.posts[0].startswith(":warning: Only 4 of 50 eligible")
    assert run.ops == run.posts


def test_full_queue_does_not_ping_ops(run):
    run.go()
    assert run.posts[0].startswith("Dialer queue ready: 50 eligible") and run.ops == []


def test_switch_off_posts_nothing(run):
    run.enabled = False
    assert run.go() == 0 and run.posts == [] and run.reads == 0


def test_dry_run_reads_but_posts_nothing_even_with_switch_off(run):
    run.enabled = False
    assert run.go(SAT_6_10, ["--force", "--dry-run"]) == 0
    assert run.reads == 1 and run.posts == [] and run.ops == []


def test_unreadable_queue_alerts_ops_instead_of_posting_a_count(run):
    run.report = RuntimeError("boom")
    assert run.go() == 1
    assert run.posts == [] and len(run.ops) == 1 and "RuntimeError" in run.ops[0]


def test_unset_david_channel_is_reported_and_a_full_queue_posts_nothing(run):
    run.channel = ""
    assert task.main([], now=TUE_6_10) == 1
    assert run.unset == ["David's dialer queue check"] and run.ops == []


def test_dry_run_failure_does_not_ping_ops(run):
    run.report = RuntimeError("boom")
    assert run.go(TUE_6_10, ["--dry-run"]) == 1 and run.ops == []


# ---- PR 486 review fixes: each destination delivered independently, and a retry resumes ----
TUE_6_25 = datetime(2026, 11, 24, 11, 25, tzinfo=timezone.utc)
TUE_7_05 = datetime(2026, 11, 24, 12, 5, tzinfo=timezone.utc)  # run.sh's third attempt lands after 6:xx


def test_david_channel_failure_still_alerts_ops_and_the_retry_posts_only_davids(run):
    run.report, run.post_fail = _report(4), True
    assert run.go() == 1
    assert run.posts == [] and len(run.ops) == 1
    run.post_fail = False
    assert run.go(TUE_7_05) == 0
    assert len(run.posts) == 1 and len(run.ops) == 1 and run.reads == 1  # same message, read once
    assert run.posts[0] == run.ops[0]


def test_ops_failure_fails_the_run_and_the_retry_sends_only_the_ops_alert(run):
    run.report, run.ops_ok = _report(4), False
    assert run.go() == 1 and len(run.posts) == 1
    run.ops_ok = True
    assert run.go(TUE_7_05) == 0
    assert len(run.posts) == 1 and len(run.ops) == 2


def test_a_finished_day_is_not_posted_again(run):
    assert run.go() == 0
    assert run.go(TUE_6_25) == 0 and run.go(TUE_7_05) == 0
    assert len(run.posts) == 1 and run.reads == 1


def test_no_run_today_means_nothing_after_6_et(run):
    assert run.go(TUE_7_05) == 0 and run.posts == [] and run.reads == 0


def test_the_autoload_switch_is_passed_to_the_queue_read(run):
    run.autoload = False
    run.go()
    assert run.read_kw == {"autoload_enabled": False}


def test_unset_david_channel_still_sends_the_ops_shortage_alert(run):
    run.channel, run.report = "", _report(4)
    assert task.main([], now=TUE_6_10) == 1  # David's post is still missing
    assert run.reads == 1 and len(run.ops) == 1 and run.ops[0].startswith(":warning: Only 4 of 50 eligible")
    assert run.unset == ["David's dialer queue check"]
    run.channel = "C_DAVID"
    assert run.go(TUE_7_05) == 0 and run.posts == run.ops and len(run.ops) == 1  # ops not alerted twice


def test_overlapping_runs_wait_on_the_lock_and_post_once(run):
    import threading
    entered, release, results = threading.Event(), threading.Event(), []

    def slow_post(message):
        entered.set()
        release.wait(5)
        run.posts.append(message)

    first = threading.Thread(target=lambda: results.append(task.main([], now=TUE_6_10, post=slow_post)))
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=lambda: results.append(run.go(TUE_6_25)))
    second.start()
    second.join(0.3)
    assert second.is_alive()  # waiting on the first run's lock
    release.set()
    first.join(5), second.join(5)
    assert results == [0, 0] and len(run.posts) == 1 and run.reads == 1
