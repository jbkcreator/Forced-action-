"""R1 1.6: the 6 AM dialer queue check task (switch, hour, weekdays, ops ping)."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

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
def run(monkeypatch):
    state = SimpleNamespace(enabled=True, channel="C_DAVID", token=SecretStr("xoxb"), report=_report(50),
                            ops=[], posts=[], reads=0, unset=[])
    monkeypatch.setattr(task, "get_settings", lambda: SimpleNamespace(
        lending_dialer_queue_check_enabled=state.enabled, lending_david_channel=state.channel,
        lending_slack_bot_token=state.token, lending_morning_list_lead_days=14))

    @contextmanager
    def session():
        yield None

    def read(db, now, *, lead_days):
        state.reads += 1
        if isinstance(state.report, Exception):
            raise state.report
        return state.report

    monkeypatch.setattr(task, "lending_session", session)
    monkeypatch.setattr(task, "read_queue", read)
    monkeypatch.setattr(task, "post_ops_notice", lambda m: state.ops.append(m) or True)
    monkeypatch.setattr(task, "report_unset", lambda post, needed: state.unset.append(post) or [])

    def go(now=TUE_6_10, argv=(), **kw):
        return task.main(list(argv), now=now, post=lambda m: state.posts.append(m) or True, **kw)

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


def test_unset_channel_is_reported_and_nothing_posts(run, monkeypatch):
    run.channel = ""
    assert task.main([], now=TUE_6_10) == 1
    assert run.unset == ["David's dialer queue check"] and run.reads == 0
