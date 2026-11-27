"""R1 1.6: the 2:45 PM caller scorecard task (switch, hour, weekdays, notes thread)."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from src.lending.dialer import delivery_state
from src.lending.dialer.scorecard import Scorecard, ScoredCall
from src.tasks import lending_caller_scorecard as task

# 2026-11-24 is a Tuesday, EST: 19:10 UTC is 2:10 PM ET.
TUE_2_10 = datetime(2026, 11, 24, 19, 10, tzinfo=timezone.utc)
TUE_1_59 = datetime(2026, 11, 24, 18, 59, tzinfo=timezone.utc)
TUE_3_10 = datetime(2026, 11, 24, 20, 10, tzinfo=timezone.utc)
SAT_2_10 = datetime(2026, 11, 28, 19, 10, tzinfo=timezone.utc)


def _card(*calls: ScoredCall) -> Scorecard:
    return Scorecard(day=date(2026, 11, 24), as_of=TUE_2_10, dials=10, connects=len(calls), long_conversations=0,
                     booked_stage=1, booked_reported=1, calls=calls, read_failed=False)


def _call(cid, *, recorded) -> ScoredCall:
    return ScoredCall(dialer_call_id=cid, phone_last4="1234", talk_sec=90, disposition="BOOKED", recorded=recorded,
                      disclosure_logged=True, dialer_contact_id=None, extraction=None, notes_source=None)


class FakeClient:
    def __init__(self):
        self.posts, self.fail_first, self.down = [], 0, False

    def chat_postMessage(self, **kw):
        if self.down is True or (self.down == "thread" and "thread_ts" in kw):
            raise RuntimeError("slack down")
        if self.fail_first:
            self.fail_first -= 1
            raise RuntimeError("slack down")
        self.posts.append(kw)
        return {"ts": f"1.{len(self.posts)}"}


@pytest.fixture
def run(monkeypatch, tmp_path):
    monkeypatch.setattr(delivery_state, "ROOT", tmp_path)
    state = SimpleNamespace(enabled=True, channel="C_DAVID", agent="D1", token=SecretStr("xoxb"),
                            card=_card(_call("c1", recorded=True)), ops=[], unset=[], client=FakeClient(), built=0)
    monkeypatch.setattr(task, "get_settings", lambda: SimpleNamespace(
        lending_caller_scorecard_enabled=state.enabled, lending_david_channel=state.channel,
        lending_david_bd_agent_id=state.agent, lending_slack_bot_token=state.token))

    @contextmanager
    def session():
        yield None

    def build(db, day, *, agent_id, as_of):
        state.built += 1
        if isinstance(state.card, Exception):
            raise state.card
        assert agent_id == state.agent
        return state.card

    monkeypatch.setattr(task, "lending_session", session)
    monkeypatch.setattr(task, "build_scorecard", build)
    monkeypatch.setattr(task, "post_ops_notice", lambda m: state.ops.append(m) or True)
    monkeypatch.setattr(task, "report_unset", lambda post, needed: state.unset.append((post, tuple(needed))) or [])
    monkeypatch.setattr(task.time, "sleep", lambda s: None)
    state.go = lambda now=TUE_2_10, argv=(): task.main(list(argv), now=now, client=state.client)
    return state


def test_task_acts_only_at_14_et_on_weekdays(run):
    assert run.go(TUE_1_59) == 0 and run.go(TUE_3_10) == 0 and run.go(SAT_2_10) == 0
    assert run.client.posts == []
    assert run.go(TUE_2_10) == 0 and len(run.client.posts) == 1


def test_notes_thread_posted_only_when_calls_need_notes(run):
    run.go()
    assert len(run.client.posts) == 1
    run.client.posts.clear()
    run.card = _card(_call("c1", recorded=True), _call("c2", recorded=False))
    run.go(argv=["--force"])  # today is already done; --force neither reads nor writes the delivery state
    main, thread = run.client.posts
    assert "thread_ts" not in main and thread["thread_ts"] == "1.1" and thread["channel"] == "C_DAVID"
    assert thread["blocks"][0]["accessory"]["value"] == "c2"


def test_main_post_carries_the_scorecard_blocks_and_a_text_fallback(run):
    run.go()
    (post,) = run.client.posts
    assert post["channel"] == "C_DAVID" and post["text"] and post["blocks"]


def test_missing_agent_id_reports_unset_and_posts_nothing(run):
    run.agent = ""
    assert run.go() == 1
    assert run.client.posts == [] and run.built == 0
    assert run.unset == [("David's 2:45 PM scorecard",
                          ("LENDING_DAVID_CHANNEL", "LENDING_DAVID_BD_AGENT_ID", "LENDING_SLACK_BOT_TOKEN"))]


def test_switch_off_posts_nothing(run):
    run.enabled = False
    assert run.go() == 0 and run.client.posts == [] and run.built == 0


def test_dry_run_builds_but_posts_nothing_even_with_switch_off(run):
    run.enabled = False
    assert run.go(SAT_2_10, ["--force", "--dry-run"]) == 0
    assert run.built == 1 and run.client.posts == []


def test_a_failed_main_post_is_retried_once_then_succeeds(run):
    run.client.fail_first = 1
    assert run.go() == 0 and len(run.client.posts) == 1


def test_unreadable_database_alerts_ops_instead_of_posting(run):
    run.card = RuntimeError("db down")
    assert run.go() == 1
    assert run.client.posts == [] and len(run.ops) == 1 and "RuntimeError" in run.ops[0]


def test_dry_run_failure_does_not_ping_ops(run):
    run.card = RuntimeError("db down")
    assert run.go(TUE_2_10, ["--dry-run"]) == 1 and run.ops == []


# ---- PR 486 review fixes: a retry after 3 PM resumes the run ----
TUE_3_00 = datetime(2026, 11, 24, 20, 0, tzinfo=timezone.utc)
TUE_3_45 = datetime(2026, 11, 24, 20, 45, tzinfo=timezone.utc)


def test_failed_notes_thread_is_resumed_after_3_pm_under_the_same_main_post(run):
    run.card = _card(_call("c2", recorded=False))
    run.client.down = "thread"
    assert run.go() == 1
    (main,) = run.client.posts
    run.client.down = False
    assert run.go(TUE_3_00) == 0
    main_again, thread = run.client.posts
    assert main_again is main and thread["thread_ts"] == "1.1"  # no second main post


def test_failed_main_post_is_resumed_after_3_pm(run):
    run.card = _card(_call("c2", recorded=False))
    run.client.down = True
    assert run.go() == 1 and run.client.posts == []
    run.client.down = False
    assert run.go(TUE_3_00) == 0
    main, thread = run.client.posts
    assert "thread_ts" not in main and thread["thread_ts"] == "1.1"


def test_failed_build_is_retried_after_3_pm(run):
    run.card = RuntimeError("db down")
    assert run.go() == 1
    run.card = _card(_call("c1", recorded=True))
    assert run.go(TUE_3_00) == 0 and len(run.client.posts) == 1


def test_a_finished_day_is_not_posted_again(run):
    assert run.go() == 0
    assert run.go(TUE_3_00) == 0 and run.go(TUE_3_45) == 0
    assert len(run.client.posts) == 1 and run.built == 1
