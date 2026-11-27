"""R1 0.6 dependency watch, no database: alert episodes start and clear once, would-have-paused while the switch is
off, held cards reopen once the feeds are fresh, LendingFlow silence in business hours, exhausted marking."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from config import lending_fallbacks as fb
from src.lending import dependency_health as dh
from src.lending import dependency_watch as dw
from src.lending import exhausted as ex
from src.lending.dispatch import circuit_breaker as cb

NOW = datetime(2026, 11, 25, 19, 0, tzinfo=timezone.utc)  # Wednesday 14:00 ET


class _Db:
    def __init__(self, *, ages=None, lf_last=None, alerts=None, fail_suppression=False, fail_health=False,
                 over_cap=None):
        ages = ages or {}
        self.rows = []
        for feed in fb.OPT_OUT_FEEDS:
            minutes = ages.get(feed.key, 1)
            last = None if minutes is None else NOW - timedelta(minutes=minutes)
            self.rows.append(SimpleNamespace(dependency=feed.key, last_success_at=last, failing_since=None,
                                             detail=None))
        if lf_last is not None:
            self.rows.append(SimpleNamespace(dependency=fb.FEED_LENDINGFLOW_INBOUND, last_success_at=lf_last,
                                             failing_since=None, detail=None))
        self.alerts = {k: {"active": v, "detail": None, "unposted": None} for k, v in (alerts or {}).items()}
        self.fail_suppression, self.fail_health = fail_suppression, fail_health
        self.over_cap = over_cap or {}
        self.marked: list[str] = []

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, stmt, params=None):
        if stmt is dh._SUPPRESSION_PROBE:
            if self.fail_suppression:
                raise RuntimeError("missing")
            return SimpleNamespace(first=lambda: (1,))
        if stmt is dh._READ:
            if self.fail_health:
                raise RuntimeError("missing")
            return iter([r for r in self.rows if r.dependency in params["deps"]])
        if stmt is dw._LAST_LENDINGFLOW_LEAD:
            return SimpleNamespace(scalar=lambda: None)
        if stmt is dw._ALERTS:
            return iter([SimpleNamespace(alert_key=k, **v) for k, v in self.alerts.items()])
        if stmt is dw._SET_ALERT:
            self.alerts[params["k"]] = {"active": params["active"], "detail": params["detail"],
                                        "unposted": params["unposted"]}
            return None
        if stmt is dw._POSTED:
            if self.alerts[params["k"]]["unposted"] == params["kind"]:
                self.alerts[params["k"]]["unposted"] = None
            return None
        if stmt is ex._OVER_CAP_NOW:
            return SimpleNamespace(fetchall=lambda: list(self.over_cap.items()))
        if stmt is ex._MARK:
            self.marked.append(params["phone"])
            return None
        raise AssertionError(f"unexpected SQL {stmt}")


def _run(db, *, enforce=True, lendingflow_on=False, now=NOW, slack_up=True):
    posted, reopened = [], []

    def reopen(channel):
        reopened.append(channel)
        return 2

    result = dw.run_watch(db, now, post=lambda message: posted.append(message) or slack_up, reopen=reopen,
                          lendingflow_on=lendingflow_on, enforce=enforce)
    return result, posted, reopened


def test_fresh_feeds_post_nothing_and_reopen_held_cards_on_every_breaker_channel():
    result, posted, reopened = _run(_Db())
    assert posted == [] and result.started == [] and set(reopened) == set(cb.CHANNELS)
    assert result.reopened_cards == 2 * len(cb.CHANNELS)


def test_a_stale_ghl_sync_alerts_once_and_holds_every_channel():
    db = _Db(ages={fb.FEED_GHL_DND_BACKSTOP: 50})
    result, posted, reopened = _run(db)
    assert result.started == ["stale:ghl_dnd_backstop"] and len(posted) == 1
    assert "PAUSED" in posted[0] and "GHL opt-out sync" in posted[0] and reopened == []
    _, posted_again, _ = _run(db)
    assert posted_again == []  # same episode: no second alert


def test_with_the_switch_off_it_says_would_have_paused():
    _, posted, _ = _run(_Db(ages={fb.FEED_GHL_DND_BACKSTOP: 50}), enforce=False)
    assert "Would have paused" in posted[0] and "LENDING_FAILCLOSED_PAUSE_ENABLED" in posted[0]


def test_a_stale_mailbox_pauses_every_channel_and_reopens_nothing():
    _, posted, reopened = _run(_Db(ages={fb.FEED_IMAP_OPT_OUT: 20}))
    assert all(channel in posted[0] for channel in fb.ALL_CHANNELS)
    assert reopened == []


def test_a_refused_alert_is_saved_and_posted_again_on_the_next_pass():
    db = _Db(ages={fb.FEED_GHL_DND_BACKSTOP: 50})
    _, posted, _ = _run(db, slack_up=False)
    saved = db.alerts["stale:ghl_dnd_backstop"]
    assert len(posted) == 1 and saved["active"] is True and saved["unposted"] == dw.POST_START
    _, posted, _ = _run(db)
    assert len(posted) == 1 and "PAUSED" in posted[0] and db.alerts["stale:ghl_dnd_backstop"]["unposted"] is None
    _, posted, _ = _run(db)
    assert posted == []  # accepted once: not posted again


def test_an_outage_that_ends_before_slack_took_the_start_is_still_reported():
    """Review (round 2) F5: the start post is refused, the feed recovers before the next pass: ops still hears of it."""
    db = _Db(ages={fb.FEED_GHL_DND_BACKSTOP: 50})
    _run(db, slack_up=False)
    db.rows = _Db().rows  # the feed is fresh again
    result, posted, _ = _run(db)
    assert result.cleared == ["stale:ghl_dnd_backstop"] and len(posted) == 1
    assert "was stale" in posted[0] and "GHL opt-out sync" in posted[0] and "fresh again" in posted[0]
    assert db.alerts["stale:ghl_dnd_backstop"] == {"active": False, "detail": db.alerts["stale:ghl_dnd_backstop"]["detail"],
                                                   "unposted": None}


def test_a_refused_recovery_notice_is_posted_again_on_the_next_pass():
    db = _Db(alerts={"stale:ghl_dnd_backstop": True})
    _run(db, slack_up=False)
    assert db.alerts["stale:ghl_dnd_backstop"]["active"] is False
    assert db.alerts["stale:ghl_dnd_backstop"]["unposted"] == dw.POST_CLEAR
    _, posted, _ = _run(db)
    assert posted == [dw.clear_message("stale:ghl_dnd_backstop")]
    assert db.alerts["stale:ghl_dnd_backstop"]["unposted"] is None


def test_the_episode_clears_once_when_the_feed_is_fresh_again():
    db = _Db(alerts={"stale:ghl_dnd_backstop": True})
    result, posted, _ = _run(db)
    assert result.cleared == ["stale:ghl_dnd_backstop"] and posted == [dw.clear_message("stale:ghl_dnd_backstop")]
    assert db.alerts["stale:ghl_dnd_backstop"]["active"] is False


@pytest.mark.parametrize("fail", [{"fail_health": True}, {"fail_suppression": True}])
def test_a_stale_feed_episode_is_not_cleared_while_its_status_cannot_be_read(fail):
    """Review (round 2) F7: an unreadable health table says nothing about the feed; it must not read as recovery."""
    db = _Db(alerts={"stale:ghl_dnd_backstop": True}, **fail)
    result, posted, _ = _run(db)
    assert result.cleared == [] and all("fresh again" not in message for message in posted)
    assert db.alerts["stale:ghl_dnd_backstop"]["active"] is True
    db.fail_health = db.fail_suppression = False
    result, posted, _ = _run(db)  # a successful read confirms recovery
    assert "stale:ghl_dnd_backstop" in result.cleared and any("fresh again" in message for message in posted)


def test_an_unreadable_health_table_is_its_own_episode_and_reopens_nothing():
    result, posted, reopened = _run(_Db(fail_health=True))
    assert result.started == [dw.KEY_HEALTH_UNREADABLE] and reopened == []


def test_business_seconds_skip_nights_and_weekends():
    friday_4pm = datetime(2026, 11, 27, 21, 0, tzinfo=timezone.utc)
    monday_11am = datetime(2026, 11, 30, 16, 0, tzinfo=timezone.utc)
    assert dw.business_seconds(friday_4pm, monday_11am) == 3 * 3600  # Fri 16-17 + Mon 9-11


def test_lendingflow_silent_one_business_day_alerts():
    last = NOW - timedelta(hours=25)  # Tue 13:00 ET -> Wed 14:00 ET = 4 + 5 = 9 business hours
    result, posted, _ = _run(_Db(lf_last=last), lendingflow_on=True)
    assert dw.KEY_LENDINGFLOW_SILENCE in result.started and "LendingFlow may be down" in posted[0]


def test_lendingflow_quiet_morning_is_not_an_outage():
    last = NOW - timedelta(hours=5)  # 09:00 ET -> 14:00 ET: 5 business hours, a normal gap at ~2 leads a day
    assert dw.lendingflow_silence(last, NOW) is None


def test_business_seconds_skip_federal_holidays():
    wednesday_4pm = datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)
    friday_10am = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
    assert dw.business_seconds(wednesday_4pm, friday_10am) == 2 * 3600  # Wed 16-17 + Fri 9-10; Thanksgiving skipped
    assert dw.us_federal_holidays(2026) >= {date(2026, 11, 26), date(2026, 7, 3), date(2026, 12, 25)}
    assert dw.is_business_day(date(2026, 11, 27)) and not dw.is_business_day(date(2026, 11, 26))


def test_lendingflow_quiet_overnight_is_not_an_outage():
    evening = datetime(2026, 11, 24, 21, 30, tzinfo=timezone.utc)  # Tue 16:30 ET
    next_morning = datetime(2026, 11, 25, 15, 0, tzinfo=timezone.utc)  # Wed 10:00 ET = 1.5 business hours
    assert dw.lendingflow_silence(evening, next_morning) is None
    assert dw.lendingflow_silence(None, next_morning) is None


def test_lendingflow_silence_is_not_checked_while_intake_is_off():
    result, _, _ = _run(_Db(lf_last=NOW - timedelta(days=3)), lendingflow_on=False)
    assert dw.KEY_LENDINGFLOW_SILENCE not in result.started


def test_the_watch_marks_exhausted_phones():
    db = _Db(over_cap={"+18135557001": 6})
    result, _, _ = _run(db)
    assert result.exhausted_marked == 1 and db.marked == ["+18135557001"]


def test_dry_run_writes_and_posts_nothing():
    db = _Db(ages={fb.FEED_GHL_DND_BACKSTOP: 50})
    posted = []
    result = dw.run_watch(db, NOW, post=posted.append, reopen=lambda c: 0, lendingflow_on=False, enforce=True,
                          dry_run=True)
    assert result.started == ["stale:ghl_dnd_backstop"] and posted == [] and db.alerts == {}
