"""Entry point of David's end-of-day post. Database- and network-free: the build, session, GHL and Slack are stubbed."""
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from src.lending.david_activity import DavidActivity
from src.tasks import lending_david_summary as task

SEVEN_PM_UTC = datetime(2026, 10, 28, 23, 25, tzinfo=timezone.utc)   # 19:25 ET (EDT)
WINTER_SEVEN_PM_UTC = datetime(2026, 12, 2, 0, 25, tzinfo=timezone.utc)  # 19:25 ET on Dec 1 (EST)
NOON_ET = datetime(2026, 10, 28, 16, 0, tzinfo=timezone.utc)


def _settings(**over):
    base = dict(lending_david_summary_enabled=True, lending_david_ghl_user_id="u-david",
                lending_flagged_tag="flag-for-josh", lending_daily_channel="C_DAILY", lending_david_channel="C1",
                lending_slack_bot_token=SecretStr("xoxb-t"), lending_ghl_app_url="https://app")
    return SimpleNamespace(**{**base, **over})


@pytest.fixture
def wired(monkeypatch):
    posted, builds = [], []

    class FakeClient:
        def __init__(self, token):
            pass

        def chat_postMessage(self, **kw):
            posted.append(kw)

    @contextmanager
    def fake_session():
        yield object()

    monkeypatch.setattr(task, "get_settings", _settings)
    monkeypatch.setattr(task, "lending_ghl_account", lambda: SimpleNamespace(api_key="k", location_id="loc"))
    monkeypatch.setattr(task, "lending_session", fake_session)
    monkeypatch.setattr(task, "ghl_transport", lambda key: (None, None, {}))
    monkeypatch.setattr(task, "build_activity",
                        lambda db, day, **kw: builds.append((day, kw["user_id"], kw["flagged_tag"])) or DavidActivity(day))
    monkeypatch.setattr("slack_sdk.WebClient", FakeClient)
    return posted, builds


def test_outside_the_7pm_hour_does_nothing(wired):
    posted, builds = wired
    assert task.main([], now=NOON_ET) == 0 and posted == [] and builds == []


@pytest.mark.parametrize("now, day", [(SEVEN_PM_UTC, "2026-10-28"), (WINTER_SEVEN_PM_UTC, "2026-12-01")])
def test_posts_once_at_7pm_et_in_both_seasons(wired, now, day):
    posted, builds = wired
    assert task.main([], now=now) == 0
    assert len(posted) == 1 and posted[0]["channel"] == "C1" and "David · end of day" in posted[0]["text"]
    assert str(builds[0][0]) == day and builds[0][1:] == ("u-david", "flag-for-josh")


def test_off_switch_posts_nothing(wired, monkeypatch):
    posted, builds = wired
    monkeypatch.setattr(task, "get_settings", lambda: _settings(lending_david_summary_enabled=False))
    assert task.main([], now=SEVEN_PM_UTC) == 0 and posted == [] and builds == []


def test_missing_user_id_is_an_error_and_posts_nothing(wired, monkeypatch):
    posted, _ = wired
    monkeypatch.setattr(task, "get_settings", lambda: _settings(lending_david_ghl_user_id=" "))
    assert task.main([], now=SEVEN_PM_UTC) == 1 and posted == []


def test_missing_slack_config_is_an_error(wired, monkeypatch):
    posted, _ = wired
    monkeypatch.setattr(task, "get_settings", lambda: _settings(lending_david_channel=""))
    assert task.main([], now=SEVEN_PM_UTC) == 1 and posted == []


def test_dry_run_builds_but_never_posts(wired, monkeypatch):
    posted, builds = wired
    monkeypatch.setattr(task, "get_settings", lambda: _settings(lending_david_channel="", lending_slack_bot_token=None))
    assert task.main(["--dry-run", "--force"], now=NOON_ET) == 0
    assert posted == [] and len(builds) == 1


def test_never_posts_to_the_scoreboard_channel(wired, monkeypatch):
    posted, _ = wired
    monkeypatch.setattr(task, "get_settings", lambda: _settings(lending_david_channel=""))
    assert task.main([], now=SEVEN_PM_UTC) == 1 and posted == []
