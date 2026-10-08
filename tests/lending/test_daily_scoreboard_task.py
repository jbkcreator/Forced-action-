"""Entry point of the 7pm scoreboard post. Database-free: the build, session and Slack client are stubbed."""
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from src.tasks import lending_daily_scoreboard as task

NOON_ET = datetime(2026, 10, 6, 16, 0, tzinfo=timezone.utc)
AFTER_MIDNIGHT_UTC = datetime(2026, 10, 7, 0, 30, tzinfo=timezone.utc)  # 20:30 ET on Oct 6
SEVEN_PM_UTC = datetime(2026, 10, 6, 23, 20, tzinfo=timezone.utc)  # 19:20 ET on Oct 6


@pytest.fixture
def wired(monkeypatch):
    posted, days = [], []

    class FakeClient:
        def __init__(self, token):
            pass

        def chat_postMessage(self, **kw):
            posted.append(kw)

    @contextmanager
    def fake_session():
        yield object()

    monkeypatch.setattr(task, "get_settings", lambda: SimpleNamespace(
        lending_daily_channel="C1", lending_dial_tasks_channel="CALLS", lending_slack_bot_token=SecretStr("xoxb-t")))
    monkeypatch.setattr(task.time, "sleep", lambda s: None)
    monkeypatch.setattr(task, "lending_session", fake_session)
    monkeypatch.setattr(task, "campaign_names", lambda: {})
    monkeypatch.setattr(task, "build_scoreboard", lambda db, day, names: days.append(day) or "data")
    monkeypatch.setattr(task, "format_slack", lambda data, day: f"board {day}")
    monkeypatch.setattr(task, "format_blocks", lambda data, day: [{"type": "divider"}])
    monkeypatch.setattr("slack_sdk.WebClient", FakeClient)
    return posted, days


def test_outside_the_7pm_hour_does_nothing(wired):
    posted, _ = wired
    assert task.main([], now=NOON_ET) == 0 and posted == []


def test_at_7_20pm_et_it_posts_once(wired):
    posted, days = wired
    assert task.main([], now=SEVEN_PM_UTC) == 0
    assert [p["text"] for p in posted] == ["board 2026-10-06"] and posted[0]["channel"] == "C1"
    assert posted[0]["blocks"] == [{"type": "divider"}]


def test_rendered_scoreboard_has_aligned_tables_and_a_totals_grid():
    from datetime import date

    from src.lending.scoreboard import NumberRow, Row, ScoreboardData, format_blocks, format_slack

    maria, josh = Row("Maria", 21, 7, 4, 2, 2, 1), Row("Josh `the` closer with a very long display name", 16, 5, 2, 1, 0)
    data = ScoreboardData([maria, josh], [maria], [], Row("TOTAL", 37, 12, 6, 3, 2, 1), [NumberRow("+18135550100", 37, 20)])
    blocks = format_blocks(data, date(2026, 10, 5))
    assert blocks[0]["type"] == "header" and "Mon Oct 5, 2026" in blocks[0]["text"]["text"]
    grid = next(b for b in blocks if b["type"] == "section" and "fields" in b)["fields"]
    assert [f["text"] for f in grid][:2] == ["*Dials*\n37", "*Live*\n12"] and "*Showed*\n1" in [f["text"] for f in grid]
    tables = [b["text"]["text"] for b in blocks if b["type"] == "section" and "text" in b]
    assert len(tables) == 4 and "_no dials_" in tables[2]
    assert "+18135550100" in tables[3] and "54%" in tables[3]  # caller-ID answer rate table
    rows = tables[0].split("```")[1].strip().splitlines()
    assert len({len(r) for r in rows if not r.startswith("-")}) == 1  # every row padded to the same width
    assert "`the`" not in tables[0] and "…" in tables[0]
    assert "Showed" in format_slack(data, date(2026, 10, 5))


def test_force_uses_the_eastern_date_not_the_utc_date(wired):
    posted, days = wired
    assert task.main(["--force"], now=AFTER_MIDNIGHT_UTC) == 0
    assert days[0].isoformat() == "2026-10-06" and len(posted) == 1


def test_unconfigured_channel_returns_1_and_posts_nothing(wired, monkeypatch):
    posted, _ = wired
    monkeypatch.setattr(task, "get_settings", lambda: SimpleNamespace(
        lending_daily_channel="", lending_dial_tasks_channel="CALLS", lending_slack_bot_token=SecretStr("xoxb-t")))
    assert task.main(["--force"], now=NOON_ET) == 1 and posted == []


def test_slack_failure_returns_1_and_logs(wired, monkeypatch, caplog):
    class Boom:
        def __init__(self, token):
            pass

        def chat_postMessage(self, **kw):
            raise RuntimeError("slack down")

    monkeypatch.setattr("slack_sdk.WebClient", Boom)
    assert task.main(["--force"], now=NOON_ET) == 1
    assert "scoreboard post failed for 2026-10-06" in caplog.text and "xoxb" not in caplog.text


def test_build_failure_returns_1(wired, monkeypatch):
    def boom(*a):
        raise RuntimeError("db down")

    monkeypatch.setattr(task, "build_scoreboard", boom)
    assert task.main(["--force"], now=NOON_ET) == 1


@pytest.mark.parametrize("http", [lambda *a, **k: 1 / 0, lambda *a, **k: [{"nope": 1}, "x"], lambda *a, **k: {"items": 5}])
def test_campaign_names_falls_back_to_empty(monkeypatch, http):
    monkeypatch.setattr(task, "get_http", lambda: http)
    assert task.campaign_names() == {}


def test_campaign_names_when_get_http_itself_raises(monkeypatch):
    def boom():
        raise RuntimeError("x")

    monkeypatch.setattr(task, "get_http", boom)
    assert task.campaign_names() == {}


def test_slack_failing_twice_then_succeeding_posts_once(wired, monkeypatch):
    posted, _ = wired
    calls = []

    class Flaky:
        def __init__(self, token):
            pass

        def chat_postMessage(self, **kw):
            calls.append(1)
            if len(calls) < 3:
                raise RuntimeError("slack down")
            posted.append(kw)

    monkeypatch.setattr("slack_sdk.WebClient", Flaky)
    assert task.main(["--force"], now=NOON_ET) == 0 and len(calls) == 3 and len(posted) == 1


def test_slack_failing_every_attempt_stops_at_three(wired, monkeypatch):
    calls = []

    class Down:
        def __init__(self, token):
            pass

        def chat_postMessage(self, **kw):
            calls.append(1)
            raise RuntimeError("slack down")

    monkeypatch.setattr("slack_sdk.WebClient", Down)
    assert task.main(["--force"], now=NOON_ET) == 1 and len(calls) == task.SLACK_ATTEMPTS


def test_client_approved_scoreboard_definitions_are_pinned():
    from config.lending_dispositions import LIVE_CONVERSATION_CODES, NURTURE_SENT_CODES

    assert "DNC_REQUEST" in LIVE_CONVERSATION_CODES
    assert NURTURE_SENT_CODES == {"GATE_FAILED_NURTURE"}
