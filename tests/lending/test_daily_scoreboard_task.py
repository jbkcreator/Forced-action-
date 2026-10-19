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
NINE_AM_UTC = datetime(2026, 10, 6, 13, 0, tzinfo=timezone.utc)  # 09:00 ET: before the first hourly post
TEN_AM_UTC = datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
SEVEN_PM_ET_ON_THE_HOUR = datetime(2026, 10, 6, 23, 0, tzinfo=timezone.utc)  # 19:00 ET
EIGHT_PM_UTC_PLUS = datetime(2026, 10, 7, 0, 0, tzinfo=timezone.utc)  # 20:00 ET on Oct 6: after the last hourly post
WINTER_TEN_AM_UTC = datetime(2026, 12, 1, 15, 0, tzinfo=timezone.utc)  # 10:00 ET in standard time (UTC-5)


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
    monkeypatch.setattr(task, "format_slack", lambda data, day, **kw: f"board {day}" + (" hourly" if kw.get("as_of") else ""))
    monkeypatch.setattr(task, "format_blocks", lambda data, day, **kw: [{"type": "divider"}])
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


@pytest.mark.parametrize("now", [TEN_AM_UTC, NOON_ET, SEVEN_PM_ET_ON_THE_HOUR, WINTER_TEN_AM_UTC])
def test_hourly_mode_posts_the_day_so_far_inside_the_hourly_window(wired, now):
    posted, _ = wired
    assert task.main(["--hourly"], now=now) == 0
    assert len(posted) == 1 and posted[0]["text"].endswith(" hourly")


@pytest.mark.parametrize("now", [NINE_AM_UTC, EIGHT_PM_UTC_PLUS])
def test_hourly_mode_is_silent_outside_the_hourly_window(wired, now):
    posted, _ = wired
    assert task.main(["--hourly"], now=now) == 0 and posted == []


def test_the_closing_board_is_not_marked_hourly_and_the_hourly_flag_does_not_move_it(wired):
    posted, _ = wired
    assert task.main([], now=SEVEN_PM_UTC) == 0
    assert posted[0]["text"] == "board 2026-10-06"
    assert task.main([], now=TEN_AM_UTC) == 0 and len(posted) == 1  # no flag: still only the 19:xx ET run acts


def test_rendered_scoreboard_has_aligned_tables_and_a_totals_grid():
    from datetime import date

    from src.lending.scoreboard import NumberRow, Row, ScoreboardData, format_blocks, format_slack

    maria, josh = Row("Maria", 21, 7, 4, 2, 2, 1, 5), Row("Josh `the` closer with a very long display name", 16, 5, 2, 1, 0)
    total = Row("TOTAL", 37, 12, 6, 3, 2, 1, 5)
    data = ScoreboardData([maria, josh], [maria], [], total, [NumberRow("+18135550100", 37, 20)],
                          [Row("Seat 1", 37, 12, 6, 3, 2, 1, 5)], [Row("List 4", 37, 12, 6, 3, 2, 1, 5)])
    blocks = format_blocks(data, date(2026, 10, 5))
    assert blocks[0]["type"] == "header" and "Mon Oct 5, 2026" in blocks[0]["text"]["text"]
    grids = [b["fields"] for b in blocks if b["type"] == "section" and "fields" in b]
    assert all(len(g) <= 10 for g in grids)  # Slack allows at most 10 fields in one section
    grid = [f["text"] for g in grids for f in g]
    assert grid[:2] == ["*Dials*\n37", "*Contacts*\n12"] and len(grid) == 11
    assert "*2+ min*\n5" in grid and "*Showed*\n1" in grid and "*Show rate*\n33%" in grid  # Showed 1 / Booked 3
    assert "*Transfers*\nn/a" in grid  # the dialer records no transfer yet
    tables = [b["text"]["text"] for b in blocks if b["type"] == "section" and "text" in b]
    assert len(tables) == 6 and "_no dials_" in tables[4]  # seat, source, caller, campaign, hook (empty), number
    assert "Seat 1" in tables[0] and "List 4" in tables[1]
    assert "+18135550100" in tables[5] and "54%" in tables[5]  # caller-ID answer rate table
    rows = tables[2].split("```")[1].strip().splitlines()
    assert len({len(r) for r in rows if not r.startswith("-")}) == 1  # every row padded to the same width
    assert "`the`" not in tables[2] and "…" in tables[2]
    assert "Showed" in format_slack(data, date(2026, 10, 5))


def test_hourly_board_says_it_is_the_day_so_far_and_the_closing_board_does_not():
    from datetime import date

    from src.lending.scoreboard import Row, ScoreboardData, format_blocks, format_slack

    data = ScoreboardData([], [], [], Row("TOTAL", 0, 0, 0, 0, 0))
    as_of = datetime(2026, 10, 6, 14, 5, tzinfo=timezone.utc).astimezone(__import__("zoneinfo").ZoneInfo("America/New_York"))
    hourly = format_blocks(data, date(2026, 10, 6), as_of=as_of)
    assert "as of 10:05 am ET" in hourly[1]["elements"][0]["text"]
    assert "as of 10:05 am ET" in format_slack(data, date(2026, 10, 6), as_of=as_of)
    assert "through 7:15pm ET" in format_blocks(data, date(2026, 10, 6))[1]["elements"][0]["text"]


def test_show_rate_is_showed_over_booked_and_blank_when_nothing_was_booked():
    from src.lending.scoreboard import Row

    assert Row("a", 10, 4, 2, 4, 0, 3).show_rate == 0.75
    assert Row("b", 10, 4, 2, 0, 0, 2).show_rate is None  # showed today, booked on an earlier day: no division error


def test_a_slack_totals_section_never_holds_more_than_ten_fields():
    from datetime import date

    from src.lending.scoreboard import Row, ScoreboardData, TOTALS_PER_SECTION, format_blocks

    assert TOTALS_PER_SECTION <= 10
    blocks = format_blocks(ScoreboardData([], [], [], Row("TOTAL", 1, 1, 1, 1, 1)), date(2026, 10, 6))
    assert all(len(b["fields"]) <= 10 for b in blocks if "fields" in b)


def test_every_scoreboard_table_fits_in_one_slack_section_even_at_the_row_limit():
    from datetime import date

    from src.lending.scoreboard import MAX_NAME_CHARS, MAX_TABLE_ROWS, Row, ScoreboardData, format_blocks

    rows = [Row("n" * MAX_NAME_CHARS, 9999, 9999, 9999, 9999, 9999, 9999, 9999, 9999) for _ in range(MAX_TABLE_ROWS + 5)]
    blocks = format_blocks(ScoreboardData(rows, rows, rows, Row("TOTAL", 1, 1, 1, 1, 1), [], rows, rows), date(2026, 10, 6))
    assert all(len(b["text"]["text"]) <= 3000 for b in blocks if b["type"] == "section" and "text" in b)
    assert len(blocks) <= 50  # Slack's block limit


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
