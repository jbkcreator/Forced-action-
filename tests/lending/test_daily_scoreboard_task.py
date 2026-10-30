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
EIGHT_PM_UTC_PLUS = datetime(2026, 10, 7, 0, 0, tzinfo=timezone.utc)  # 20:00 ET on Oct 6: after the last (19:00) hourly post
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
    monkeypatch.setattr(task, "build_scoreboard",
                        lambda db, day, names: days.append(day) or SimpleNamespace(total=SimpleNamespace(dials=1)))
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


@pytest.mark.parametrize("now", [TEN_AM_UTC, NOON_ET, WINTER_TEN_AM_UTC, SEVEN_PM_ET_ON_THE_HOUR])
def test_hourly_mode_posts_the_day_so_far_inside_the_hourly_window(wired, now):
    posted, _ = wired
    assert task.main(["--hourly"], now=now) == 0
    assert len(posted) == 1 and posted[0]["text"].endswith(" hourly")


@pytest.mark.parametrize("now", [NINE_AM_UTC, EIGHT_PM_UTC_PLUS])
def test_hourly_mode_is_silent_outside_the_hourly_window(wired, now):
    """The hourly window is 10:00 through 19:00 ET (client Q48); the 7:20pm closing board follows the 7:00pm one."""
    posted, _ = wired
    assert task.main(["--hourly"], now=now) == 0 and posted == []


def test_the_closing_board_is_not_marked_hourly_and_the_hourly_flag_does_not_move_it(wired):
    posted, _ = wired
    assert task.main([], now=SEVEN_PM_UTC) == 0
    assert posted[0]["text"] == "board 2026-10-06"
    assert task.main([], now=TEN_AM_UTC) == 0 and len(posted) == 1  # no flag: still only the 19:xx ET run acts


def test_closing_board_leads_with_headline_kpis_then_one_line_per_caller():
    from datetime import date

    from src.lending.scoreboard import NumberRow, Row, ScoreboardData, format_blocks, format_slack

    maria, josh = Row("Maria", 21, 7, 4, 2, 2, 1, 5), Row("Josh `the` closer with a very long display name and more", 16, 5, 2, 1, 0)
    idle = Row("Idle", 0, 0, 0, 0, 0)
    total = Row("TOTAL", 37, 12, 6, 3, 2, 1, 5)
    data = ScoreboardData([josh, idle, maria], [maria], [], total, [NumberRow("+18135550100", 37, 20)],
                          [Row("Seat 1", 20, 6, 3, 2, 1), Row("Seat 2", 17, 6, 3, 1, 1)], [Row("List 4", 37, 12, 6, 3, 2, 1, 5)])
    blocks = format_blocks(data, date(2026, 10, 5))
    assert blocks[0]["type"] == "header" and "Mon Oct 5, 2026" in blocks[0]["text"]["text"]
    assert not any("fields" in b for b in blocks)
    texts = [b["text"]["text"] for b in blocks if b["type"] == "section"]
    assert "*37* dials" in texts[0] and "*12* contacts (32% connect)" in texts[0] and "*3* booked (25%)" in texts[0]
    assert "1 showed (held ÷ booked 33%)" in blocks[3]["elements"][0]["text"]
    callers = texts[1].splitlines()[1:]
    assert callers[0].startswith("*Maria* — 21 dials · 7 contacts · 2 booked · 5 over 2 min")  # most booked first
    assert len(callers) == 2 and "Idle" not in texts[1]  # an all-zero row is dropped
    assert "`the`" not in texts[1] and "…" in texts[1]
    assert any(t.startswith("*By seat*") for t in texts)  # splits the day two ways
    assert not any(t.startswith(("*By source*", "*By campaign*", "*By hook")) for t in texts)  # one row or none
    assert "+18135550100" in texts[-1] and "54%" in texts[-1]
    assert "```" not in "".join(texts)
    assert "Showed" in format_slack(data, date(2026, 10, 5))


def test_hourly_board_stops_after_the_callers():
    from datetime import date

    from src.lending.scoreboard import NumberRow, Row, ScoreboardData, format_blocks

    row = Row("Maria", 21, 7, 4, 2, 2)
    data = ScoreboardData([row], [row, Row("Other", 1, 0, 0, 0, 0)], [], row, [NumberRow("+18135550100", 21, 7)])
    blocks = format_blocks(data, date(2026, 10, 5), as_of=datetime(2026, 10, 5, 11, 0))
    texts = [b["text"]["text"] for b in blocks if b["type"] == "section"]
    assert len(texts) == 2 and texts[1].startswith("*By caller*")


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


def test_an_hourly_board_with_no_dials_yet_is_not_posted(wired, monkeypatch):
    posted, _ = wired
    monkeypatch.setattr(task, "build_scoreboard", lambda db, day, names: SimpleNamespace(total=SimpleNamespace(dials=0)))
    assert task.main(["--hourly"], now=TEN_AM_UTC) == 0 and posted == []
    assert task.main([], now=SEVEN_PM_UTC) == 0 and len(posted) == 1  # the closing board still posts


def test_section_tables_leave_out_xfer_while_transfers_are_not_recorded():
    from datetime import date

    from src.lending.scoreboard import Row, ScoreboardData, TRANSFER_CODES, format_blocks

    assert not TRANSFER_CODES
    row = Row("Maria", 21, 7, 4, 2, 2, 1, 5)
    blocks = format_blocks(ScoreboardData([row], [row], [row], row, [], [row], [row]), date(2026, 10, 5))
    assert not any("transferred" in b["text"]["text"] for b in blocks if b["type"] == "section")
