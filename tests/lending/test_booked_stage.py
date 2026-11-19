"""B1: Booked for David = GHL Booked stage entries; the scoreboard shows them next to the caller-reported Booked and
uses them for Held ÷ Booked. Pure: fake sessions."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date

from config.lending_dispositions import BOOKED_STAGE_KEYS
from src.lending import david_activity as da
from src.lending import scoreboard as sb
from src.lending.scoreboard import Row, ScoreboardData, format_blocks, format_slack

DAY = date(2026, 11, 3)


class _Db:
    def __init__(self, row=(4, 3), fail=False):
        self.row, self.fail, self.sql = row, fail, []

    @contextmanager
    def _nested(self):
        yield

    def begin_nested(self):
        return self._nested()

    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError("no table")
        self.sql.append((" ".join(str(sql).split()), params))
        row = self.row
        return type("R", (), {"one": lambda s: row, "scalar": lambda s: row[0]})()


def test_booked_stage_key_is_configured():
    assert BOOKED_STAGE_KEYS == frozenset({"booked"})


def test_david_booked_counts_booked_stage_entries_not_dispositions():
    db = _Db()
    assert da.stage_counts(db, DAY) == (4, 3)
    sql, params = db.sql[0]
    assert "call_dispositions" not in sql and sql.count("lending.ghl_stage_events") == 2
    assert params["booked"] == ["booked"] and params["held"] == ["held", "showed"]


def test_scoreboard_booked_stage_count_and_its_failure():
    db = _Db(row=(5,))
    assert sb.booked_stage_count(db, DAY) == 5
    sql, params = db.sql[0]
    assert "lending.ghl_stage_events" in sql and params["booked"] == ["booked"]
    assert sb.booked_stage_count(_Db(fail=True), DAY) is None


TOTAL = Row("TOTAL", 40, 12, 3, 3, 1, 1)  # dials, live, gated, booked (caller), nurture, showed


def test_totals_show_both_booked_counts_and_held_over_stage_booked():
    data = ScoreboardData([TOTAL], [], [], TOTAL, booked_stage=4)
    text = format_slack(data, DAY)
    assert "Booked (caller) 3" in text and "Booked (stage) 4" in text and "Held ÷ Booked (stage) 25%" in text
    secondary = format_blocks(data, DAY)[3]["elements"][0]["text"]
    assert "1 showed (held ÷ booked 25%)" in secondary and "4 booked (stage)" in secondary


def test_stage_count_unavailable_falls_back_to_caller_booked():
    data = ScoreboardData([TOTAL], [], [], TOTAL)
    text = format_slack(data, DAY)
    assert "Booked (stage) n/a" in text and "Held ÷ Booked (stage) 33%" not in text
    secondary = format_blocks(data, DAY)[3]["elements"][0]["text"]
    assert "1 showed (held ÷ booked 33%)" in secondary and "booked (stage)" not in secondary


def test_no_stage_bookings_shows_a_dash_not_a_division_error():
    text = format_slack(ScoreboardData([TOTAL], [], [], TOTAL, booked_stage=0), DAY)
    assert "Held ÷ Booked (stage) –" in text
