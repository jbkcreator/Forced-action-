"""The calendar mode reads its new env name and still honours the old one."""
from config.settings import AppSettings


def test_old_name_still_sets_the_mode(monkeypatch):
    monkeypatch.delenv("LENDING_CALENDAR_MODE", raising=False)
    monkeypatch.setenv("FA_MAX_CALENDAR_MODE", "live")
    assert AppSettings().lending_calendar_mode == "live"


def test_new_name_wins_over_old(monkeypatch):
    monkeypatch.setenv("FA_MAX_CALENDAR_MODE", "live")
    monkeypatch.setenv("LENDING_CALENDAR_MODE", "fake")
    assert AppSettings().lending_calendar_mode == "fake"
