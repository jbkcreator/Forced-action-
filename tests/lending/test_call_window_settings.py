"""Dialer calling window from env (LENDING_CALL_WINDOW_*): defaults equal the shipped window, values can only
narrow the legal 08:00-20:00 recipient-local floor, and texts keep their fixed window."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from config import lending_compliance as cfg
from src.lending import compliance

NY = "+19175550123"        # Eastern area code
LA = "+13105550123"        # Pacific area code
# 2026-11-05 is EST (UTC-5).


def _at_et(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, 5, 5, 0, tzinfo=timezone.utc) + timedelta(hours=hour, minutes=minute)


def _use_window(monkeypatch, local=("08:00", "20:00"), et=("09:00", "19:15")) -> None:
    fake = SimpleNamespace(lending_call_window_local_start=local[0], lending_call_window_local_end=local[1],
                           lending_call_window_et_start=et[0], lending_call_window_et_end=et[1])
    monkeypatch.setattr("config.settings.get_settings", lambda: fake)


# --- settings validation -------------------------------------------------------------------------------------

def test_defaults_equal_the_shipped_window(monkeypatch):
    from config.settings import AppSettings

    for name in ("LOCAL_START", "LOCAL_END", "ET_START", "ET_END"):
        monkeypatch.delenv(f"LENDING_CALL_WINDOW_{name}", raising=False)
    s = AppSettings()
    assert (s.lending_call_window_local_start, s.lending_call_window_local_end) == (
        cfg.CALL_WINDOW_START.strftime("%H:%M"), cfg.CALL_WINDOW_END.strftime("%H:%M"))
    assert (s.lending_call_window_et_start, s.lending_call_window_et_end) == (
        cfg.ET_WINDOW_START.strftime("%H:%M"), cfg.ET_WINDOW_END.strftime("%H:%M"))


@pytest.mark.parametrize("env, value", [
    ("LENDING_CALL_WINDOW_LOCAL_START", "07:59"),   # earlier than the legal floor
    ("LENDING_CALL_WINDOW_LOCAL_END", "20:01"),     # later than the legal floor
    ("LENDING_CALL_WINDOW_ET_START", "06:00"),
    ("LENDING_CALL_WINDOW_ET_END", "21:00"),
    ("LENDING_CALL_WINDOW_ET_START", "20:00"),      # start after end
    ("LENDING_CALL_WINDOW_LOCAL_START", "9am"),     # not HH:MM
    ("LENDING_CALL_WINDOW_ET_END", "25:00"),
])
def test_unsafe_or_malformed_values_are_refused_at_startup(monkeypatch, env, value):
    from config.settings import AppSettings

    monkeypatch.setenv(env, value)
    with pytest.raises(ValidationError, match="LENDING_CALL_WINDOW"):
        AppSettings()


def test_a_narrower_window_and_single_digit_hours_are_accepted(monkeypatch):
    from config.settings import AppSettings

    monkeypatch.setenv("LENDING_CALL_WINDOW_LOCAL_START", "9:00")
    monkeypatch.setenv("LENDING_CALL_WINDOW_ET_END", "18:00")
    s = AppSettings()
    assert (s.lending_call_window_local_start, s.lending_call_window_et_end) == ("09:00", "18:00")


# --- the dial gate ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("hour, minute, outside", [(8, 59, True), (9, 0, False), (19, 14, False), (19, 15, True)])
def test_defaults_keep_the_shipped_eastern_edges(monkeypatch, hour, minute, outside):
    _use_window(monkeypatch)
    assert compliance._outside_call_window(NY, _at_et(hour, minute)) is outside


def test_env_et_window_moves_the_edges(monkeypatch):
    _use_window(monkeypatch, et=("10:00", "17:00"))
    assert compliance._outside_call_window(NY, _at_et(9, 30)) is True
    assert compliance._outside_call_window(NY, _at_et(10, 0)) is False
    assert compliance._outside_call_window(NY, _at_et(17, 0)) is True


def test_local_window_still_applies_to_other_timezones(monkeypatch):
    _use_window(monkeypatch, et=("08:00", "20:00"))
    assert compliance._outside_call_window(LA, _at_et(10, 0)) is True     # 07:00 in Los Angeles
    assert compliance._outside_call_window(LA, _at_et(11, 0)) is False    # 08:00 in Los Angeles


def test_the_legal_floor_holds_even_if_validation_is_bypassed(monkeypatch):
    _use_window(monkeypatch, local=("06:00", "23:00"), et=("06:00", "23:00"))
    window = compliance.dial_window()
    assert (window.local_start, window.local_end) == (cfg.CALL_WINDOW_START, cfg.CALL_WINDOW_END)
    assert compliance._outside_call_window(NY, _at_et(7, 30)) is True
    assert compliance._outside_call_window(NY, _at_et(20, 30)) is True


def test_unreadable_settings_fall_back_to_the_shipped_window(monkeypatch):
    _use_window(monkeypatch, local=("garbage", "20:00"))
    window = compliance.dial_window()
    assert window == compliance.DialWindow(cfg.CALL_WINDOW_START, cfg.CALL_WINDOW_END,
                                           cfg.ET_WINDOW_START, cfg.ET_WINDOW_END)


def test_a_shift_group_never_reaches_outside_the_et_window(monkeypatch):
    _use_window(monkeypatch, et=("10:00", "19:15"))                     # group A is 09:00-15:00
    assert compliance._outside_call_window(NY, _at_et(9, 30), seat_group="A") is True
    assert compliance._outside_call_window(NY, _at_et(10, 30), seat_group="A") is False
    assert compliance._outside_call_window(NY, _at_et(15, 30), seat_group="A") is True


def test_texts_keep_the_fixed_local_window(monkeypatch):
    _use_window(monkeypatch, local=("10:00", "18:00"))
    assert compliance.text_window_open(NY, now=_at_et(8, 30)) is True
    assert compliance.text_window_open(NY, now=_at_et(19, 30)) is True
    assert compliance.text_window_open(NY, now=_at_et(20, 30)) is False


def test_shipped_constants_are_unchanged():
    assert (cfg.CALL_WINDOW_START, cfg.CALL_WINDOW_END) == (time(8, 0), time(20, 0))
    assert (cfg.ET_WINDOW_START, cfg.ET_WINDOW_END) == (time(9, 0), time(19, 15))
