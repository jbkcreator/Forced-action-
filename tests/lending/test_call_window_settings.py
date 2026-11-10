"""Dialer calling window from env (LENDING_CALL_WINDOW_*): defaults equal the shipped window, a wider window is
clamped back to the legal 08:00-20:00 floor (P0.1) and logged, and texts keep their fixed window."""
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
    ("LENDING_CALL_WINDOW_ET_START", "20:00"),      # start after end
    ("LENDING_CALL_WINDOW_LOCAL_START", "20:00"),   # start equal to end
    ("LENDING_CALL_WINDOW_LOCAL_START", "9am"),     # not HH:MM
    ("LENDING_CALL_WINDOW_ET_END", "25:00"),
])
def test_malformed_or_reversed_values_are_refused_at_startup(monkeypatch, env, value):
    from config.settings import AppSettings

    monkeypatch.setenv(env, value)
    with pytest.raises(ValidationError, match="LENDING_CALL_WINDOW"):
        AppSettings()


def test_a_wider_window_is_accepted_for_testing(monkeypatch):
    from config.settings import AppSettings

    monkeypatch.setenv("LENDING_CALL_WINDOW_LOCAL_START", "00:00")
    monkeypatch.setenv("LENDING_CALL_WINDOW_ET_START", "00:00")
    monkeypatch.setenv("LENDING_CALL_WINDOW_LOCAL_END", "23:59")
    s = AppSettings()
    assert (s.lending_call_window_local_start, s.lending_call_window_et_start) == ("00:00", "00:00")


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


def test_a_wider_env_window_is_clamped_to_8_to_8_and_logged_as_error(monkeypatch, caplog):
    _use_window(monkeypatch, local=("00:00", "23:59"), et=("00:00", "23:59"))
    with caplog.at_level("ERROR"):
        window = compliance.dial_window()
    assert window == compliance.DialWindow(time(8, 0), time(20, 0), time(8, 0), time(20, 0))
    assert "wider than the legal" in caplog.text


@pytest.mark.parametrize("hour, minute", [(1, 30), (7, 59), (20, 0), (21, 0), (23, 30)])
def test_a_wider_env_window_never_opens_calling_out_of_hours(monkeypatch, hour, minute):
    _use_window(monkeypatch, local=("00:00", "23:59"), et=("00:00", "23:59"))
    assert compliance._outside_call_window(NY, _at_et(hour, minute)) is True


def test_a_wider_env_window_still_allows_8_to_8(monkeypatch):
    _use_window(monkeypatch, local=("00:00", "23:59"), et=("00:00", "23:59"))
    assert compliance._outside_call_window(NY, _at_et(8, 0)) is False
    assert compliance._outside_call_window(NY, _at_et(19, 59)) is False


def test_a_window_entirely_after_8pm_blocks_every_call(monkeypatch):
    _use_window(monkeypatch, local=("21:00", "23:00"), et=("21:00", "23:00"))
    for hour in (8, 12, 19, 21, 22):
        assert compliance._outside_call_window(NY, _at_et(hour)) is True


def test_the_shipped_window_logs_no_error(monkeypatch, caplog):
    _use_window(monkeypatch)
    with caplog.at_level("WARNING"):
        compliance.dial_window()
    assert "wider than the legal" not in caplog.text


def test_9pm_call_is_refused_and_logged_blocked_hours(monkeypatch, caplog):
    _use_window(monkeypatch, local=("00:00", "23:59"), et=("00:00", "23:59"))
    with caplog.at_level("INFO"):
        result = compliance.can_dial_now(NY, db=None, now=_at_et(21, 0))
    assert result.allowed is False
    assert result.reason == compliance.ReasonCode.OUTSIDE_CALL_WINDOW
    assert "BLOCKED_HOURS call refused" in caplog.text
    assert "9175550123" not in caplog.text


def test_9pm_text_is_refused_and_logged_blocked_hours(monkeypatch, caplog):
    _use_window(monkeypatch, local=("00:00", "23:59"), et=("00:00", "23:59"))
    with caplog.at_level("INFO"):
        assert compliance.text_window_open(NY, now=_at_et(21, 0)) is False
    assert "BLOCKED_HOURS text refused" in caplog.text
    assert "9175550123" not in caplog.text


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
