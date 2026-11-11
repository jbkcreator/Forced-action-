"""LENDING_TEXT_WINDOW_TEST_PHONES: only listed team test numbers may be texted outside 8am-8pm; every other number
stays blocked, calls are never affected, and the default (empty) changes nothing."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.lending import compliance

TEST_PHONE = "+19177303812"      # team test number (Eastern area code)
BORROWER = "+19175550123"        # any other number
NINE_PM_ET = datetime(2026, 11, 11, 2, 0, tzinfo=timezone.utc)  # 21:00 EST on Nov 10


@pytest.fixture(autouse=True)
def _fresh_log_state():
    compliance.reset_hours_log_state()
    yield
    compliance.reset_hours_log_state()


def _allow(monkeypatch, value: str) -> None:
    fake = SimpleNamespace(lending_text_window_test_phones=value,
                           lending_call_window_local_start="08:00", lending_call_window_local_end="20:00",
                           lending_call_window_et_start="09:00", lending_call_window_et_end="19:15")
    monkeypatch.setattr("config.settings.get_settings", lambda: fake)


def test_a_listed_test_number_may_be_texted_at_9pm_and_it_is_logged(monkeypatch, caplog):
    _allow(monkeypatch, TEST_PHONE)
    with caplog.at_level("WARNING"):
        assert compliance.text_window_open(TEST_PHONE, now=NINE_PM_ET) is True
    assert "TEXT WINDOW TEST BYPASS" in caplog.text and "7303812" not in caplog.text


def test_every_other_number_is_still_blocked_at_9pm(monkeypatch, caplog):
    _allow(monkeypatch, TEST_PHONE)
    with caplog.at_level("INFO"):
        assert compliance.text_window_open(BORROWER, now=NINE_PM_ET) is False
    assert "BLOCKED_HOURS text refused" in caplog.text


def test_the_default_empty_setting_changes_nothing(monkeypatch):
    _allow(monkeypatch, "")
    assert compliance.text_window_open(TEST_PHONE, now=NINE_PM_ET) is False


def test_numbers_are_matched_in_any_format_and_junk_is_ignored(monkeypatch):
    _allow(monkeypatch, " not-a-phone , (917) 730-3812 ,, 123 ")
    assert compliance.text_window_test_phones() == frozenset({TEST_PHONE})
    assert compliance.text_window_open("917-730-3812", now=NINE_PM_ET) is True


def test_an_unreadable_setting_means_no_exceptions(monkeypatch):
    def broken():
        raise RuntimeError("settings unavailable")

    monkeypatch.setattr("config.settings.get_settings", broken)
    assert compliance.text_window_test_phones() == frozenset()
    assert compliance.text_window_open(TEST_PHONE, now=NINE_PM_ET) is False


def test_calls_to_a_listed_number_are_still_blocked_at_9pm(monkeypatch):
    _allow(monkeypatch, TEST_PHONE)
    result = compliance.can_dial_now(TEST_PHONE, db=None, now=NINE_PM_ET)
    assert result.allowed is False and result.reason == compliance.ReasonCode.OUTSIDE_CALL_WINDOW


def test_inside_the_window_nothing_changes(monkeypatch):
    _allow(monkeypatch, TEST_PHONE)
    noon_et = NINE_PM_ET - timedelta(hours=9)
    assert compliance.text_window_open(BORROWER, now=noon_et) is True
    assert compliance.text_window_open(TEST_PHONE, now=noon_et) is True
