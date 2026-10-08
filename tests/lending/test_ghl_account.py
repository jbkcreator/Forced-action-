"""Lending talks only to the Next Deal Lending GHL sub-account (LENDING_GHL_*), never to the
platform's GHL_* account, and does nothing at all until both LENDING_GHL_* values are set."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from config.settings import get_settings
from src.lending import ghl_account, ghl_dnd, ghl_dnd_backstop

PLATFORM_KEY, LENDING_KEY = "platform-key", "lending-key"
PHONE = "+18135559902"


@pytest.fixture(autouse=True)
def real_ghl_dnd(monkeypatch):
    """conftest stubs get_ghl_dnd for every lending test; these tests cover the real one."""
    monkeypatch.undo()


def _creds(monkeypatch, *, lending_key=None, lending_location=None):
    """The platform account is always configured, as in prod; only LENDING_GHL_* varies."""
    settings = get_settings()
    monkeypatch.setattr(settings, "ghl_api_key", SecretStr(PLATFORM_KEY), raising=False)
    monkeypatch.setattr(settings, "ghl_location_id", "platform-loc", raising=False)
    monkeypatch.setattr(settings, "lending_ghl_api_key", SecretStr(lending_key) if lending_key else None, raising=False)
    monkeypatch.setattr(settings, "lending_ghl_location_id", lending_location, raising=False)
    monkeypatch.setattr(ghl_account, "_warned_partial", False)


def _ghl_request(monkeypatch, status=200, raises=None):
    calls = []

    def fake(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        if raises:
            raise raises
        response = MagicMock(status_code=status)
        response.json.return_value = {"contacts": [{"id": "c1", "phone": PHONE}]}
        return response

    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    return calls


def test_the_platform_account_alone_is_never_used(monkeypatch):
    _creds(monkeypatch)
    assert ghl_account.lending_ghl_account() is None
    assert ghl_dnd.get_ghl_dnd() is None


def test_both_lending_values_select_the_next_deal_lending_account(monkeypatch):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    assert ghl_account.lending_ghl_account() == ghl_account.GhlAccount(LENDING_KEY, "lending-loc")
    assert ghl_dnd.get_ghl_dnd() is ghl_dnd.set_ghl_dnd


@pytest.mark.parametrize("key,location,missing", [
    (LENDING_KEY, None, "LENDING_GHL_LOCATION_ID"),
    (None, "lending-loc", "LENDING_GHL_API_KEY"),
])
def test_a_half_set_pair_is_unusable_and_says_which_value_is_missing(monkeypatch, caplog, key, location, missing):
    _creds(monkeypatch, lending_key=key, lending_location=location)
    with caplog.at_level("ERROR"):
        assert ghl_account.lending_ghl_account() is None
        assert ghl_account.lending_ghl_account() is None
    assert caplog.text.count(missing) == 1  # logged once, not on every call
    assert ghl_dnd.get_ghl_dnd() is None


def test_dnd_upsert_uses_the_lending_credentials_only(monkeypatch):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_request(monkeypatch)
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    (call,) = calls
    assert call["url"].endswith("/contacts/upsert")
    assert call["headers"]["Authorization"] == f"Bearer {LENDING_KEY}"
    assert call["json"]["locationId"] == "lending-loc" and call["json"]["phone"] == PHONE and call["json"]["dnd"] is True
    assert PLATFORM_KEY not in str(call) and "platform-loc" not in str(call)


@pytest.mark.parametrize("kwargs", [{"status": 400}, {"raises": RuntimeError("down")}])
def test_dnd_upsert_reports_failure_without_raising(monkeypatch, kwargs):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    _ghl_request(monkeypatch, **kwargs)
    assert ghl_dnd.set_ghl_dnd(PHONE) is False


def test_dnd_upsert_without_the_lending_account_makes_no_request(monkeypatch):
    _creds(monkeypatch)
    calls = _ghl_request(monkeypatch)
    assert ghl_dnd.set_ghl_dnd(PHONE) is False and calls == []


def test_backstop_fetch_reads_the_lending_account_only(monkeypatch):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_request(monkeypatch)
    assert ghl_dnd_backstop._ghl_fetch_dnd_page(1) == [{"id": "c1", "phone": PHONE}]
    (call,) = calls
    assert call["url"].endswith("/contacts/search")
    assert call["headers"]["Authorization"] == f"Bearer {LENDING_KEY}"
    assert call["json"]["locationId"] == "lending-loc"
    assert PLATFORM_KEY not in str(call) and "platform-loc" not in str(call)


def test_backstop_fetch_without_the_lending_account_makes_no_request(monkeypatch):
    _creds(monkeypatch)
    calls = _ghl_request(monkeypatch)
    with pytest.raises(RuntimeError, match="LENDING_GHL_API_KEY"):
        ghl_dnd_backstop._ghl_fetch_dnd_page(1)
    assert calls == []


def test_backstop_main_without_the_lending_account_does_nothing(monkeypatch, caplog):
    _creds(monkeypatch)

    def _must_not_run(*args, **kwargs):
        raise AssertionError("the backstop touched the DB or GHL without a lending account")

    monkeypatch.setattr("src.core.database.get_db_context", _must_not_run)
    monkeypatch.setattr(ghl_dnd_backstop, "_ghl_fetch_dnd_page", _must_not_run)
    with caplog.at_level("ERROR"):
        ghl_dnd_backstop.main()
    assert "LENDING_GHL_API_KEY / LENDING_GHL_LOCATION_ID not set" in caplog.text
