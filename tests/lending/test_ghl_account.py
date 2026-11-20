"""Lending talks only to the Next Deal Lending GHL sub-account (LENDING_GHL_*), never to the
platform's GHL_* account, and does nothing at all until both LENDING_GHL_* values are set."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from config.lending_compliance import GHL_DND_CHANNELS
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


def _ghl_upsert_then_tags(monkeypatch, *, contact=None, tag_status=200, tag_raises=None):
    """Fake GHL: the upsert answers with ``contact``; the add-tags call answers ``tag_status`` or raises."""
    calls = []

    def fake(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        if url.endswith("/tags"):
            if tag_raises:
                raise tag_raises
            return MagicMock(status_code=tag_status)
        response = MagicMock(status_code=200)
        response.json.return_value = {"contact": contact} if contact else {}
        return response

    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    return calls


def test_dnd_upsert_never_sends_tags_because_ghl_upsert_replaces_the_tag_list(monkeypatch):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_upsert_then_tags(monkeypatch, contact={"id": "c9"})
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    upsert = calls[0]
    assert upsert["url"].endswith("/contacts/upsert") and "tags" not in upsert["json"]


def test_the_opt_out_tag_is_added_without_touching_the_contacts_other_tags(monkeypatch):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_upsert_then_tags(monkeypatch, contact={"id": "c9"})
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    add = calls[1]
    assert (add["method"], add["url"].rsplit("/", 3)[-3:]) == ("POST", ["contacts", "c9", "tags"])
    assert add["json"] == {"tags": ["lending-opt-out"]}
    assert add["headers"]["Authorization"] == f"Bearer {LENDING_KEY}"
    assert len(calls) == 2  # no remove-tags call, no tag-list overwrite


@pytest.mark.parametrize("kwargs", [{"tag_status": 500}, {"tag_raises": RuntimeError("down")}])
def test_a_failed_tag_call_does_not_undo_the_dnd(monkeypatch, caplog, kwargs):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    _ghl_upsert_then_tags(monkeypatch, contact={"id": "c9"}, **kwargs)
    with caplog.at_level("WARNING"):
        assert ghl_dnd.set_ghl_dnd(PHONE) is True
    assert "opt-out tag not added" in caplog.text and PHONE not in caplog.text


def test_dnd_without_a_contact_id_in_the_reply_still_counts_and_skips_the_tag(monkeypatch, caplog):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_upsert_then_tags(monkeypatch, contact=None)
    with caplog.at_level("WARNING"):
        assert ghl_dnd.set_ghl_dnd(PHONE) is True
    assert len(calls) == 1 and "no contact id" in caplog.text


@pytest.mark.parametrize("kwargs", [{"status": 400}, {"raises": RuntimeError("down")}])
def test_dnd_upsert_reports_failure_without_raising(monkeypatch, kwargs):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    _ghl_request(monkeypatch, **kwargs)
    assert ghl_dnd.set_ghl_dnd(PHONE) is False


def _ghl_refusing_permanent_channels(monkeypatch, permanent: set[str], refusal_status=401):
    """Fake GHL: an upsert that touches a permanently blocked channel is refused, naming that channel."""
    calls = []

    def fake(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        if url.endswith("/tags"):
            return MagicMock(status_code=200)
        blocked = sorted(permanent & set((kwargs["json"].get("dndSettings") or {})))
        response = MagicMock(status_code=refusal_status if blocked else 200)
        response.json.return_value = (
            {"statusCode": refusal_status, "message": f"Not authorized to update permanent dnd setting for {blocked[0]}"}
            if blocked else {"contact": {"id": "c9"}})
        return response

    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    return calls


def test_a_channel_ghl_blocked_permanently_is_left_out_and_the_rest_are_set(monkeypatch):
    """A STOP reply makes GHL's SMS DND permanent and the API may not touch it, which used to fail the whole
    upsert on every poll; the opt-out is already in force there, so the other channels are set instead."""
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_refusing_permanent_channels(monkeypatch, {"SMS"})
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    first, retry, tag = calls
    assert "SMS" in first["json"]["dndSettings"]
    assert "SMS" not in retry["json"]["dndSettings"] and retry["json"]["dnd"] is True
    assert set(retry["json"]["dndSettings"]) == set(GHL_DND_CHANNELS) - {"SMS"}
    assert tag["url"].endswith("/contacts/c9/tags")


def test_every_permanently_blocked_channel_is_left_out_one_refusal_at_a_time(monkeypatch):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_refusing_permanent_channels(monkeypatch, {"SMS", "Call"})
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    upserts = [call for call in calls if call["url"].endswith("/contacts/upsert")]
    assert len(upserts) == 3
    assert set(upserts[-1]["json"]["dndSettings"]) == set(GHL_DND_CHANNELS) - {"SMS", "Call"}


@pytest.mark.parametrize("body", [
    {"message": "Invalid JWT"},
    {"message": "Not authorized to update permanent dnd setting for Fax"},
])
def test_any_other_refusal_still_fails_without_retrying(monkeypatch, body):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = []

    def fake(method, url, **kwargs):
        calls.append(url)
        response = MagicMock(status_code=401)
        response.json.return_value = body
        return response

    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    assert ghl_dnd.set_ghl_dnd(PHONE) is False
    assert len(calls) == 1


@pytest.mark.parametrize("status,message", [
    (403, "Not authorized to update permanent dnd setting for SMS"),
    (401, "not authorized to update PERMANENT DND setting for sms"),
])
def test_a_permanent_refusal_is_read_from_a_403_and_in_any_case(monkeypatch, status, message):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = []

    def fake(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        if url.endswith("/tags"):
            return MagicMock(status_code=200)
        refuse = "SMS" in kwargs["json"]["dndSettings"]
        response = MagicMock(status_code=status if refuse else 200)
        response.json.return_value = {"message": message} if refuse else {"contact": {"id": "c9"}}
        return response

    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    assert "SMS" not in calls[1]["json"]["dndSettings"]


@pytest.mark.parametrize("status", [400, 422, 500])
def test_a_permanent_message_with_another_status_drops_nothing(monkeypatch, status):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_refusing_permanent_channels(monkeypatch, {"SMS"}, refusal_status=status)
    assert ghl_dnd.set_ghl_dnd(PHONE) is False
    assert len(calls) == 1


def _ghl_refusing_through_the_global_flag(monkeypatch, *, flag_free_refused=False):
    """Fake GHL where the global ``dnd`` flag also covers the permanently blocked SMS channel: an upsert with the
    flag or with SMS is refused naming SMS. With ``flag_free_refused`` every upsert is refused."""
    calls = []

    def fake(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        if url.endswith("/tags"):
            return MagicMock(status_code=200)
        body = kwargs["json"]
        refuse = flag_free_refused or body.get("dnd") or "SMS" in body["dndSettings"]
        response = MagicMock(status_code=401 if refuse else 200)
        response.json.return_value = (
            {"statusCode": 401, "message": "Not authorized to update permanent dnd setting for SMS"}
            if refuse else {"contact": {"id": "c9"}})
        return response

    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    return calls


def test_when_the_global_flag_covers_the_permanent_channel_each_channel_is_set_without_it(monkeypatch):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_refusing_through_the_global_flag(monkeypatch)
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    upserts = [call["json"] for call in calls if call["url"].endswith("/contacts/upsert")]
    assert len(upserts) == 3
    assert upserts[1]["dnd"] is True and "SMS" not in upserts[1]["dndSettings"]
    assert "dnd" not in upserts[2]
    assert set(upserts[2]["dndSettings"]) == set(GHL_DND_CHANNELS) - {"SMS"}
    assert all(s["status"] == "active" for s in upserts[2]["dndSettings"].values())
    assert calls[-1]["url"].endswith("/contacts/c9/tags")


def test_a_channel_refused_again_without_the_global_flag_fails_and_says_so(monkeypatch, caplog):
    _creds(monkeypatch, lending_key=LENDING_KEY, lending_location="lending-loc")
    calls = _ghl_refusing_through_the_global_flag(monkeypatch, flag_free_refused=True)
    with caplog.at_level("WARNING"):
        assert ghl_dnd.set_ghl_dnd(PHONE) is False
    assert len(calls) == 3  # all channels, then without SMS, then without the flag: never loops
    assert "refused sms again" in caplog.text and PHONE not in caplog.text


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
