"""Aircall contact writes used by the lending dialer load.

Unit tests: the HTTP layer, the clock and credentials are faked. The live
round trip against a real account is in test_aircall_contact_writes_live.py.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.services import aircall_client
from src.services.aircall_client import (
    AircallAmbiguousContact,
    AircallContactFields,
    AircallRequestError,
    ContactUpsertResult,
    upsert_contact,
)

PHONE = "+18135550100"
FIELDS = AircallContactFields(
    first_name="John", last_name="Smith", company_name="SMITH HOLDINGS LLC",
    information="Property: 123 Main St", email="john@smithbuilders.com",
)


def _response(status: int, body: dict | None = None, headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.headers = headers or {}
    resp.content = b"{}" if body is not None else b""
    resp.json.return_value = body or {}
    return resp


@pytest.fixture(autouse=True)
def no_waiting():
    """No real sleeping or pacing in unit tests; credentials are faked."""
    with patch.object(aircall_client.time, "sleep") as sleep, \
         patch.object(aircall_client._rate_limiter, "acquire"), \
         patch.object(aircall_client, "_auth", return_value=("id", "token")):
        yield sleep


def _patch_requests(*responses):
    return patch.object(aircall_client.requests, "request", side_effect=list(responses))


class TestUpsert:
    def test_creates_when_no_contact_holds_the_phone(self):
        with _patch_requests(_response(200, {"contacts": []}),
                             _response(201, {"contact": {"id": 42}})) as req:
            result = upsert_contact(PHONE, FIELDS)
        assert result == ContactUpsertResult(contact_id=42, created=True)
        search, create = req.call_args_list
        assert search.args[:2] == ("GET", f"{aircall_client._BASE_URL}/contacts/search")
        assert search.kwargs["params"] == {"phone_number": PHONE}
        assert create.args[0] == "POST"
        body = create.kwargs["json"]
        assert body["phone_numbers"] == [{"label": "Work", "value": PHONE}]
        assert body["emails"] == [{"label": "Work", "value": "john@smithbuilders.com"}]
        assert body["company_name"] == "SMITH HOLDINGS LLC"

    def test_updates_existing_contact_instead_of_duplicating(self):
        with _patch_requests(_response(200, {"contacts": [{"id": 7}]}),
                             _response(200, {"contact": {"id": 7}})) as req:
            result = upsert_contact(PHONE, FIELDS)
        assert result == ContactUpsertResult(contact_id=7, created=False)
        update = req.call_args_list[1]
        assert update.args[:2] == ("POST", f"{aircall_client._BASE_URL}/contacts/7")
        assert "phone_numbers" not in update.kwargs["json"]
        assert update.kwargs["json"]["information"] == "Property: 123 Main St"

    def test_several_contacts_on_one_phone_raise_instead_of_guessing(self):
        with _patch_requests(_response(200, {"contacts": [{"id": 1}, {"id": 2}]})) as req:
            with pytest.raises(AircallAmbiguousContact):
                upsert_contact(PHONE, FIELDS)
        assert req.call_count == 1

    def test_empty_fields_are_sent_blank_so_stale_values_are_cleared(self):
        body = AircallContactFields(first_name="Ann").update_body()
        assert body == {"first_name": "Ann", "last_name": "", "company_name": "", "information": ""}


class TestRetryPolicy:
    def test_429_is_retried_honouring_retry_after(self, no_waiting):
        with _patch_requests(_response(429, headers={"Retry-After": "3"}),
                             _response(200, {"contacts": []})) as req:
            assert aircall_client.find_contacts_by_phone(PHONE) == []
        assert req.call_count == 2
        no_waiting.assert_called_once_with(3.0)

    def test_429_is_retried_even_for_post(self):
        with _patch_requests(_response(429), _response(201, {"contact": {"id": 5}})) as req:
            assert aircall_client.create_contact(PHONE, FIELDS)["id"] == 5
        assert req.call_count == 2

    def test_5xx_is_retried_for_contact_update(self):
        with _patch_requests(_response(503), _response(200, {"contact": {"id": 7}})) as req:
            aircall_client.update_contact(7, FIELDS)
        assert req.call_count == 2

    def test_5xx_on_post_is_not_retried(self):
        with _patch_requests(_response(502)) as req:
            with pytest.raises(AircallRequestError) as err:
                aircall_client.create_contact(PHONE, FIELDS)
        assert err.value.status == 502
        assert req.call_count == 1

    def test_network_error_on_post_is_not_retried(self):
        with patch.object(aircall_client.requests, "request",
                          side_effect=requests.ConnectionError("reset")) as req:
            with pytest.raises(AircallRequestError) as err:
                aircall_client.create_contact(PHONE, FIELDS)
        assert err.value.status == 0
        assert req.call_count == 1

    def test_4xx_is_not_retried(self):
        with _patch_requests(_response(422)) as req:
            with pytest.raises(AircallRequestError):
                aircall_client.update_contact(7, FIELDS)
        assert req.call_count == 1

    def test_gives_up_after_max_attempts(self):
        responses = [_response(429)] * aircall_client.AIRCALL_MAX_ATTEMPTS
        with _patch_requests(*responses) as req:
            with pytest.raises(AircallRequestError) as err:
                aircall_client.find_contacts_by_phone(PHONE)
        assert err.value.status == 429
        assert req.call_count == aircall_client.AIRCALL_MAX_ATTEMPTS

    def test_retry_wait_is_capped(self):
        assert aircall_client._retry_wait(1, "86400") == aircall_client.AIRCALL_MAX_RETRY_WAIT_SECONDS
        assert aircall_client._retry_wait(10, None) == aircall_client.AIRCALL_MAX_RETRY_WAIT_SECONDS

    def test_error_message_holds_no_phone(self):
        with _patch_requests(_response(400)):
            with pytest.raises(AircallRequestError) as err:
                aircall_client.find_contacts_by_phone(PHONE)
        assert PHONE not in str(err.value)


class TestRateLimiter:
    def test_requests_are_spaced_to_the_per_minute_limit(self):
        limiter = aircall_client._RateLimiter(per_minute=120)
        clock = iter([100.0, 100.0, 100.0])
        with patch.object(aircall_client.time, "monotonic", side_effect=lambda: next(clock)), \
             patch.object(aircall_client.time, "sleep") as sleep:
            limiter.acquire()
            limiter.acquire()
            limiter.acquire()
        assert [c.args[0] for c in sleep.call_args_list] == [pytest.approx(0.5), pytest.approx(1.0)]
