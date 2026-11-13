"""Cora P2.2: POST /api/lending/instant-quote (keyed, rate limited, off by default). No database."""
from __future__ import annotations

import itertools

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from config.settings import get_settings

URL = "/api/lending/instant-quote"
KEY = "test-quote-key"
_ip_counter = itertools.count(1)
BODY = {
    "state": "FL", "zip_code": "33602", "borrower_type": "entity",
    "guarantors": [{"fico": 720, "completed_deals": 3, "exits": 3}],
    "purpose": "purchase", "purchase_price": "200000", "rehab": "50000", "arv": "400000",
}


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_instant_quote_enabled", True)
    monkeypatch.setattr(get_settings(), "lending_instant_quote_api_key", SecretStr(KEY))


class FakeQuoteStore:
    def __init__(self, fail: bool = False) -> None:
        self.records = []
        self.fail = fail

    def save(self, record) -> str:
        if self.fail:
            raise RuntimeError("db down")
        self.records.append(record)
        return f"q-{len(self.records)}"


@pytest.fixture
def store():
    return FakeQuoteStore()


@pytest.fixture
def client(store):
    from src.api.lending_instant_quote_router import get_quote_store, router
    from src.services.rate_limit import reset_local_buckets

    reset_local_buckets()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_quote_store] = lambda: store
    return TestClient(app, headers={"x-forwarded-for": f"198.19.0.{next(_ip_counter)}"})


def test_returns_an_indicative_quote(enabled, client):
    resp = client.post(URL, json=BODY, headers={"X-Api-Key": KEY})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "indicative"
    assert body["loan_amount"] == "193000"
    assert body["rehab_funding"] == "50000"
    assert body["purchase_advance"] == "143000"
    assert body["disclaimer"] == "Indicative, subject to underwriting"
    assert body["unconfirmed"] is True
    assert body["quote_id"]


def test_never_names_a_lender(enabled, client):
    text = client.post(URL, json=BODY, headers={"X-Api-Key": KEY}).text.lower()

    assert "central" not in text
    assert "backflip" not in text


def test_a_rejected_quote_names_no_lender_and_carries_no_figures(enabled, client):
    small = {**BODY, "purchase_price": "50000", "rehab": "10000", "arv": "120000"}
    resp = client.post(URL, json=small, headers={"X-Api-Key": KEY})

    body = resp.json()
    assert resp.status_code == 200
    assert body["status"] == "rejected"
    assert body["reason"] == "LOAN_TOO_SMALL"
    assert body["loan_amount"] is None
    assert "central" not in resp.text.lower() and "backflip" not in resp.text.lower()


def test_refinance_without_existing_debt_is_quoted(enabled, client):
    body = {**BODY, "purpose": "refinance"}

    assert client.post(URL, json=body, headers={"X-Api-Key": KEY}).json()["status"] == "indicative"


def test_switched_off_endpoint_reveals_nothing_even_for_a_bad_body(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_instant_quote_enabled", False)

    assert client.post(URL, json={"junk": 1}, headers={"X-Api-Key": KEY}).status_code == 404


def test_keyless_caller_gets_401_even_for_a_bad_body(enabled, client):
    assert client.post(URL, json={"junk": 1}).status_code == 401


def test_off_by_default_is_not_found(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_instant_quote_enabled", False)

    assert client.post(URL, json=BODY, headers={"X-Api-Key": KEY}).status_code == 404


@pytest.mark.parametrize("headers", [{}, {"X-Api-Key": "wrong"}])
def test_missing_or_wrong_key_is_unauthorized(enabled, client, headers):
    assert client.post(URL, json=BODY, headers=headers).status_code == 401


def test_unset_server_key_refuses_every_caller(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_instant_quote_enabled", True)
    monkeypatch.setattr(get_settings(), "lending_instant_quote_api_key", None)

    assert client.post(URL, json=BODY, headers={"X-Api-Key": ""}).status_code == 401


@pytest.mark.parametrize("change", [
    {"purchase_price": "0"}, {"arv": "-1"}, {"guarantors": []}, {"purpose": "cash_out"},
    {"borrower_type": "trust"}, {"guarantors": [{"fico": 200}]},
])
def test_invalid_input_is_rejected(enabled, client, change):
    assert client.post(URL, json={**BODY, **change}, headers={"X-Api-Key": KEY}).status_code == 422


def test_keyless_callers_get_401_not_429(enabled, client):
    codes = {client.post(URL, json=BODY).status_code for _ in range(35)}

    assert codes == {401}


def test_rate_limited_after_30_a_minute(enabled, client):
    codes = [client.post(URL, json=BODY, headers={"X-Api-Key": KEY}).status_code for _ in range(31)]

    assert codes[:30] == [200] * 30
    assert codes[30] == 429


# --- 02 audit trail ---

def test_every_quote_is_stored_and_its_id_is_the_quote_id(enabled, client, store):
    body = client.post(URL, json=BODY, headers={"X-Api-Key": KEY}).json()

    assert len(store.records) == 1
    record = store.records[0]
    assert body["quote_id"] == "q-1"
    assert record.status == "indicative"
    assert record.request["purchase_price"] == "200000"
    assert record.result["loan_amount"] == "193000"
    assert record.unconfirmed is True
    assert KEY not in record.api_key_label


def test_a_rejected_quote_is_stored_with_its_reason(enabled, client, store):
    client.post(URL, json={**BODY, "borrower_type": "individual"}, headers={"X-Api-Key": KEY})

    assert store.records[0].status == "rejected"
    assert store.records[0].reason == "NEEDS_LLC_FORMATION"
    assert store.records[0].rules_fired == ["NEEDS_LLC_FORMATION"]


def test_no_quote_is_returned_when_it_cannot_be_stored(enabled, store, client):
    store.fail = True
    resp = client.post(URL, json=BODY, headers={"X-Api-Key": KEY})

    assert resp.status_code == 500
    assert "db down" not in resp.text
