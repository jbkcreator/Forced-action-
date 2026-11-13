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


@pytest.fixture
def client():
    from src.api.lending_instant_quote_router import router
    from src.services.rate_limit import reset_local_buckets

    reset_local_buckets()
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, headers={"x-forwarded-for": f"198.19.0.{next(_ip_counter)}"})


def test_returns_an_indicative_quote(enabled, client):
    resp = client.post(URL, json=BODY, headers={"X-Api-Key": KEY})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "indicative"
    assert body["loan_amount"] == "212000"
    assert body["rehab_funding"] == "50000"
    assert body["purchase_advance"] == "162000"
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
