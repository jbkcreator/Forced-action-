"""GHL workflow webhook -> lending text consent (website form checkbox, inbound texts, STOP)."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from src.lending.consent import has_text_consent, record_consent

PHONE = "+18135559402"
SECRET = "test-ghl-secret"
URL = "/webhooks/lending/ghl-text-consent"
HEADERS = {"X-Webhook-Secret": SECRET}


@pytest.fixture
def client(lending_db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.api.lending_ghl_router import router
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: lending_db
    return TestClient(app)


def _sources(db):
    return [r[0] for r in db.execute(text("SELECT source FROM lending.text_consents WHERE phone = :p AND revoked_at IS NULL"), {"p": PHONE})]


@pytest.mark.parametrize("flag", [True, "true", "Yes", "1", "checked"])
def test_a_checked_web_form_box_records_consent(client, lending_db, flag):
    r = client.post(URL, headers=HEADERS, json={"source": "web_form", "phone": "(813) 555-9402", "consent": flag, "contact_id": "ct1"})
    assert r.status_code == 200 and r.json()["recorded"] is True
    assert _sources(lending_db) == ["web_form"] and has_text_consent(lending_db, PHONE)


@pytest.mark.parametrize("flag", [False, "false", "", "no", None, "unchecked"])
def test_an_unchecked_or_missing_box_records_nothing(client, lending_db, flag):
    r = client.post(URL, headers=HEADERS, json={"source": "web_form", "phone": PHONE, "consent": flag})
    assert r.status_code == 200 and r.json()["recorded"] is False and _sources(lending_db) == []


def test_an_inbound_text_is_consent_to_reply(client, lending_db):
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": "Yes call me tomorrow"})
    assert r.json()["recorded"] is True and _sources(lending_db) == ["inbound_text"]


@pytest.mark.parametrize("message", ["STOP", " stop ", "Stop.", "UNSUBSCRIBE", "cancel", "END", "Quit", "stopall"])
def test_a_stop_text_is_never_consent_and_revokes_what_exists(client, lending_db, message):
    record_consent(lending_db, PHONE, "on_call_yes")
    record_consent(lending_db, PHONE, "web_form")
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": message})
    assert r.json() == {"recorded": False, "revoked": True}
    assert _sources(lending_db) == [] and not has_text_consent(lending_db, PHONE)


@pytest.mark.parametrize("message", [
    "stop?", "stop,", '"STOP"', "STOP!!", "Stop it", "stop texting me", "please stop", "opt out",
    "Opt-out please", "revoke", "unsubscribe me", "please don't stop calling",
])
def test_an_opt_out_phrasing_revokes_even_inside_a_sentence(client, lending_db, message):
    record_consent(lending_db, PHONE, "web_form")
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": message})
    assert r.json() == {"recorded": False, "revoked": True}
    assert _sources(lending_db) == []


@pytest.mark.parametrize("message", ["cancel my appointment", "end of month works"])
def test_single_word_keywords_inside_a_sentence_are_normal_messages(client, lending_db, message):
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": message})
    assert r.json() == {"recorded": True, "revoked": False} and _sources(lending_db) == ["inbound_text"]


@pytest.mark.parametrize("extra", [{"message": ""}, {}, {"message": {"text": "stop"}}, {"message": ["yes"]}, {"message": "  ?! "}])
def test_an_empty_or_non_text_reply_records_nothing_and_revokes_nothing(client, lending_db, extra):
    record_consent(lending_db, PHONE, "web_form")
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, **extra})
    assert r.json() == {"recorded": False, "revoked": False}
    assert _sources(lending_db) == ["web_form"]


@pytest.mark.parametrize("body,code", [
    ({"source": "carrier_pigeon", "phone": PHONE}, 422),
    ({"source": "web_form", "consent": True}, 422),
    ({"source": "web_form", "phone": "nope", "consent": True}, 422),
])
def test_bad_requests_are_rejected(client, body, code):
    assert client.post(URL, headers=HEADERS, json=body).status_code == code


def test_the_secret_is_required(client, lending_db):
    assert client.post(URL, json={"source": "web_form", "phone": PHONE, "consent": True}).status_code == 401
    assert _sources(lending_db) == []
