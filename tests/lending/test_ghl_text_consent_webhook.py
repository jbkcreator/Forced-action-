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


@pytest.fixture(autouse=True)
def removed_from_dialer(monkeypatch):
    """Never reach the real dialer: record the removals instead."""
    removed = []
    monkeypatch.setattr("src.lending.compliance._default_dialer_remover",
                        lambda: lambda phone, reason=None: removed.append(phone))
    return removed


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


SOFT = [
    "cancel my appointment", "end of month works", "I want to quit", "wrong number", "Wrong number!", "please remove me",
    "take me off your list", "no more calls", "don't text me", "dont text me", "Do NOT text me again", "do not contact me",
    "leave me alone", "not interested", "Not interested, thanks",
]
HARD = ["STOP", "stop?", "Stop it", "please stop", "stopall", "UNSUBSCRIBE", "unsubscribe me", "opt out", "Opt-out please",
        "optout", "revoke", "cancel", "END", "quit"]


def _suppressed(db):
    return db.execute(text("SELECT count(*) FROM lending.suppression_list WHERE phone = :p"), {"p": PHONE}).scalar()


# Changed from the earlier rule: "cancel my appointment" and "end of month works" used to be recorded as consent.
# A cancel/end/quit word inside a longer reply is now a SOFT decline: consent is revoked, nothing is recorded.
@pytest.mark.parametrize("message", SOFT)
def test_a_soft_decline_revokes_consent_records_none_and_suppresses_nothing(client, lending_db, message):
    record_consent(lending_db, PHONE, "on_call_yes")
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": message})
    assert r.json() == {"recorded": False, "revoked": True}
    assert _sources(lending_db) == [] and not has_text_consent(lending_db, PHONE) and _suppressed(lending_db) == 0
    record_consent(lending_db, PHONE, "on_call_yes")  # not durable: a later grant works again
    assert has_text_consent(lending_db, PHONE)


@pytest.mark.parametrize("message", HARD)
def test_a_hard_opt_out_is_made_durable_in_the_suppression_list(client, lending_db, message):
    record_consent(lending_db, PHONE, "on_call_yes")
    r = client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": message, "contact_id": "ct9"})
    assert r.json() == {"recorded": False, "revoked": True}
    assert _suppressed(lending_db) == 1 and not has_text_consent(lending_db, PHONE)
    record_consent(lending_db, PHONE, "on_call_yes")  # a later answered call cannot re-grant it
    assert not has_text_consent(lending_db, PHONE)


def test_a_hard_opt_out_is_queued_for_the_dialer_and_the_ghl_dnd_sync_and_is_idempotent(client, lending_db, removed_from_dialer):
    for _ in range(2):  # a redelivered webhook writes nothing more
        client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": "STOP", "contact_id": "ct9"})
    rows = lending_db.execute(text("SELECT channel, ghl_dnd_at, status FROM lending.opt_out_events")).all()
    assert len(rows) == 1 and rows[0][0] == "sms" and rows[0][1] is None  # ghl_dnd_at unset: the sync will write the DND to GHL
    assert _suppressed(lending_db) == 1 and removed_from_dialer == [PHONE]


def test_a_soft_decline_does_not_touch_the_dialer(client, lending_db, removed_from_dialer):
    client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": "not interested"})
    assert removed_from_dialer == [] and lending_db.execute(text("SELECT count(*) FROM lending.opt_out_events")).scalar() == 0


def test_a_hard_opt_out_without_a_contact_id_still_suppresses(client, lending_db):
    client.post(URL, headers=HEADERS, json={"source": "inbound_text", "phone": PHONE, "message": "STOP"})
    assert _suppressed(lending_db) == 1


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


def test_an_unconfigured_secret_gives_a_generic_503(lending_db, monkeypatch):
    from fastapi import FastAPI
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.api.lending_ghl_router import router
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", None, raising=False)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: lending_db
    r = TestClient(app).post(URL, headers=HEADERS, json={"source": "web_form", "phone": PHONE, "consent": True})
    assert r.status_code == 503 and r.json() == {"detail": "GHL webhook is not configured"}


def test_the_secret_is_required(client, lending_db):
    assert client.post(URL, json={"source": "web_form", "phone": PHONE, "consent": True}).status_code == 401
    assert _sources(lending_db) == []
