"""WP-GL-11: POST /api/lending/web-leads, the endpoint the static page submits to."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from config.lending_web import SMS_CONSENT_TEXT
from src.lending.consent import has_text_consent

PHONE = "+18135550142"
URL = "/api/lending/web-leads"
FORM = {"name": "Dana Builder", "phone": "(813) 555-0142", "email": "dana@example.com",
        "property_city": "Tampa", "deal_type": "Fix and flip", "completed_projects_3y": "1 to 2"}


@pytest.fixture
def delivered(monkeypatch):
    ids: list[int] = []
    monkeypatch.setattr("src.api.lending_web_router.deliver_in_background", ids.append)
    return ids


@pytest.fixture
def client(web_leads_db, delivered):
    from src.api.deps import get_db
    from src.api.lending_web_router import router
    from src.services.rate_limit import reset_local_buckets

    reset_local_buckets()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: web_leads_db
    return TestClient(app)


def _count(db):
    return db.execute(text("SELECT count(*) FROM lending.web_leads")).scalar()


def test_a_submission_is_stored_and_queued_for_delivery(client, web_leads_db, delivered):
    response = client.post(URL, data=FORM)
    assert response.status_code == 200 and response.json() == {"received": True, "will_contact": False}  # box not ticked
    assert _count(web_leads_db) == 1 and len(delivered) == 1


def test_consent_is_unticked_by_default(client, web_leads_db):
    client.post(URL, data=FORM)
    assert web_leads_db.execute(text("SELECT sms_consent, deal_drop_optin FROM lending.web_leads")).one() == (False, False)
    assert has_text_consent(web_leads_db, PHONE) is False


def test_a_ticked_box_saves_the_value_with_its_evidence(client, web_leads_db):
    client.post(URL, data={**FORM, "sms_consent": "yes", "consent_text": SMS_CONSENT_TEXT,
                           "page_url": "https://nextdeallending.com/"},
                headers={"x-forwarded-for": "203.0.113.50, 10.0.0.1", "user-agent": "pytest-agent"})
    row = web_leads_db.execute(text("SELECT sms_consent, consent_text_matches, ip_address, page_url, user_agent FROM lending.web_leads")).one()
    assert row == (True, True, "203.0.113.50", "https://nextdeallending.com/", "pytest-agent")
    assert has_text_consent(web_leads_db, PHONE) is True


@pytest.mark.parametrize("overrides", [{"phone": "123"}, {"phone": ""}, {"name": ""}, {"email": "nope"}])
def test_an_unusable_submission_is_a_422_with_a_plain_message(client, web_leads_db, delivered, overrides):
    response = client.post(URL, data={**FORM, **overrides})
    assert response.status_code == 422 and isinstance(response.json()["detail"], str)
    assert _count(web_leads_db) == 0 and delivered == []


def test_a_filled_honeypot_is_dropped_silently(client, web_leads_db, delivered):
    response = client.post(URL, data={**FORM, "company_website": "http://spam.example"})
    assert response.status_code == 200 and _count(web_leads_db) == 0 and delivered == []


def test_a_double_submit_is_stored_and_delivered_once(client, web_leads_db, delivered):
    client.post(URL, data=FORM)
    client.post(URL, data=FORM)
    assert _count(web_leads_db) == 1 and len(delivered) == 1


def test_only_a_repeat_inside_the_window_is_flagged_as_a_duplicate(client):
    assert client.post(URL, data=FORM).json() == {"received": True, "will_contact": False}
    assert client.post(URL, data=FORM).json() == {"received": True, "duplicate": True}


def test_a_repeat_adds_a_missing_email_but_never_overwrites_or_touches_consent(client, web_leads_db, delivered):
    no_email = {k: v for k, v in FORM.items() if k != "email"}
    client.post(URL, data={**no_email, "sms_consent": "yes"})
    web_leads_db.execute(text("UPDATE lending.web_leads SET ghl_status = 'synced', ghl_attempts = 1"))
    response = client.post(URL, data={**FORM, "sms_consent": "yes", "property_city": "Orlando"})
    row = web_leads_db.execute(text("SELECT email, property_city, sms_consent, ghl_status, ghl_attempts FROM lending.web_leads")).one()
    assert response.json() == {"received": True, "duplicate": True}
    assert row == ("dana@example.com", "Tampa", True, "pending", 0)  # email added, city kept, re-queued
    assert len(delivered) == 2 and _count(web_leads_db) == 1


def test_a_repeat_with_nothing_new_is_not_requeued(client, web_leads_db, delivered):
    client.post(URL, data=FORM)
    web_leads_db.execute(text("UPDATE lending.web_leads SET ghl_status = 'synced', ghl_attempts = 1"))
    client.post(URL, data=FORM)
    assert web_leads_db.execute(text("SELECT ghl_status FROM lending.web_leads")).scalar() == "synced"
    assert len(delivered) == 1


def test_will_contact_is_true_only_for_a_ticked_box_on_a_clear_number(client, web_leads_db):
    clear_ticked = client.post(URL, data={**FORM, "sms_consent": "yes"}).json()
    clear_unticked = client.post(URL, data={**FORM, "phone": "(813) 555-0143"}).json()
    web_leads_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES ('+18135550144', 'test', 'manual')"))
    suppressed_ticked = client.post(URL, data={**FORM, "phone": "(813) 555-0144", "sms_consent": "yes"}).json()
    assert clear_ticked == {"received": True, "will_contact": True}
    assert clear_unticked == {"received": True, "will_contact": False}
    assert suppressed_ticked == {"received": True, "will_contact": False}


def test_a_filled_honeypot_answers_in_the_normal_shape_without_promising_contact(client):
    assert client.post(URL, data={**FORM, "sms_consent": "yes", "company_website": "x"}).json() == {"received": True, "will_contact": False}


def test_the_endpoint_is_rate_limited_per_ip(client, monkeypatch):
    monkeypatch.setattr("src.api.lending_web_router.RATE_LIMIT_PER_WINDOW", 2)
    statuses = [client.post(URL, data={**FORM, "phone": f"(813) 555-01{n:02d}"}).status_code for n in range(4)]
    assert statuses == [200, 200, 429, 429]


def test_a_save_failure_is_a_generic_500_that_leaks_nothing(client, monkeypatch, delivered):
    def boom(*_args, **_kwargs):
        raise RuntimeError("relation lending.web_leads missing +18135550142")

    monkeypatch.setattr("src.api.lending_web_router.save_web_lead", boom)
    response = client.post(URL, data=FORM)
    assert response.status_code == 500 and "web_leads" not in response.text and "8135550142" not in response.text
    assert delivered == []


def test_the_router_is_mounted_on_the_app():
    from src.api.main import app

    # openapi() resolves nested routers, which app.routes does not in newer FastAPI versions
    assert "post" in app.openapi()["paths"].get(URL, {})
