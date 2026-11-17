"""GHL workflow webhook -> lending.ghl_stage_events (scoreboard "showed")."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
BODY = {"opportunity_id": "opp-1", "stage_name": "Held", "pipeline_id": "pipe-1", "phone": "(813) 555-0142", "booked_by": "Dana"}


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


def _rows(db):
    return db.execute(text("SELECT stage_key, phone, booked_by, pipeline_id FROM lending.ghl_stage_events")).all()


def test_records_the_stage_with_a_normalized_phone(client, lending_db):
    r = client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=BODY)
    assert r.status_code == 200 and r.json() == {"recorded": True}
    assert _rows(lending_db) == [("held", "+18135550142", "Dana", "pipe-1")]


def test_an_unresolved_booked_by_or_phone_merge_field_is_stored_as_null(client, lending_db):
    r = client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json={**BODY, "booked_by": "null", "phone": "undefined"})
    assert r.status_code == 200
    assert _rows(lending_db) == [("held", None, None, "pipe-1")]


def test_a_ghl_key_value_body_under_custom_data_is_recorded(client, lending_db):
    """A GHL webhook action sends its key/value rows under customData, next to GHL's own contact fields."""
    body = {"contact_id": "c-1", "first_name": "Test", "phone": "(813) 555-0142",
            "customData": {"opportunity_id": "opp-2", "stage_name": "Funded", "pipeline_id": "pipe-1",
                           "phone": "(813) 555-0142", "monetary_value": "1500"}}
    r = client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=body)
    assert r.status_code == 200 and r.json() == {"recorded": True}
    assert _rows(lending_db) == [("funded", "+18135550142", None, "pipe-1")]
    raw = lending_db.execute(text("SELECT raw_event -> 'customData' ->> 'monetary_value' FROM lending.ghl_stage_events")).scalar()
    assert raw == "1500"


def test_a_redelivered_webhook_is_not_counted_twice(client, lending_db):
    client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=BODY)
    r = client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=BODY)
    assert r.json() == {"recorded": False} and len(_rows(lending_db)) == 1


def test_wrong_or_missing_secret_is_rejected(client, lending_db):
    assert client.post("/webhooks/lending/ghl-stage", json=BODY).status_code == 401
    assert client.post("/webhooks/lending/ghl-stage", headers={"X-Webhook-Secret": "nope"}, json=BODY).status_code == 401
    assert _rows(lending_db) == []


def test_missing_stage_or_opportunity_is_422(client):
    assert client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json={"opportunity_id": "x"}).status_code == 422
    assert client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json={"stage_name": "Held"}).status_code == 422


@pytest.mark.parametrize("body", [
    {"opportunity_id": "opp-9", "stage_name": "null"},
    {"opportunity_id": "opp-9", "stage_name": " None "},
    {"opportunity_id": "undefined", "stage_name": "Funded"},
    {"customData": {"opportunity_id": "opp-9", "stage_name": "null"}},
])
def test_a_merge_field_that_did_not_resolve_is_refused_not_saved(client, lending_db, body):
    r = client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=body)
    assert r.status_code == 422 and "did not resolve" in r.json()["detail"]
    assert _rows(lending_db) == []


# ---- P2.4 current stage (lending.ghl_opportunity_stage) -------------------------------------------------------------

def _current(db, opp="opp-9"):
    return db.execute(text("SELECT stage_key FROM lending.ghl_opportunity_stage WHERE ghl_opportunity_id = :o"),
                      {"o": opp}).scalar()


def _stage(client, stage, opp="opp-9"):
    response = client.post("/webhooks/lending/ghl-stage", headers=HEADERS,
                           json={"opportunity_id": opp, "stage_name": stage, "phone": "(813) 555-0199"})
    assert response.status_code == 200
    return response.json()["recorded"]


def test_a_late_redelivery_never_reopens_a_funded_deal(client, lending_db):
    assert _stage(client, "Application") and _stage(client, "Funded")
    assert _stage(client, "Application") is False      # GHL redelivers the earlier event after Funded
    assert _current(lending_db) == "funded"


def test_a_repeated_event_never_reopens_a_lost_deal(client, lending_db):
    _stage(client, "Submitted to lender"), _stage(client, "Lost")
    _stage(client, "Submitted to lender")
    assert _current(lending_db) == "lost"


def test_a_first_time_stage_after_funded_still_applies(client, lending_db):
    _stage(client, "Funded")
    assert _stage(client, "Application") is True        # never seen before for this deal: a real move
    assert _current(lending_db) == "application"


def _age_events(db, hours=2, opp="opp-9"):
    """The deal's earlier deliveries, and its entry into its current stage, happened ``hours`` ago."""
    db.execute(text("UPDATE lending.ghl_stage_events SET event_at = now() - make_interval(hours => :h) "
                    "WHERE ghl_opportunity_id = :o"), {"h": hours, "o": opp})
    db.execute(text("UPDATE lending.ghl_opportunity_stage SET entered_at = now() - make_interval(hours => :h) "
                    "WHERE ghl_opportunity_id = :o"), {"h": hours, "o": opp})


def _held_items(db):
    return db.execute(text("SELECT summary FROM lending.cora_inbox WHERE event_type = 'ghl_stage_repeat_held'")
                      ).scalars().all()


def test_a_redelivery_after_a_revival_never_reopens_a_funded_deal(client, lending_db):
    """Application (day 1), Lost, revived to Application days later, Funded, then the revival redelivered."""
    _stage(client, "Application"), _stage(client, "Lost")
    _age_events(lending_db, hours=72)
    _stage(client, "Application")                     # the revival: applied
    assert _current(lending_db) == "application"
    _stage(client, "Funded")
    _stage(client, "Application")                     # GHL redelivers the revival minutes after Funded
    assert _current(lending_db) == "funded"


def test_a_held_repeat_raises_one_inbox_item_naming_both_stages(client, lending_db):
    _stage(client, "Application"), _stage(client, "Lost")
    _stage(client, "Application"), _stage(client, "Application")
    assert _current(lending_db) == "lost"
    items = _held_items(lending_db)
    assert len(items) == 1 and '"Application"' in items[0] and '"Lost"' in items[0] and "opp-9" in items[0]


def test_a_redelivery_within_the_hour_never_moves_an_active_deal_back(client, lending_db):
    _stage(client, "Application"), _stage(client, "Submitted to lender")
    _stage(client, "Application")                     # GHL redelivers the earlier event minutes later
    assert _current(lending_db) == "submitted to lender"


def test_a_repeated_stage_later_than_the_hour_is_a_real_move(client, lending_db):
    _stage(client, "Application"), _stage(client, "Submitted to lender")
    _age_events(lending_db)
    _stage(client, "Application")                     # the deal really went back
    assert _current(lending_db) == "application"


def test_a_lost_deal_revived_to_a_stage_it_was_in_before_is_reopened(client, lending_db):
    _stage(client, "Application"), _stage(client, "Lost")
    _age_events(lending_db, hours=48)
    assert _stage(client, "Application") is False     # a repeat in ghl_stage_events...
    assert _current(lending_db) == "application"      # ...but days later: Josh revived it


def test_a_redelivered_funded_keeps_funded(client, lending_db):
    _stage(client, "Funded"), _stage(client, "Funded")
    assert _current(lending_db) == "funded"
