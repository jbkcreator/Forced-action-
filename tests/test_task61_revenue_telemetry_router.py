"""Task 6.1 — revenue telemetry router tests (route, auth, validation, shape).

Margin correctness is covered by tests/scenarios/test_task61_revenue_telemetry_e2e.py;
here we exercise the HTTP surface. DB is read-only over a far-future window (for the
windowed endpoint) so it returns a clean empty/zero result regardless of shared-DB
contents. zip-territory-margin is a current-state snapshot with no window param.
"""

import pytest
from fastapi.testclient import TestClient

from src.api import deps
from src.api.admin_router import get_current_admin
from src.api.main import app
from src.core.database import get_db_context


def _override_db():
    with get_db_context() as s:
        yield s


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def auth_client():
    app.dependency_overrides[get_current_admin] = lambda: {"sub": "test-admin"}
    app.dependency_overrides[deps.get_db] = _override_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_current_admin, None)
        app.dependency_overrides.pop(deps.get_db, None)


def test_confirmed_delivery_margin_requires_auth(client):
    resp = client.get("/api/revenue/confirmed-delivery-margin")
    assert resp.status_code in (401, 403)


def test_confirmed_delivery_margin_returns_list_shape(auth_client):
    resp = auth_client.get("/api/revenue/confirmed-delivery-margin?from=2099-06-01&to=2099-07-01")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_confirmed_delivery_margin_defaults_to_last_30_days(auth_client):
    resp = auth_client.get("/api/revenue/confirmed-delivery-margin")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_confirmed_delivery_margin_invalid_date_is_400(auth_client):
    resp = auth_client.get("/api/revenue/confirmed-delivery-margin?from=not-a-date")
    assert resp.status_code == 400


def test_confirmed_delivery_margin_from_after_to_is_400(auth_client):
    resp = auth_client.get("/api/revenue/confirmed-delivery-margin?from=2099-07-01&to=2099-06-01")
    assert resp.status_code == 400


def test_zip_territory_margin_requires_auth(client):
    resp = client.get("/api/revenue/zip-territory-margin")
    assert resp.status_code in (401, 403)


def test_zip_territory_margin_returns_list_shape(auth_client):
    resp = auth_client.get("/api/revenue/zip-territory-margin")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_zip_territory_margin_leads_requires_auth(client):
    resp = client.get("/api/revenue/zip-territory-margin/1/leads")
    assert resp.status_code in (401, 403)


def test_zip_territory_margin_leads_returns_list_shape(auth_client):
    resp = auth_client.get("/api/revenue/zip-territory-margin/999999999/leads")
    assert resp.status_code == 200
    assert resp.json() == []


def test_zip_territory_margin_leads_non_int_subscriber_id_is_422(auth_client):
    resp = auth_client.get("/api/revenue/zip-territory-margin/not-an-int/leads")
    assert resp.status_code == 422
