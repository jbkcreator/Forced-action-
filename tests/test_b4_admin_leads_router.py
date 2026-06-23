"""B4 — admin deliveries router (route, auth, validation, shape).

Data-correctness of filtering is covered by tests/scenarios/test_b4_admin_leads_e2e.py;
here we exercise the HTTP surface the frontend depends on.
"""

import pytest
from fastapi.testclient import TestClient

from src.api import deps
from src.api.admin_router import get_current_admin
from src.api.main import app
from src.core.database import get_db_context

_ENVELOPE_KEYS = {"total", "limit", "offset", "items"}


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


def test_requires_auth(client):
    assert client.get("/api/admin/deliveries").status_code in (401, 403)


def test_returns_envelope(auth_client):
    resp = auth_client.get("/api/admin/deliveries?limit=5")
    assert resp.status_code == 200
    body = resp.json()
    assert _ENVELOPE_KEYS <= set(body.keys())
    assert isinstance(body["items"], list)
    assert body["limit"] == 5 and body["offset"] == 0


def test_invalid_status_is_400(auth_client):
    assert auth_client.get("/api/admin/deliveries?status=bogus").status_code == 400


def test_invalid_account_id_is_400(auth_client):
    assert auth_client.get("/api/admin/deliveries?account_id=not-a-uuid").status_code == 400


def test_invalid_date_is_400(auth_client):
    assert auth_client.get("/api/admin/deliveries?from=nope").status_code == 400


def test_from_after_to_is_400(auth_client):
    resp = auth_client.get("/api/admin/deliveries?from=2099-07-01&to=2099-06-01")
    assert resp.status_code == 400


def test_limit_out_of_bounds_is_422(auth_client):
    # ge=1, le=200 enforced by the framework
    assert auth_client.get("/api/admin/deliveries?limit=0").status_code == 422
    assert auth_client.get("/api/admin/deliveries?limit=999").status_code == 422
