"""M11 / B3 — revenue metrics router tests (route, auth, validation, shape).

Metric correctness is covered by tests/scenarios/test_m11_revenue_metrics_e2e.py;
here we exercise the HTTP surface. DB is read-only over a far-future window so it
returns a clean zero bundle regardless of shared-DB contents.
"""

import pytest
from fastapi.testclient import TestClient

from src.api import deps
from src.api.admin_router import get_current_admin
from src.api.main import app
from src.core.database import get_db_context

_EXPECTED_KEYS = {
    "from", "to", "mrr_cents", "new_mrr_cents", "active_accounts",
    "leads_delivered_by_grade", "leads_delivered_by_account", "entitlement_utilization",
    "free_to_paid_rate", "avg_time_to_convert_days", "past_due_count", "at_risk_mrr_cents",
    "voluntary_churn_cents", "involuntary_churn_cents", "churned_count",
    "cost_per_record_cents", "revenue_per_lead_cents",
}


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
    # no bearer token → rejected (HTTPBearer → 403)
    resp = client.get("/api/revenue/metrics")
    assert resp.status_code in (401, 403)


def test_returns_full_metric_bundle(auth_client):
    resp = auth_client.get("/api/revenue/metrics?from=2099-06-01&to=2099-07-01")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == _EXPECTED_KEYS
    assert isinstance(body["mrr_cents"], int)
    assert isinstance(body["leads_delivered_by_grade"], dict)


def test_defaults_to_last_30_days(auth_client):
    resp = auth_client.get("/api/revenue/metrics")
    assert resp.status_code == 200
    assert "from" in resp.json() and "to" in resp.json()


def test_invalid_date_is_400(auth_client):
    resp = auth_client.get("/api/revenue/metrics?from=not-a-date")
    assert resp.status_code == 400


def test_from_after_to_is_400(auth_client):
    resp = auth_client.get("/api/revenue/metrics?from=2099-07-01&to=2099-06-01")
    assert resp.status_code == 400
