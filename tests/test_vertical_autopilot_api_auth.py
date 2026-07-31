"""Auth coverage for the 3 vertical-autopilot endpoints (REVINT-I4 review fix).

Data-correctness of the probe/verdict/presell-confirm flow is covered by
tests/test_revint.py's TestProbeLoop; this file only exercises the HTTP auth
surface, matching the pattern in tests/test_b4_admin_leads_router.py.
"""

import pytest
from fastapi.testclient import TestClient

from src.api import deps
from src.api.admin_router import get_current_admin
from src.api.main import app


def _override_db():
    from src.core.database import get_db_context
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


class TestVerticalProbeAuth:
    def test_anonymous_probe_rejected(self, client):
        resp = client.post("/api/vertical/probe", json={"vertical_candidate_packet_id": 1})
        assert resp.status_code in (401, 403)

    def test_anonymous_presell_confirm_rejected(self, client):
        resp = client.post("/api/vertical/presell-confirm", json={"verdict_id": 1})
        assert resp.status_code in (401, 403)

    def test_anonymous_verdict_read_rejected(self, client):
        resp = client.get("/api/vertical/verdict/1")
        assert resp.status_code in (401, 403)

    def test_authorized_probe_reaches_business_logic(self, auth_client):
        # Nonexistent packet id — proves the request cleared auth and reached
        # run_probe(), which then 422s on a real ValueError, not a 401/403.
        resp = auth_client.post("/api/vertical/probe", json={"vertical_candidate_packet_id": 999999})
        assert resp.status_code == 422

    def test_authorized_presell_confirm_reaches_business_logic(self, auth_client):
        resp = auth_client.post("/api/vertical/presell-confirm", json={"verdict_id": 999999})
        assert resp.status_code == 404

    def test_authorized_verdict_read_reaches_business_logic(self, auth_client):
        resp = auth_client.get("/api/vertical/verdict/999999")
        assert resp.status_code == 404
