"""RESPA fee-gate admin toggle — POST /api/admin/lanes/{lane_id}/fee-config.

- enabling without acknowledge_respa is refused (422) and touches nothing
- enabling with acknowledgement flips the flag and money becomes visible
- disabling needs no acknowledgement (turning fees OFF is always safe)
- unknown lane → 404
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from src.api.admin_router import get_current_admin


def _property(session) -> int:
    return session.execute(
        text("INSERT INTO properties (parcel_id, county_id, created_at, updated_at) "
             "VALUES (:pc,'hillsborough',NOW(),NOW()) RETURNING id"),
        {"pc": f"P-{uuid.uuid4().hex[:10]}"},
    ).scalar()


def _lane(session, property_id: int) -> str:
    return str(session.execute(
        text("""INSERT INTO lanes (property_id, lane_type, current_stage)
                VALUES (:p,'distressed-payoff','entered') RETURNING lane_id"""),
        {"p": property_id},
    ).scalar())


def _flag(session, lane_id: str) -> bool:
    return bool(session.execute(
        text("SELECT fee_config_flag FROM lanes WHERE lane_id = CAST(:l AS uuid)"),
        {"l": lane_id},
    ).scalar())


@pytest.fixture
def client(fresh_db):
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: (yield fresh_db)
    app.dependency_overrides[get_current_admin] = lambda: {"sub": "test-admin"}
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_current_admin, None)


def test_enable_without_ack_is_refused(fresh_db, client):
    lane_id = _lane(fresh_db, _property(fresh_db))

    resp = client.post(f"/api/admin/lanes/{lane_id}/fee-config", json={"enabled": True})

    assert resp.status_code == 422
    assert "RESPA" in resp.json()["detail"]
    assert _flag(fresh_db, lane_id) is False


def test_enable_with_ack_flips_flag(fresh_db, client):
    lane_id = _lane(fresh_db, _property(fresh_db))

    resp = client.post(
        f"/api/admin/lanes/{lane_id}/fee-config",
        json={"enabled": True, "acknowledge_respa": True},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["previous"] is False
    assert body["enabled"] is True
    assert "RESPA" in body["respa_warning"]
    assert _flag(fresh_db, lane_id) is True


def test_disable_needs_no_ack(fresh_db, client):
    lane_id = _lane(fresh_db, _property(fresh_db))
    fresh_db.execute(
        text("UPDATE lanes SET fee_config_flag = true WHERE lane_id = CAST(:l AS uuid)"),
        {"l": lane_id},
    )

    resp = client.post(f"/api/admin/lanes/{lane_id}/fee-config", json={"enabled": False})

    assert resp.status_code == 200
    assert resp.json()["previous"] is True
    assert _flag(fresh_db, lane_id) is False


def test_unknown_lane_404(client):
    resp = client.post(
        f"/api/admin/lanes/{uuid.uuid4()}/fee-config",
        json={"enabled": True, "acknowledge_respa": True},
    )
    assert resp.status_code == 404
