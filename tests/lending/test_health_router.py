"""R1 0.10: lending-api reports the deployed commit SHA on /health (internal) and /api/lending/health (public)."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.lending.api import app
from src.utils.build_info import UNKNOWN, BuildInfo

SHA = "e1636938" + "0" * 32


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr("src.api.health_router.build_info", lambda: BuildInfo(SHA, "apply_x"))
    return TestClient(app)


def test_public_health_returns_commit_sha_and_migration_head(client):
    r = client.get("/api/lending/health")
    assert (r.status_code, r.json()) == (200, {"status": "ok", "commit_sha": SHA, "migration_head": "apply_x"})


def test_internal_health_answers_the_same(client):
    assert client.get("/health").json() == client.get("/api/lending/health").json()


def test_unknown_build_info_still_answers_ok(monkeypatch):
    monkeypatch.setattr("src.api.health_router.build_info", lambda: BuildInfo(UNKNOWN, UNKNOWN))
    r = TestClient(app).get("/api/lending/health")
    assert (r.status_code, r.json()["status"], r.json()["commit_sha"]) == (200, "ok", UNKNOWN)


def test_fa_api_does_not_serve_the_lending_health_route():
    from src.api.main import app as fa_app
    assert TestClient(fa_app).get("/api/lending/health").status_code == 404
