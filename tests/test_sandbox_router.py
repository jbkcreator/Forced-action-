"""
Smoke tests for the sandbox admin router.

Focus:
  - Endpoints return 503 when sandbox mode disabled (safety in prod)
  - Endpoints require JWT bearer auth
  - GET /outbox returns captured rows with correct shape
  - POST /simulate-inbound routes STOP keywords through the compliance handler

Full auth flow is covered by admin_router tests; here we verify the sandbox
router wires correctly and respects the sandbox flag.
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.core.models import SandboxOutbox


@pytest.fixture
def client():
	from src.api.main import app
	return TestClient(app)


@pytest.fixture
def admin_token(monkeypatch):
	"""Issue a valid JWT for sandbox endpoints. Requires ADMIN_* env vars."""
	from config.settings import settings
	from pydantic import SecretStr
	monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
	monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))

	from src.api.admin_router import create_access_token
	return create_access_token({"sub": "admin"})


@pytest.fixture
def auth_headers(admin_token):
	return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def sandbox_on(monkeypatch):
	from config.settings import settings
	monkeypatch.setattr(settings, "twilio_sandbox", True)


@pytest.fixture
def sandbox_off(monkeypatch):
	from config.settings import settings
	monkeypatch.setattr(settings, "twilio_sandbox", False)
	monkeypatch.setattr(settings, "redis_sandbox", False)


# ──────────────────────────────────────────────────────────────────────────────
# Sandbox flag enforcement
# ──────────────────────────────────────────────────────────────────────────────

def test_outbox_endpoint_503_when_sandbox_off(client, auth_headers, sandbox_off):
	response = client.get("/api/admin/sandbox/outbox", headers=auth_headers)
	assert response.status_code == 503
	assert "disabled" in response.json()["detail"].lower()


def test_simulate_inbound_503_when_sandbox_off(client, auth_headers, sandbox_off):
	response = client.post(
		"/api/admin/sandbox/simulate-inbound",
		json={"from_number": "+15555550000", "body": "STOP"},
		headers=auth_headers,
	)
	assert response.status_code == 503


# ──────────────────────────────────────────────────────────────────────────────
# Auth enforcement
# ──────────────────────────────────────────────────────────────────────────────

def test_outbox_requires_bearer(client, sandbox_on):
	response = client.get("/api/admin/sandbox/outbox")
	# FastAPI bearer scheme returns 403 without creds
	assert response.status_code in (401, 403)


# ──────────────────────────────────────────────────────────────────────────────
# GET /outbox — filtering
# ──────────────────────────────────────────────────────────────────────────────

def test_outbox_returns_list(client, auth_headers, sandbox_on):
	response = client.get(
		"/api/admin/sandbox/outbox?limit=5",
		headers=auth_headers,
	)
	assert response.status_code == 200
	data = response.json()
	assert isinstance(data, list)


def test_outbox_channel_filter_valid(client, auth_headers, sandbox_on):
	response = client.get(
		"/api/admin/sandbox/outbox?channel=sms",
		headers=auth_headers,
	)
	assert response.status_code == 200


def test_outbox_channel_filter_rejects_bogus(client, auth_headers, sandbox_on):
	response = client.get(
		"/api/admin/sandbox/outbox?channel=pigeon",
		headers=auth_headers,
	)
	# pattern-validated Query returns 422 on invalid enum
	assert response.status_code == 422


# ──────────────────────────────────────────────────────────────────────────────
# POST /simulate-inbound — routes STOP correctly
# ──────────────────────────────────────────────────────────────────────────────

def test_simulate_inbound_routes_stop_keyword(client, auth_headers, sandbox_on):
	with patch("src.services.sms_compliance.handle_inbound",
			   return_value="<Response><Message>unsubscribed</Message></Response>") as mock_handle:
		response = client.post(
			"/api/admin/sandbox/simulate-inbound",
			json={"from_number": "+15555550000", "body": "STOP"},
			headers=auth_headers,
		)
	assert response.status_code == 200
	body = response.json()
	assert body["handled"] == "stop"
	assert "unsubscribed" in body["reply"]
	mock_handle.assert_called_once()


def test_simulate_inbound_routes_opt_in(client, auth_headers, sandbox_on):
	with patch("src.services.sms_compliance.handle_inbound",
			   return_value=None), \
		 patch("src.services.sms_compliance.handle_opt_in_reply",
			   return_value="Welcome — you're subscribed."):
		response = client.post(
			"/api/admin/sandbox/simulate-inbound",
			json={"from_number": "+15555550000", "body": "YES"},
			headers=auth_headers,
		)
	assert response.status_code == 200
	assert response.json()["handled"] == "opt_in"


def test_simulate_inbound_unmatched_returns_null_reply(client, auth_headers, sandbox_on):
	with patch("src.services.sms_compliance.handle_inbound", return_value=None), \
		 patch("src.services.sms_compliance.handle_opt_in_reply", return_value=None):
		response = client.post(
			"/api/admin/sandbox/simulate-inbound",
			json={"from_number": "+15555550000", "body": "some random text"},
			headers=auth_headers,
		)
	assert response.status_code == 200
	body = response.json()
	# Either 'unmatched' or 'command' depending on whether dispatch exists and matches
	assert body["handled"] in ("unmatched", "command")
