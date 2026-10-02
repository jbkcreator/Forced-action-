"""Tests for WP-GL-10 booking_webhook: cancel/reschedule and secret auth."""
from __future__ import annotations

import hashlib
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.lending.booking_webhook import router

# Build a minimal app just for this router
from fastapi import FastAPI

app = FastAPI()
app.include_router(router)
client = TestClient(app, raise_server_exceptions=False)

_SECRET = "test-booking-secret"


def _headers(secret: str = _SECRET) -> dict:
    return {"X-Webhook-Secret": secret}


def _patch_secret(secret: str = _SECRET):
    return patch("src.lending.booking_webhook._secret", return_value=secret)


class TestBookingWebhook:
    def test_closed_when_secret_not_configured(self):
        with patch("src.lending.booking_webhook._secret", return_value=None):
            resp = client.post("/webhooks/lending/booking", json={})
        assert resp.status_code == 405

    def test_unauthorized_when_wrong_secret(self):
        with _patch_secret():
            resp = client.post(
                "/webhooks/lending/booking",
                json={"appointmentId": "appt-1", "status": "cancelled"},
                headers={"X-Webhook-Secret": "wrong"},
            )
        assert resp.status_code == 401

    def test_cancels_rows_on_cancelled_status(self):
        db = MagicMock()
        db.execute.return_value.rowcount = 2
        with (
            _patch_secret(),
            patch("src.lending.booking_webhook.get_db", return_value=iter([db])),
            patch("src.lending.booking_messages.cancel_booking_messages",
                  return_value=2) as mock_cancel,
        ):
            resp = client.post(
                "/webhooks/lending/booking",
                json={"appointmentId": "appt-99", "appointmentStatus": "cancelled"},
                headers=_headers(),
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cancelled"
        assert data["rows_cancelled"] == 2

    def test_cancels_rows_on_rescheduled_status(self):
        db = MagicMock()
        with (
            _patch_secret(),
            patch("src.lending.booking_webhook.get_db", return_value=iter([db])),
            patch("src.lending.booking_messages.cancel_booking_messages",
                  return_value=1) as mock_cancel,
        ):
            resp = client.post(
                "/webhooks/lending/booking",
                json={"appointmentId": "appt-99", "status": "rescheduled"},
                headers=_headers(),
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "rescheduled"
        # db arg is the real SQLAlchemy session injected by get_db; just check booking_ref and reason
        assert mock_cancel.call_args[0][1] == "appt-99"
        assert mock_cancel.call_args[0][2] == "booking_rescheduled"

    def test_noop_for_no_appointment_id(self):
        with _patch_secret():
            resp = client.post(
                "/webhooks/lending/booking",
                json={"type": "ping"},
                headers=_headers(),
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "noop"

    def test_noop_for_unhandled_status(self):
        db = MagicMock()
        with (
            _patch_secret(),
            patch("src.lending.booking_webhook.get_db", return_value=iter([db])),
        ):
            resp = client.post(
                "/webhooks/lending/booking",
                json={"appointmentId": "appt-1", "status": "showed"},
                headers=_headers(),
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "noop"
