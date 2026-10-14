"""A GHL opt-out or consent webhook that fails is alerted, saved and replayed, so no opt-out is lost.

No database: the save, the session and the handlers are scripted.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.lending import webhook_failures
from src.lending.ghl_consent_events import InvalidEvent
from src.lending.webhook_failures import GHL_OPT_OUT, GHL_TEXT_CONSENT, report_webhook_failure

PHONE = "+18135559301"
BODY = {"contact_id": "ghl-c-42", "phone": PHONE, "message": "please stop texting me"}


class DatabaseDown(Exception):
    pass


def test_a_failure_is_saved_and_alerted_without_the_phone_or_message():
    saved: list[tuple] = []
    alerts: list[str] = []
    failure_id = report_webhook_failure(GHL_TEXT_CONSENT, BODY, DatabaseDown("conn refused 8135559301"),
                                        save=lambda *args: saved.append(args) or 17, notify=alerts.append)
    assert failure_id == 17 and saved == [(GHL_TEXT_CONSENT, BODY, "DatabaseDown")]
    assert "ghl-c-42" in alerts[0] and "#17" in alerts[0] and "retried automatically" in alerts[0]
    assert "8135559301" not in alerts[0] and "stop texting" not in alerts[0]


def test_when_the_save_also_fails_the_alert_says_it_is_not_recorded():
    def refuse(*args):
        raise DatabaseDown("no connection slots")

    alerts: list[str] = []
    assert report_webhook_failure(GHL_OPT_OUT, BODY, DatabaseDown(), save=refuse, notify=alerts.append) is None
    assert "NOT recorded" in alerts[0] and "re-run the GHL workflow" in alerts[0] and PHONE not in alerts[0]


class FakeDb:
    """Records statements; begin_nested rolls nothing back but lets exceptions through like a savepoint."""

    def __init__(self, rows):
        self.rows = rows
        self.updates: list[dict] = []

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, statement, params=None):
        sql = str(statement)
        if sql.lstrip().startswith("SELECT"):
            return SimpleNamespace(all=lambda: self.rows)
        self.updates.append({"sql": sql, **(params or {})})
        return SimpleNamespace()


def _row(failure_id=1, endpoint=GHL_TEXT_CONSENT, attempts=1):
    return SimpleNamespace(id=failure_id, endpoint=endpoint, payload=dict(BODY), ghl_contact_id="ghl-c-42",
                           attempts=attempts)


def _sweep(monkeypatch, rows, handler):
    db = FakeDb(rows)

    @contextmanager
    def session():
        yield db

    monkeypatch.setitem(webhook_failures.ENDPOINT_HANDLERS, GHL_TEXT_CONSENT, handler)
    alerts: list[str] = []
    result = webhook_failures.retry_pending(session_factory=session, notify=alerts.append)
    return result, db, alerts


def test_a_saved_failure_is_replayed_through_the_same_handler_and_resolved(monkeypatch):
    replayed: list[dict] = []
    result, db, alerts = _sweep(monkeypatch, [_row()], lambda db, body: replayed.append(body))
    assert replayed == [BODY] and result.resolved == 1 and alerts == []
    assert "resolved_at = now()" in db.updates[0]["sql"]


def test_a_failure_that_keeps_failing_is_retried_then_given_up_with_an_alert(monkeypatch):
    def still_down(db, body):
        raise DatabaseDown()

    result, db, alerts = _sweep(monkeypatch, [_row(attempts=1)], still_down)
    assert result.still_failing == 1 and alerts == [] and db.updates[0]["give_up"] is False

    last_try = webhook_failures.MAX_RETRY_ATTEMPTS - 1
    result, db, alerts = _sweep(monkeypatch, [_row(attempts=last_try)], still_down)
    assert result.gave_up == 1 and db.updates[0]["give_up"] is True
    assert "NOT recorded" in alerts[0] and "ghl-c-42" in alerts[0] and PHONE not in alerts[0]


def test_a_payload_that_can_never_work_is_given_up_at_once(monkeypatch):
    def invalid(db, body):
        raise InvalidEvent("phone or email is required")

    result, _, alerts = _sweep(monkeypatch, [_row(attempts=1)], invalid)
    assert result.gave_up == 1 and len(alerts) == 1


# ---- the endpoints report their failures -------------------------------------------------------------------

SECRET = "test-ghl-secret"


@pytest.fixture
def client(monkeypatch):
    from src.api import lending_ghl_router as router_module
    from src.api.deps import get_db

    monkeypatch.setattr(router_module, "get_settings",
                        lambda: SimpleNamespace(lending_ghl_webhook_secret=SecretStr(SECRET)))
    app = FastAPI()
    app.include_router(router_module.router)
    app.dependency_overrides[get_db] = lambda: SimpleNamespace(commit=lambda: None, rollback=lambda: None)
    return TestClient(app), router_module


@pytest.mark.parametrize("path, handler_name, endpoint", [
    ("/webhooks/lending/ghl-opt-out", "record_ghl_opt_out", GHL_OPT_OUT),
    ("/webhooks/lending/ghl-text-consent", "record_inbound_consent", GHL_TEXT_CONSENT),
])
def test_an_endpoint_that_fails_reports_the_failure_and_answers_500(client, monkeypatch, path, handler_name,
                                                                      endpoint):
    test_client, router_module = client

    def boom(db, body):
        raise DatabaseDown()

    reported: list[tuple] = []
    monkeypatch.setattr(router_module, handler_name, boom)
    monkeypatch.setattr(router_module, "report_webhook_failure", lambda *args: reported.append(args))
    response = test_client.post(path, json=BODY, headers={"X-Webhook-Secret": SECRET})
    assert response.status_code == 500 and reported[0][0] == endpoint and reported[0][1] == BODY


def test_an_invalid_payload_is_a_422_and_not_reported_as_a_failure(client, monkeypatch):
    test_client, router_module = client
    reported: list[tuple] = []
    monkeypatch.setattr(router_module, "report_webhook_failure", lambda *args: reported.append(args))
    response = test_client.post("/webhooks/lending/ghl-opt-out", json={"contact_id": "x"},
                                headers={"X-Webhook-Secret": SECRET})
    assert response.status_code == 422 and reported == []
