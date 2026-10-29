"""A GHL opt-out or consent webhook that fails is alerted, saved and replayed, so no opt-out is lost.

No database: the save, the session and the handlers are scripted.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
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

    def __init__(self, rows, revoked_since: bool = False):
        self.rows = rows
        self.revoked_since = revoked_since  # answer to "was this number's consent revoked after the failure?"
        self.updates: list[dict] = []

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, statement, params=None):
        sql = str(statement)
        if "revoked_at >=" in sql:
            return SimpleNamespace(scalar=lambda: self.revoked_since)
        if sql.lstrip().startswith("SELECT"):
            return SimpleNamespace(all=lambda: self.rows)
        self.updates.append({"sql": sql, **(params or {})})
        return SimpleNamespace()


FAILED_AT = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)


def _row(failure_id=1, endpoint=GHL_TEXT_CONSENT, attempts=1, payload=None):
    return SimpleNamespace(id=failure_id, endpoint=endpoint, payload=dict(payload or BODY), ghl_contact_id="ghl-c-42",
                           attempts=attempts, created_at=FAILED_AT)


def _sweep(monkeypatch, rows, handler, revoked_since: bool = False):
    db = FakeDb(rows, revoked_since=revoked_since)

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


# ---- a replayed consent grant never overrides a newer refusal ------------------------------------------------

GRANT = {"source": "inbound_text", "contact_id": "ghl-c-42", "phone": PHONE, "message": "Yes, call me tomorrow"}


def test_a_failed_grant_is_not_replayed_after_a_newer_revocation(monkeypatch):
    """Saved grant, then a newer 'not interested' revokes consent, then the sweep: texting must stay blocked."""
    replayed: list[dict] = []
    result, db, alerts = _sweep(monkeypatch, [_row(payload=GRANT)], lambda db, body: replayed.append(body),
                                revoked_since=True)
    assert replayed == [] and result.superseded == 1 and result.resolved == 0 and alerts == []
    assert "superseded_by_newer_revocation" in db.updates[0]["sql"]


def test_a_failed_grant_is_replayed_when_nothing_revoked_it_since(monkeypatch):
    replayed: list[dict] = []
    result, _, _ = _sweep(monkeypatch, [_row(payload=GRANT)], lambda db, body: replayed.append(body),
                          revoked_since=False)
    assert replayed == [GRANT] and result.resolved == 1


@pytest.mark.parametrize("payload", [
    {"source": "inbound_text", "phone": PHONE, "message": "STOP"},
    {"source": "inbound_text", "phone": PHONE, "message": "not interested"},
])
def test_an_opt_out_or_decline_is_always_replayed(monkeypatch, payload):
    replayed: list[dict] = []
    result, _, _ = _sweep(monkeypatch, [_row(payload=payload)], lambda db, body: replayed.append(body),
                          revoked_since=True)
    assert replayed == [payload] and result.resolved == 1


def test_granted_phone_matches_what_the_handler_would_grant():
    from src.lending.ghl_consent_events import granted_phone

    assert granted_phone(GRANT) == PHONE
    assert granted_phone({**GRANT, "message": "STOP"}) is None
    assert granted_phone({**GRANT, "message": "please don't text me"}) is None
    assert granted_phone({"source": "web_form", "phone": PHONE, "consent": "checked"}) == PHONE
    assert granted_phone({"source": "web_form", "phone": PHONE, "consent": ""}) is None


# ── Cora's inbox ─────────────────────────────────────────────────────────────

def test_a_failure_given_up_is_raised_in_coras_inbox_without_the_phone(monkeypatch, inbox_writes):
    def still_down(db, body):
        raise DatabaseDown()

    _sweep(monkeypatch, [_row(failure_id=7, attempts=webhook_failures.MAX_RETRY_ATTEMPTS - 1)], still_down)
    assert len(inbox_writes) == 1
    event = inbox_writes[0]
    assert (event["event_type"], event["dedup_key"], event["target_channel"]) == ("consent_webhook_gave_up", "7", "ops_notice")
    assert "ghl-c-42" in event["payload"] and PHONE not in event["payload"]


def test_a_failure_still_retrying_raises_nothing(monkeypatch, inbox_writes):
    def still_down(db, body):
        raise DatabaseDown()

    _sweep(monkeypatch, [_row(attempts=1)], still_down)
    assert inbox_writes == []
