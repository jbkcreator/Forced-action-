"""T-11: LendingFlow intake: parsing, dedupe, consent vault, GHL delivery, event, pre-qual hand-off, route."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from src.lending import lendingflow, lendingflow_events
from src.lending.contracts import LoanType
from src.lending.db import get_lending_db
from src.lending.lendingflow import (
    ParseError,
    _credit_band_min_fico,
    deliver_pending,
    parse_lendingflow,
    run_followups,
    save_lead,
)
from src.lending.lendingflow_webhook import router
from src.lending.web_leads import DeliveryError, PushResult

SECRET = "s3cret"
CERT_RAW = "  -----CERT-----\n  consent ✓ line two\t\n"
FIXTURE = Path(__file__).parent / "fixtures" / "lendingflow_fake.json"


def _payload(**overrides):
    """The Fake LendingFlow payload (D1); swap the fixture when David's real schema lands."""
    body = json.loads(FIXTURE.read_text(encoding="utf-8"))
    body.update(overrides)
    return body


class RecordingSink:
    def __init__(self, error=None):
        self.error, self.pushed = error, []

    def push(self, lead):
        self.pushed.append(dict(lead))
        if self.error:
            raise self.error
        return PushResult(contact_id="ghl_1", pipeline_card=False)


@pytest.fixture
def lf_db(lending_db):
    from migrations.apply_lending_lendingflow import apply_to
    from migrations.apply_lending_web_leads import apply_to as apply_web  # noqa: F401  (schema parity)

    apply_to(lending_db.get_bind())
    return lending_db


@pytest.fixture
def client(lf_db, monkeypatch):
    from src.lending import lendingflow_webhook as hook

    settings = hook.get_settings()
    monkeypatch.setattr(settings, "lending_lendingflow_enabled", True)
    monkeypatch.setattr(settings, "lending_lendingflow_webhook_secret", SecretStr(SECRET))
    scheduled = []
    monkeypatch.setattr(hook, "_deliver_in_background", lambda lead_id: scheduled.append(lead_id))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_lending_db] = lambda: lf_db
    c = TestClient(app)
    c.scheduled = scheduled
    return c


def _post(client, payload=None, *, raw=None, secret=SECRET):
    headers = {"X-Webhook-Secret": secret} if secret else {}
    data = raw if raw is not None else json.dumps(payload if payload is not None else _payload())
    return client.post("/api/lending/lendingflow", content=data, headers=headers)


def _count(db, table):
    return db.execute(text(f"SELECT count(*) FROM lending.{table}")).scalar_one()


# ------------------------------------------------------------------ parsing

def test_parse_normalizes_and_maps():
    parsed = parse_lendingflow(_payload())
    assert parsed.phone == "+17275550100" and parsed.email == "test.borrower@example.com"
    assert parsed.loan_type == "FIX_AND_FLIP" and parsed.property_state == "FL"
    assert parsed.credit_band_min_fico == 680 and parsed.loan_amount == 250000
    assert parsed.certificate.consented_at == datetime(2026, 10, 9, 14, 3, 22, tzinfo=timezone.utc)


@pytest.mark.parametrize("raw,expected", [("680-719", 680), ("720+", 720), ("Excellent", None), (None, None), ("", None)])
def test_credit_band_min_fico(raw, expected):
    assert _credit_band_min_fico(raw) == expected


@pytest.mark.parametrize("payload", [[], "x", {}, {"lead_id": "1"}, {"lead_id": "1", "phone": "12345"},
                                     ])
def test_parse_rejects_malformed(payload):
    with pytest.raises(ParseError):
        parse_lendingflow(payload)


@pytest.mark.parametrize("field,value", [("email", "nope"), ("loan_amount", "abc"), ("loan_amount", -5)])
def test_bad_optional_field_is_dropped_not_rejected(field, value):
    parsed = parse_lendingflow({"lead_id": "1", "phone": "7275550100", field: value})
    assert getattr(parsed, field) is None


def test_parse_error_never_echoes_values():
    with pytest.raises(ParseError) as exc:
        parse_lendingflow({"lead_id": "1", "phone": "555-SECRET-NUM"})
    assert "SECRET" not in str(exc.value)


# ------------------------------------------------------------------ route

def test_new_lead_creates_lead_contact_certificate(client, lf_db):
    response = _post(client)
    assert response.status_code == 200 and response.json()["status"] == "created"
    assert _count(lf_db, "lendingflow_leads") == 1 and _count(lf_db, "lead_consent_certificates") == 1
    assert lf_db.execute(text("SELECT count(*) FROM lending.contacts WHERE phone = '+17275550100'")).scalar_one() == 1
    assert len(client.scheduled) == 1
    row = lf_db.execute(text("SELECT contact_id, consent_status, ghl_status FROM lending.lendingflow_leads")).one()
    assert row.contact_id is not None and row.consent_status == "present" and row.ghl_status == "pending"


def test_duplicate_vendor_id(client, lf_db):
    first = _post(client).json()
    second = _post(client, _payload(phone="(813) 555-0142", email="other@example.com")).json()
    assert second == {"status": "duplicate", "lead_id": first["lead_id"]}
    assert _count(lf_db, "lendingflow_leads") == 1
    assert lf_db.execute(text("SELECT duplicate_count FROM lending.lendingflow_leads")).scalar_one() == 1
    assert _count(lf_db, "lead_consent_certificates") == 2  # the duplicate's evidence is kept
    assert len(client.scheduled) == 1  # no second delivery


def test_same_phone_email_different_vendor_id(client, lf_db):
    first = _post(client).json()
    second = _post(client, _payload(lead_id="LF-2", phone="727-555-0100", email="TEST.BORROWER@example.com")).json()
    assert second["status"] == "duplicate" and second["lead_id"] == first["lead_id"]
    assert _count(lf_db, "lendingflow_leads") == 1 and len(client.scheduled) == 1


@pytest.mark.parametrize("raw", ["not json", "[]", json.dumps({"phone": "7275550100"}),
                                 json.dumps({"lead_id": "1", "phone": "12345"})])
def test_malformed_payload_is_400_with_detail(client, lf_db, raw):
    response = _post(client, raw=raw)
    assert response.status_code == 400 and response.json() == {"detail": "Invalid LendingFlow payload"}
    assert _count(lf_db, "lendingflow_leads") == 0


def test_certificate_stored_verbatim(client, lf_db):
    _post(client)
    cert = lf_db.execute(text("SELECT * FROM lending.lead_consent_certificates")).mappings().one()
    assert cert["raw_certificate"] == CERT_RAW
    assert cert["client_ip"] == "203.0.113.7" and cert["source_url"] == "https://lendingflow.example/apply"
    assert cert["tcpa_disclosure_text"] == "By clicking Submit you agree"
    assert cert["verification_method"] == "certificate_timestamp"


def test_certificate_without_timestamp_is_receipt_only(client, lf_db):
    _post(client, _payload(consent={"raw": "abc"}))
    cert = lf_db.execute(text("SELECT verified_at, received_at, verification_method FROM lending.lead_consent_certificates")).one()
    assert cert.verification_method == "receipt_only" and cert.verified_at == cert.received_at


def test_no_certificate_marks_consent_missing(client, lf_db):
    body = _payload()
    del body["consent"]
    assert _post(client, body).status_code == 200
    assert lf_db.execute(text("SELECT consent_status FROM lending.lendingflow_leads")).scalar_one() == "missing"
    assert _count(lf_db, "lead_consent_certificates") == 0 and len(client.scheduled) == 1


def test_auth(client, monkeypatch):
    assert _post(client, secret="wrong").status_code == 401
    assert _post(client, secret=None).status_code == 401
    from src.lending import lendingflow_webhook as hook
    monkeypatch.setattr(hook.get_settings(), "lending_lendingflow_webhook_secret", None)
    assert _post(client).status_code == 503


def test_non_ascii_secret_is_401_not_500(client):
    from fastapi import HTTPException
    from src.lending.lendingflow_webhook import _verify_lendingflow_secret

    with pytest.raises(HTTPException) as exc:  # a raw header can carry non-ASCII; str compare_digest would raise TypeError
        _verify_lendingflow_secret("sécret")
    assert exc.value.status_code == 401


def test_duplicate_has_no_delivery_event_or_prequal(client, lf_db, seen):
    events, prequal = seen
    _post(client)
    _post(client)
    assert len(client.scheduled) == 1 and events == [] and prequal == []


def test_duplicate_never_touches_contacts(client, lf_db):
    body = _payload()
    del body["email"]
    _post(client, body)
    _post(client, _payload(phone="(727) 555-0100", email="late@example.com", lead_id="LF-1"))
    assert lf_db.execute(text("SELECT email FROM lending.contacts WHERE phone = '+17275550100'")).scalar_one() is None


def test_flag_off_is_503_and_stores_nothing(client, lf_db, monkeypatch):
    from src.lending import lendingflow_webhook as hook
    monkeypatch.setattr(hook.get_settings(), "lending_lendingflow_enabled", False)
    assert _post(client).status_code == 503
    assert _count(lf_db, "lendingflow_leads") == 0


def test_oversized_body_is_413(client):
    assert _post(client, raw="{" + " " * (300 * 1024) + "}").status_code == 413


def test_suppressed_phone_is_stored_but_never_delivered(client, lf_db):
    lf_db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES ('+17275550100', true) "
                       "ON CONFLICT (phone) DO UPDATE SET do_not_contact = true"))
    assert _post(client).json()["status"] == "created"
    row = lf_db.execute(text("SELECT suppressed, suppression_reason, ghl_status FROM lending.lendingflow_leads")).one()
    assert row.suppressed and row.suppression_reason == "do_not_contact" and row.ghl_status == "skipped"
    assert client.scheduled == []
    assert deliver_pending(lf_db, RecordingSink()) == []


def test_concurrent_identical_deliveries_make_one_row(lf_db):
    """The UNIQUE indexes, not app code, are the dedupe: a conflicting insert does nothing."""
    parsed = parse_lendingflow(_payload())
    first = save_lead(lf_db, parsed, _payload())
    second = save_lead(lf_db, parsed, _payload())
    assert first.created and not second.created and first.lead_id == second.lead_id
    assert _count(lf_db, "lendingflow_leads") == 1


# ------------------------------------------------------------------ delivery, event, pre-qual

@pytest.fixture
def seen(monkeypatch):
    events, prequal = [], []
    lendingflow_events._clear_for_tests()
    lendingflow_events.subscribe(events.append)
    monkeypatch.setattr("src.lending.prequal_letters.queue_and_send_in_background", lambda **kw: prequal.append(kw))
    yield events, prequal
    lendingflow_events._clear_for_tests()


def _saved(db, **overrides):
    payload = _payload(**overrides)
    return save_lead(db, parse_lendingflow(payload), payload)


def test_delivery_emits_event_once_and_queues_prequal(lf_db, seen):
    events, prequal = seen
    _saved(lf_db)
    sink = RecordingSink()
    followups = deliver_pending(lf_db, sink)
    run_followups(followups)
    assert len(sink.pushed) == 1 and sink.pushed[0]["phone"] == "+17275550100"
    assert len(events) == 1 and events[0].loan_type is LoanType.FIX_AND_FLIP and events[0].credit_band_min_fico == 680
    assert events[0].phone == "+17275550100" and events[0].loan_amount == 250000
    assert len(prequal) == 1 and prequal[0]["lead_source"] == "lendingflow" and prequal[0]["lead_ref"] == "LF-1"
    assert prequal[0]["contact_id"] == "ghl_1" and prequal[0]["lead"].property_state == "FL"
    assert deliver_pending(lf_db, sink) == [] and len(sink.pushed) == 1  # nothing left to deliver


def test_failed_push_then_sweep_emits_exactly_once(lf_db, seen):
    events, _ = seen
    _saved(lf_db)
    assert deliver_pending(lf_db, RecordingSink(error=DeliveryError("contact upsert: HTTP 500"))) == []
    row = lf_db.execute(text("SELECT ghl_status, ghl_attempts, ghl_last_error, event_emitted_at "
                             "FROM lending.lendingflow_leads")).one()
    assert row.ghl_status == "failed" and row.ghl_attempts == 1 and row.event_emitted_at is None
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    run_followups(deliver_pending(lf_db, RecordingSink(), now=later))
    run_followups(deliver_pending(lf_db, RecordingSink(), now=later))
    assert len(events) == 1


def test_missing_prequal_field_still_emits_but_skips_prequal(lf_db, seen):
    events, prequal = seen
    _saved(lf_db, loan_amount=None)
    run_followups(deliver_pending(lf_db, RecordingSink()))
    assert len(events) == 1 and prequal == []


def test_event_handler_failure_does_not_stop_prequal(lf_db, seen):
    events, prequal = seen

    def boom(event):
        raise RuntimeError("handler bug")

    lendingflow_events._handlers.insert(0, boom)
    _saved(lf_db)
    run_followups(deliver_pending(lf_db, RecordingSink()))
    assert len(events) == 1 and len(prequal) == 1


def test_no_sink_leaves_lead_pending(lf_db):
    _saved(lf_db)
    assert deliver_pending(lf_db, None) == []
    assert lf_db.execute(text("SELECT ghl_status, ghl_attempts FROM lending.lendingflow_leads")).one() == ("pending", 0)


def test_ghl_sink_tags_missing_consent_and_skips_card_without_stage(monkeypatch):
    from src.lending.ghl_account import GhlAccount
    from src.lending.lendingflow_ghl import LendingFlowGhlSink

    calls = []

    class Resp:
        status_code = 200

        def __init__(self, body):
            self._b = body

        def json(self):
            return self._b

    def fake(method, url, **kw):
        calls.append((method, url.rsplit("/", 1)[-1], kw.get("json")))
        return Resp({"contact": {"id": "c1"}})

    from src.services import ghl_webhook
    monkeypatch.setattr(ghl_webhook, "_ghl_request", fake)
    lead = {"id": 1, "lead_uuid": "u-1", "vendor_lead_id": "LF-1", "phone": "+17275550100", "email": None, "first_name": None,
            "last_name": None, "consent_status": "missing", "received_at": datetime(2026, 10, 9, tzinfo=timezone.utc)}
    result = LendingFlowGhlSink(GhlAccount("k", "loc")).push(lead)
    assert result == PushResult(contact_id="c1", pipeline_card=False)
    upsert = next(c for c in calls if c[1] == "upsert")
    assert "dnd" not in upsert[2] and "tags" not in upsert[2] and "email" not in upsert[2]
    tags = next(c for c in calls if c[1] == "tags")
    assert tags[2]["tags"] == ["lendingflow", "lendingflow-consent-missing"]
    note = next(c for c in calls if c[1] == "notes")
    assert note[2]["body"] == "LendingFlow lead u-1 (vendor id LF-1)."


def test_logs_carry_no_phone_or_email(client, lf_db, seen, caplog):
    caplog.set_level(logging.DEBUG)
    _post(client)
    run_followups(deliver_pending(lf_db, RecordingSink()))
    text_logged = caplog.text
    assert "5550100" not in text_logged and "example.com" not in text_logged.lower()
